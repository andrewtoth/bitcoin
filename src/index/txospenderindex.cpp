// Copyright (c) The Bitcoin Core developers
// Distributed under the MIT software license, see the accompanying
// file COPYING or http://www.opensource.org/licenses/mit-license.php.

#include <index/txospenderindex.h>

#include <chain.h>
#include <common/args.h>
#include <crypto/siphash.h>
#include <dbwrapper.h>
#include <flatfile.h>
#include <index/base.h>
#include <index/disktxpos.h>
#include <index/txindex_key.h>
#include <index/txospenderindex_key.h>
#include <interfaces/chain.h>
#include <node/blockstorage.h>
#include <primitives/block.h>
#include <primitives/transaction.h>
#include <random.h>
#include <serialize.h>
#include <streams.h>
#include <sync.h>
#include <tinyformat.h>
#include <uint256.h>
#include <util/fs.h>
#include <util/log.h>
#include <validation.h>

#include <cassert>
#include <cstdint>
#include <exception>
#include <memory>
#include <optional>
#include <string>
#include <utility>
#include <vector>

std::unique_ptr<TxoSpenderIndex> g_txospenderindex;

namespace {
SipHasher13UJ ReadOrCreateOutpointHasher(CDBWrapper& db)
{
    std::pair<uint64_t, uint64_t> salt;
    if (!db.Read(txospenderindex::DB_OUTPOINT_HASH_SALT, salt)) {
        FastRandomContext rng{};
        salt = {rng.rand64(), rng.rand64()};
        db.Write(txospenderindex::DB_OUTPOINT_HASH_SALT, salt, /*fSync=*/true);
    }
    return SipHasher13UJ{salt.first, salt.second};
}

std::vector<txindex::DBKey> BuildKeys(const SipHasher13UJ& hasher, const interfaces::BlockInfo& block)
{
    assert(block.data);
    assert(block.height >= 0);
    std::vector<txindex::DBKey> keys;
    uint32_t tx_offset_in_block{txindex::BLOCK_HEADER_SIZE + GetSizeOfCompactSize(block.data->vtx.size())};
    for (const auto& tx : block.data->vtx) {
        if (!tx->IsCoinBase()) {
            for (const auto& input : tx->vin) {
                keys.emplace_back(txospenderindex::CreateKeyPrefix(hasher, input.prevout),
                                  txindex::BlockTxPosition{static_cast<uint32_t>(block.height), tx_offset_in_block});
            }
        }
        tx_offset_in_block += tx->ComputeTotalSize();
    }
    return keys;
}

util::Expected<TxoSpender, std::string> ReadLegacyTransaction(node::BlockManager& blockman, const CDiskTxPos& tx_pos)
{
    AutoFile file{blockman.OpenBlockFile(tx_pos, /*fReadOnly=*/true)};
    if (file.IsNull()) {
        return util::Unexpected("cannot open block");
    }
    CBlockHeader header;
    TxoSpender spender;
    try {
        file >> header;
        file.seek(tx_pos.nTxOffset, SEEK_CUR);
        file >> TX_WITH_WITNESS(spender.tx);
        spender.block_hash = header.GetHash();
        return spender;
    } catch (const std::exception& e) {
        return util::Unexpected(e.what());
    }
}

bool DbHasLegacyPrefix(CDBWrapper& db)
{
    std::unique_ptr<CDBIterator> it{db.NewIterator()};
    const uint8_t prefix{txospenderindex::DB_TXOSPENDERINDEX};
    uint8_t found{0};
    it->Seek(prefix);
    return it->Valid() && it->GetKey(found) && found == prefix;
}
} // namespace

static fs::path TxoSpenderIndexDBPath() { return gArgs.GetDataDirNet() / "indexes" / "txospenderindex" / "db"; }

class TxoSpenderIndex::DB : public BaseIndex::DB
{
public:
    explicit DB(size_t n_cache_size, bool f_memory = false, bool f_wipe = false);

    void WriteKeys(const std::vector<txindex::DBKey>& keys);
    void EraseKeys(const std::vector<txindex::DBKey>& keys);

    const SipHasher13UJ m_hasher;
    const bool m_has_legacy;

    CBlockLocator ReadBestBlock() const override;
    void WriteBestBlock(CDBBatch& batch, const CBlockLocator& locator) override;
};

TxoSpenderIndex::DB::DB(size_t n_cache_size, bool f_memory, bool f_wipe)
    // Hashed and legacy lookups both seek with an iterator, which bypasses bloom
    // filters, so they are never worth building for this index.
    : BaseIndex::DB(TxoSpenderIndexDBPath(), n_cache_size, f_memory, f_wipe, /*f_obfuscate=*/false, /*f_bloom=*/false),
      m_hasher{ReadOrCreateOutpointHasher(*this)},
      m_has_legacy{DbHasLegacyPrefix(*this)}
{
}

CBlockLocator TxoSpenderIndex::DB::ReadBestBlock() const
{
    CBlockLocator locator;
    if (Read(txospenderindex::DB_BEST_BLOCK_V2, locator)) {
        return locator;
    }
    return BaseIndex::DB::ReadBestBlock();
}

void TxoSpenderIndex::DB::WriteBestBlock(CDBBatch& batch, const CBlockLocator& locator)
{
    batch.Write(txospenderindex::DB_BEST_BLOCK_V2, locator);
}

void TxoSpenderIndex::DB::WriteKeys(const std::vector<txindex::DBKey>& keys)
{
    CDBBatch batch{*this};
    for (const auto& key : keys) {
        batch.Write(key, txindex::EMPTY_VALUE);
    }
    WriteBatch(batch);
}

void TxoSpenderIndex::DB::EraseKeys(const std::vector<txindex::DBKey>& keys)
{
    CDBBatch batch{*this};
    for (const auto& key : keys) {
        batch.Erase(key);
    }
    WriteBatch(batch);
}

TxoSpenderIndex::TxoSpenderIndex(std::unique_ptr<interfaces::Chain> chain, size_t n_cache_size, bool f_memory, bool f_wipe)
    : BaseIndex(std::move(chain), "txospenderindex", "txospenderidx"), m_db(std::make_unique<DB>(n_cache_size, f_memory, f_wipe))
{
    if (m_db->m_has_legacy) {
        LogInfo("txospenderindex contains entries in the legacy format, which uses excessive disk space. "
                "To reclaim disk space, stop the node, delete %s and restart to rebuild the index.",
                fs::PathToString(gArgs.GetDataDirNet() / "indexes" / "txospenderindex"));
    }
}

TxoSpenderIndex::~TxoSpenderIndex() = default;

bool TxoSpenderIndex::AllowPrune() const { return !m_db->m_has_legacy; }

bool TxoSpenderIndex::HasLegacyEntries() const { return m_db->m_has_legacy; }

interfaces::Chain::NotifyOptions TxoSpenderIndex::CustomOptions()
{
    interfaces::Chain::NotifyOptions options;
    options.disconnect_data = true;
    return options;
}

bool TxoSpenderIndex::CustomAppend(const interfaces::BlockInfo& block)
{
    m_db->WriteKeys(BuildKeys(m_db->m_hasher, block));
    return true;
}

bool TxoSpenderIndex::CustomRemove(const interfaces::BlockInfo& block)
{
    m_db->EraseKeys(BuildKeys(m_db->m_hasher, block));
    return true;
}

BaseIndex::DB& TxoSpenderIndex::GetDB() const { return *m_db; }

util::Expected<std::optional<TxoSpender>, std::string> TxoSpenderIndex::FindLegacySpender(const COutPoint& txo) const
{
    std::pair<uint64_t, uint64_t> siphash_key;
    if (!m_db->Read(txospenderindex::DB_LEGACY_SIPHASH_KEY, siphash_key)) {
        return std::optional<TxoSpender>{};
    }
    const uint64_t prefix{PresaltedSipHasher(siphash_key.first, siphash_key.second)(txo.hash.ToUint256(), txo.n)};
    std::unique_ptr<CDBIterator> it{m_db->NewIterator()};
    txospenderindex::LegacyDBKey key;
    for (it->Seek(std::pair{txospenderindex::DB_TXOSPENDERINDEX, prefix});
         it->Valid() && it->GetKey(key) && key.hash == prefix; it->Next()) {
        if (const auto spender{ReadLegacyTransaction(m_chainstate->m_blockman, key.pos)}) {
            for (const auto& input : spender->tx->vin) {
                if (input.prevout == txo) {
                    return std::optional{*spender};
                }
            }
        } else {
            LogError("Deserialize or I/O error - %s", spender.error());
            return util::Unexpected{strprintf("IO error finding spending tx for outpoint %s:%d.", txo.hash.GetHex(), txo.n)};
        }
    }
    return std::optional<TxoSpender>{};
}

util::Expected<std::optional<TxoSpender>, std::string> TxoSpenderIndex::FindSpender(const COutPoint& txo) const
{
    std::vector<txindex::DBKey> keys;
    {
        std::unique_ptr<CDBIterator> it{m_db->NewIterator()};
        const txindex::TxHashKeyPrefix prefix{txospenderindex::CreateKeyPrefix(m_db->m_hasher, txo)};
        txindex::DBKey key{prefix, {}};
        for (it->Seek(key); it->Valid() && it->GetKey(key) && key.hash_prefix == prefix; it->Next()) {
            keys.push_back(key);
        }
    }

    struct Candidate {
        FlatFilePos tx_pos;
        uint256 block_hash;
    };
    std::vector<Candidate> candidates;
    std::optional<uint256> pruned_block_hash;
    {
        LOCK(cs_main);
        for (const auto& key : keys) {
            const CBlockIndex* block_index{m_chainstate->m_chain[static_cast<int>(key.pos.block_seq)]};
            if (!block_index) {
                LogWarning("Block at height %u not found for outpoint %s:%d",
                           key.pos.block_seq, txo.hash.GetHex(), txo.n);
                continue;
            }
            if (!(block_index->nStatus & BLOCK_HAVE_DATA)) {
                pruned_block_hash = block_index->GetBlockHash();
                continue;
            }
            candidates.emplace_back(FlatFilePos{block_index->nFile, block_index->nDataPos + key.pos.tx_offset_in_block},
                                    block_index->GetBlockHash());
        }
    }

    for (const auto& candidate : candidates) {
        AutoFile file{m_chainstate->m_blockman.OpenBlockFile(candidate.tx_pos, /*fReadOnly=*/true)};
        if (file.IsNull()) {
            LogError("OpenBlockFile failed for outpoint %s:%d", txo.hash.GetHex(), txo.n);
            return util::Unexpected{strprintf("IO error finding spending tx for outpoint %s:%d.", txo.hash.GetHex(), txo.n)};
        }
        CTransactionRef tx;
        try {
            file >> TX_WITH_WITNESS(tx);
        } catch (const std::exception& e) {
            LogError("Deserialize or I/O error - %s", e.what());
            return util::Unexpected{strprintf("IO error finding spending tx for outpoint %s:%d.", txo.hash.GetHex(), txo.n)};
        }
        for (const auto& input : tx->vin) {
            if (input.prevout == txo) {
                return std::optional{TxoSpender{std::move(tx), candidate.block_hash}};
            }
        }
    }

    // A hashed candidate whose block has been pruned is authoritative: do not
    // fall back to the legacy schema, which cannot name the missing block.
    if (pruned_block_hash) {
        return std::optional{TxoSpender{/*tx=*/nullptr, *pruned_block_hash}};
    }

    if (m_db->m_has_legacy) return FindLegacySpender(txo);
    return std::optional<TxoSpender>{};
}
