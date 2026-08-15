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
#include <ios>
#include <memory>
#include <optional>
#include <string>
#include <utility>

/* The database is used to find the spending transaction of a given utxo.
 * New entries store a compact key reused from txindex:
 *   ['x', 5-byte siphash(outpoint), varint height, 3-byte tx offset] -> (empty)
 * Lookups seek the hash prefix and, because reorged blocks are deleted, treat
 * the packed sequence number as the spending block's height.
 * Legacy entries remain readable:
 *   ['s', 8-byte siphash(outpoint), CDiskTxPos] -> (empty)
 */

// LevelDB key prefix. We only have one key for now but it will make it easier to add others if needed.
constexpr uint8_t DB_TXOSPENDERINDEX{'s'};

std::unique_ptr<TxoSpenderIndex> g_txospenderindex;

namespace {
SipHasher13UJ ReadOrCreateHasher(CDBWrapper& db)
{
    std::pair<uint64_t, uint64_t> salt;
    if (!db.Read(txindex::DB_TXID_HASH_SALT, salt)) {
        FastRandomContext rng(false);
        salt = {rng.rand64(), rng.rand64()};
        db.Write(txindex::DB_TXID_HASH_SALT, salt, /*fSync=*/true);
    }
    return SipHasher13UJ{salt.first, salt.second};
}

struct LegacyDBKey {
    uint64_t hash;
    CDiskTxPos pos;

    explicit LegacyDBKey(const uint64_t& hash_in, const CDiskTxPos& pos_in) : hash(hash_in), pos(pos_in) {}

    SERIALIZE_METHODS(LegacyDBKey, obj)
    {
        uint8_t prefix{DB_TXOSPENDERINDEX};
        READWRITE(prefix);
        if (prefix != DB_TXOSPENDERINDEX) {
            throw std::ios_base::failure("Invalid format for spender index DB key");
        }
        READWRITE(obj.hash);
        READWRITE(obj.pos);
    }
};
} // namespace

/** Access to the txospenderindex database (indexes/txospenderindex/db) */
class TxoSpenderIndex::DB : public BaseIndex::DB
{
public:
    explicit DB(size_t n_cache_size, bool f_memory = false, bool f_wipe = false);

    const SipHasher13UJ m_hasher;

    CBlockLocator ReadBestBlock() const override;
    void WriteBestBlock(CDBBatch& batch, const CBlockLocator& locator) override;
};

static fs::path TxoSpenderIndexDBPath() { return gArgs.GetDataDirNet() / "indexes" / "txospenderindex" / "db"; }

TxoSpenderIndex::DB::DB(size_t n_cache_size, bool f_memory, bool f_wipe) :
    BaseIndex::DB(TxoSpenderIndexDBPath(), n_cache_size, f_memory, f_wipe, /*f_obfuscate=*/false, /*f_bloom=*/false),
    m_hasher{ReadOrCreateHasher(*this)}
{}

CBlockLocator TxoSpenderIndex::DB::ReadBestBlock() const
{
    CBlockLocator locator;
    if (Read(txindex::DB_BEST_BLOCK_V2, locator)) {
        return locator;
    }
    return BaseIndex::DB::ReadBestBlock();
}

void TxoSpenderIndex::DB::WriteBestBlock(CDBBatch& batch, const CBlockLocator& locator)
{
    batch.Write(txindex::DB_BEST_BLOCK_V2, locator);
}

TxoSpenderIndex::TxoSpenderIndex(std::unique_ptr<interfaces::Chain> chain, size_t n_cache_size, bool f_memory, bool f_wipe)
    : BaseIndex(std::move(chain), "txospenderindex", "txospenderidx"), m_db{std::make_unique<DB>(n_cache_size, f_memory, f_wipe)}
{
    if (m_db->Read("siphash_key", m_siphash_key)) {
        m_has_legacy = true;
        LogInfo("txospenderindex contains entries in the legacy format, which uses excessive disk space. "
                "To reclaim disk space, stop the node, delete %s and restart to rebuild the index.",
                fs::PathToString(TxoSpenderIndexDBPath()));
    }
}

interfaces::Chain::NotifyOptions TxoSpenderIndex::CustomOptions()
{
    interfaces::Chain::NotifyOptions options;
    options.disconnect_data = true;
    return options;
}

namespace {
txindex::TxHashKeyPrefix CreateHashedPrefix(const SipHasher13UJ& hasher, const COutPoint& outpoint)
{
    return txindex::CreateKeyPrefix(hasher, outpoint.hash.ToUint256(), outpoint.n);
}

uint64_t CreateLegacyPrefix(const std::pair<uint64_t, uint64_t>& salt, const COutPoint& outpoint)
{
    return PresaltedSipHasher(salt.first, salt.second)(outpoint.hash.ToUint256(), outpoint.n);
}

template <typename Fn>
void ForEachSpend(const interfaces::BlockInfo& block, Fn&& fn)
{
    assert(block.data);
    assert(block.height >= 0);
    const uint32_t tx_count_size{static_cast<uint32_t>(GetSizeOfCompactSize(block.data->vtx.size()))};
    uint32_t tx_offset_in_block{txindex::BLOCK_HEADER_SIZE + tx_count_size};
    uint32_t tx_offset_after_header{tx_count_size};
    for (const auto& tx : block.data->vtx) {
        if (!tx->IsCoinBase()) {
            const txindex::BlockTxPosition hashed_pos{static_cast<uint32_t>(block.height), tx_offset_in_block};
            const CDiskTxPos legacy_pos{{block.file_number, block.data_pos}, tx_offset_after_header};
            for (const auto& input : tx->vin) {
                fn(input.prevout, hashed_pos, legacy_pos);
            }
        }
        const uint32_t tx_size{tx->ComputeTotalSize()};
        tx_offset_in_block += tx_size;
        tx_offset_after_header += tx_size;
    }
}
} // namespace

void TxoSpenderIndex::WriteSpenders(const interfaces::BlockInfo& block)
{
    CDBBatch batch(*m_db);
    ForEachSpend(block, [&](const COutPoint& outpoint, const txindex::BlockTxPosition& hashed_pos, const CDiskTxPos&) {
        batch.Write(txindex::DBKey{CreateHashedPrefix(m_db->m_hasher, outpoint), hashed_pos}, txindex::EMPTY_VALUE);
    });
    m_db->WriteBatch(batch);
}

void TxoSpenderIndex::EraseSpenders(const interfaces::BlockInfo& block)
{
    CDBBatch batch(*m_db);
    ForEachSpend(block, [&](const COutPoint& outpoint, const txindex::BlockTxPosition& hashed_pos, const CDiskTxPos& legacy_pos) {
        batch.Erase(txindex::DBKey{CreateHashedPrefix(m_db->m_hasher, outpoint), hashed_pos});
        if (m_has_legacy) {
            batch.Erase(LegacyDBKey{CreateLegacyPrefix(m_siphash_key, outpoint), legacy_pos});
        }
    });
    m_db->WriteBatch(batch);
}

TxoSpenderIndex::~TxoSpenderIndex() = default;

bool TxoSpenderIndex::CustomAppend(const interfaces::BlockInfo& block)
{
    WriteSpenders(block);
    return true;
}

bool TxoSpenderIndex::CustomRemove(const interfaces::BlockInfo& block)
{
    EraseSpenders(block);
    return true;
}

util::Expected<TxoSpender, std::string> ReadLegacyTransaction(Chainstate& chainstate, const CDiskTxPos& tx_pos)
{
    AutoFile file{chainstate.m_blockman.OpenBlockFile(tx_pos, /*fReadOnly=*/true)};
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

util::Expected<std::optional<TxoSpender>, std::string> TxoSpenderIndex::FindLegacySpender(const COutPoint& txo) const
{
    const uint64_t prefix{CreateLegacyPrefix(m_siphash_key, txo)};
    std::unique_ptr<CDBIterator> it(m_db->NewIterator());
    LegacyDBKey key(prefix, CDiskTxPos());

    // find all keys that start with the outpoint hash, load the transaction at the location specified in the key
    // and return it if it does spend the provided outpoint
    for (it->Seek(std::pair{DB_TXOSPENDERINDEX, prefix}); it->Valid() && it->GetKey(key) && key.hash == prefix; it->Next()) {
        if (const auto spender{ReadLegacyTransaction(*m_chainstate, key.pos)}) {
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
    return util::Expected<std::optional<TxoSpender>, std::string>(std::nullopt);
}

util::Expected<std::optional<TxoSpender>, std::string> TxoSpenderIndex::FindSpender(const COutPoint& txo) const
{
    const txindex::TxHashKeyPrefix prefix{CreateHashedPrefix(m_db->m_hasher, txo)};
    std::unique_ptr<CDBIterator> it{m_db->NewIterator()};
    txindex::DBKey key{prefix, {}};
    for (it->Seek(key); it->Valid() && it->GetKey(key) && key.hash_prefix == prefix; it->Next()) {
        FlatFilePos tx_position;
        uint256 block_hash;
        {
            LOCK(cs_main);
            const CBlockIndex* block_index{m_chainstate->m_chain[key.pos.block_seq]};
            if (!block_index) {
                LogWarning("Block at height %u not found for outpoint %s:%d", key.pos.block_seq, txo.hash.GetHex(), txo.n);
                continue;
            }
            if (!(block_index->nStatus & BLOCK_HAVE_DATA)) continue;
            tx_position = {block_index->nFile, block_index->nDataPos + key.pos.tx_offset_in_block};
            block_hash = block_index->GetBlockHash();
        }
        AutoFile file{m_chainstate->m_blockman.OpenBlockFile(tx_position, /*fReadOnly=*/true)};
        if (file.IsNull()) {
            LogWarning("OpenBlockFile failed for outpoint %s:%d", txo.hash.GetHex(), txo.n);
            continue;
        }
        CTransactionRef tx;
        try {
            file >> TX_WITH_WITNESS(tx);
        } catch (const std::exception& e) {
            LogWarning("Deserialize or I/O error - %s", e.what());
            continue;
        }
        for (const auto& input : tx->vin) {
            if (input.prevout == txo) {
                return std::optional{TxoSpender{std::move(tx), block_hash}};
            }
        }
    }
    return m_has_legacy ? FindLegacySpender(txo) : util::Expected<std::optional<TxoSpender>, std::string>(std::nullopt);
}

BaseIndex::DB& TxoSpenderIndex::GetDB() const { return *m_db; }
