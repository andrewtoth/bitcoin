// Copyright (c) The Bitcoin Core developers
// Distributed under the MIT software license, see the accompanying
// file COPYING or http://www.opensource.org/licenses/mit-license.php.

#include <index/scriptpubkeyindex.h>

#include <chain.h>
#include <common/args.h>
#include <crypto/siphash.h>
#include <dbwrapper.h>
#include <index/base.h>
#include <index/scriptpubkeyindex_key.h>
#include <interfaces/chain.h>
#include <node/blockstorage.h>
#include <primitives/block.h>
#include <primitives/transaction.h>
#include <random.h>
#include <script/script.h>
#include <serialize.h>
#include <streams.h>
#include <sync.h>
#include <uint256.h>
#include <util/fs.h>
#include <util/log.h>
#include <validation.h>

#include <cstdint>
#include <memory>
#include <set>
#include <string>
#include <utility>
#include <vector>

std::unique_ptr<ScriptPubKeyIndex> g_scriptpubkeyindex;

namespace {
SipHasher13UJ ReadOrCreateHasher(CDBWrapper& db)
{
    std::pair<uint64_t, uint64_t> salt;
    if (!db.Read(scriptpubkeyindex::DB_SPK_HASH_SALT, salt)) {
        FastRandomContext rng{};
        salt = {rng.rand64(), rng.rand64()};
        db.Write(scriptpubkeyindex::DB_SPK_HASH_SALT, salt, /*fSync=*/true);
    }
    return SipHasher13UJ{salt.first, salt.second};
}

std::vector<scriptpubkeyindex::DBKey> BuildKeys(const SipHasher13UJ& hasher, const interfaces::BlockInfo& block)
{
    assert(block.data);
    assert(block.height >= 0);
    std::vector<scriptpubkeyindex::DBKey> keys;
    uint32_t tx_offset_in_block{scriptpubkeyindex::BLOCK_HEADER_SIZE + GetSizeOfCompactSize(block.data->vtx.size())};
    for (const auto& tx : block.data->vtx) {
        std::set<scriptpubkeyindex::HashPrefix> prefixes;
        for (const auto& output : tx->vout) {
            prefixes.insert(scriptpubkeyindex::CreateKeyPrefix(hasher, scriptpubkeyindex::Sha256ScriptPubKey(output.scriptPubKey)));
        }
        for (const auto prefix : prefixes) {
            keys.emplace_back(prefix, static_cast<uint32_t>(block.height), tx_offset_in_block);
        }
        tx_offset_in_block += tx->ComputeTotalSize();
    }
    return keys;
}
} // namespace

class ScriptPubKeyIndex::DB : public BaseIndex::DB
{
public:
    explicit DB(size_t n_cache_size, bool f_memory, bool f_wipe)
        : BaseIndex::DB(gArgs.GetDataDirNet() / "indexes" / "scriptpubkeyindex", n_cache_size, f_memory, f_wipe, /*f_obfuscate=*/false, /*f_bloom=*/false),
          m_hasher{ReadOrCreateHasher(*this)}
    {}

    const SipHasher13UJ m_hasher;

    void WriteKeys(const std::vector<scriptpubkeyindex::DBKey>& keys)
    {
        CDBBatch batch{*this};
        for (const auto& key : keys) {
            batch.Write(key, scriptpubkeyindex::EMPTY_VALUE);
        }
        WriteBatch(batch);
    }

    void EraseKeys(const std::vector<scriptpubkeyindex::DBKey>& keys)
    {
        CDBBatch batch{*this};
        for (const auto& key : keys) {
            batch.Erase(key);
        }
        WriteBatch(batch);
    }
};

ScriptPubKeyIndex::ScriptPubKeyIndex(std::unique_ptr<interfaces::Chain> chain, size_t n_cache_size, bool f_memory, bool f_wipe)
    : BaseIndex(std::move(chain), "scriptpubkeyindex", "spkidx"), m_db(std::make_unique<DB>(n_cache_size, f_memory, f_wipe))
{}

ScriptPubKeyIndex::~ScriptPubKeyIndex() = default;

interfaces::Chain::NotifyOptions ScriptPubKeyIndex::CustomOptions()
{
    interfaces::Chain::NotifyOptions options;
    options.disconnect_data = true;
    return options;
}

bool ScriptPubKeyIndex::CustomAppend(const interfaces::BlockInfo& block)
{
    m_db->WriteKeys(BuildKeys(m_db->m_hasher, block));
    return true;
}

bool ScriptPubKeyIndex::CustomRemove(const interfaces::BlockInfo& block)
{
    m_db->EraseKeys(BuildKeys(m_db->m_hasher, block));
    return true;
}

BaseIndex::DB& ScriptPubKeyIndex::GetDB() const { return *m_db; }

std::vector<ScriptPubKeyIndexMatch> ScriptPubKeyIndex::Find(const uint256& script_hash, int start_height, int end_height) const
{
    if (start_height < 0 || end_height < start_height) return {};

    const scriptpubkeyindex::HashPrefix prefix{scriptpubkeyindex::CreateKeyPrefix(m_db->m_hasher, script_hash)};
    std::vector<scriptpubkeyindex::DBKey> keys;
    {
        std::unique_ptr<CDBIterator> it{m_db->NewIterator()};
        scriptpubkeyindex::DBKey key{prefix, static_cast<uint32_t>(start_height), /*tx_offset_in_block=*/0};
        for (it->Seek(key); it->Valid() && it->GetKey(key) && key.hash_prefix == prefix; it->Next()) {
            if (key.height > static_cast<uint32_t>(end_height)) break;
            keys.push_back(key);
        }
    }

    struct Candidate {
        FlatFilePos tx_pos;
        uint256 block_hash;
        int height;
    };
    std::vector<Candidate> candidates;
    candidates.reserve(keys.size());
    {
        LOCK(cs_main);
        for (const auto& key : keys) {
            const CBlockIndex* block_index{m_chainstate->m_chain[key.height]};
            if (!block_index || !(block_index->nStatus & BLOCK_HAVE_DATA)) continue;
            candidates.emplace_back(FlatFilePos{block_index->nFile, block_index->nDataPos + key.tx_offset_in_block},
                                    block_index->GetBlockHash(),
                                    static_cast<int>(key.height));
        }
    }

    std::vector<ScriptPubKeyIndexMatch> matches;
    for (const auto& candidate : candidates) {
        AutoFile file{m_chainstate->m_blockman.OpenBlockFile(candidate.tx_pos, /*fReadOnly=*/true)};
        if (file.IsNull()) {
            LogWarning("OpenBlockFile failed for scriptpubkeyindex at height %d", candidate.height);
            continue;
        }
        CTransactionRef tx;
        try {
            file >> TX_WITH_WITNESS(tx);
        } catch (const std::exception& e) {
            LogWarning("Deserialize or I/O error in scriptpubkeyindex - %s", e.what());
            continue;
        }
        std::vector<uint32_t> vouts;
        for (uint32_t i{0}; i < tx->vout.size(); ++i) {
            if (scriptpubkeyindex::Sha256ScriptPubKey(tx->vout[i].scriptPubKey) == script_hash) {
                vouts.push_back(i);
            }
        }
        if (vouts.empty()) continue;
        matches.emplace_back(std::move(tx), candidate.block_hash, candidate.height, std::move(vouts));
    }
    return matches;
}

std::vector<ScriptPubKeyIndexMatch> ScriptPubKeyIndex::Find(const CScript& script_pubkey, int start_height, int end_height) const
{
    return Find(scriptpubkeyindex::Sha256ScriptPubKey(script_pubkey), start_height, end_height);
}
