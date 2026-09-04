// Copyright (c) The Bitcoin Core developers
// Distributed under the MIT software license, see the accompanying
// file COPYING or http://www.opensource.org/licenses/mit-license.php.

#include <index/scriptpubkeyindex.h>

#include <chain.h>
#include <common/args.h>
#include <crypto/siphash.h>
#include <dbwrapper.h>
#include <flatfile.h>
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
#include <validation.h>

#include <cassert>
#include <cstdint>
#include <functional>
#include <memory>
#include <ranges>
#include <set>
#include <stdexcept>
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
    explicit DB(size_t cache_size, bool memory, bool wipe)
        : BaseIndex::DB(gArgs.GetDataDirNet() / "indexes" / "scriptpubkeyindex", cache_size, memory, wipe, /*f_obfuscate=*/false, /*f_bloom=*/false),
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

ScriptPubKeyIndex::ScriptPubKeyIndex(std::unique_ptr<interfaces::Chain> chain, size_t cache_size, bool memory, bool wipe)
    : BaseIndex(std::move(chain), "scriptpubkeyindex", "spkidx"), m_db{std::make_unique<DB>(cache_size, memory, wipe)}
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

util::Expected<std::vector<ScriptHistoryMatch>, std::string> ScriptPubKeyIndex::Find(const uint256& script_hash, int32_t start_height, int32_t end_height, const std::function<void(const CTransaction&, const uint256&)>& on_match) const
{
    if (start_height < 0 || end_height < start_height) return {};

    const scriptpubkeyindex::HashPrefix prefix{scriptpubkeyindex::CreateKeyPrefix(m_db->m_hasher, script_hash)};
    std::vector<ScriptHistoryMatch> matches;
    std::set<uint256> pruned_blocks;
    const std::unique_ptr<CDBIterator> it{m_db->NewIterator()};
    scriptpubkeyindex::DBKey key{prefix, static_cast<uint32_t>(start_height), /*tx_offset_in_block=*/0};
    for (it->Seek(key); it->Valid(); it->Next()) {
        if (!it->GetKey(key)) return util::Unexpected("Invalid scriptpubkeyindex key");
        if (key.hash_prefix != prefix || key.height > static_cast<uint32_t>(end_height)) break;

        FlatFilePos tx_pos;
        uint256 block_hash;
        const int32_t height{static_cast<int32_t>(key.height)};
        {
            LOCK(cs_main);
            const CBlockIndex* block_index{m_chainstate->m_chain[key.height]};
            if (!block_index) continue;
            block_hash = block_index->GetBlockHash();
            if (block_index->nStatus & BLOCK_HAVE_DATA) {
                tx_pos = FlatFilePos{block_index->nFile, block_index->nDataPos + key.tx_offset_in_block};
            } else {
                if (pruned_blocks.insert(block_hash).second) {
                    matches.emplace_back(/*tx=*/nullptr, block_hash, height, std::vector<uint32_t>{});
                }
                continue;
            }
        }
        AutoFile file{m_chainstate->m_blockman.OpenBlockFile(tx_pos, /*fReadOnly=*/true)};
        if (file.IsNull()) {
            return util::Unexpected("Unable to read scriptpubkeyindex transaction in block " + block_hash.ToString() + ": OpenBlockFile failed");
        }
        CTransactionRef tx;
        try {
            file >> TX_WITH_WITNESS(tx);
        } catch (const std::exception& e) {
            return util::Unexpected("Unable to read scriptpubkeyindex transaction in block " + block_hash.ToString() + ": " + e.what());
        }
        std::vector<uint32_t> outputs;
        for (const uint32_t index : std::views::iota(uint32_t{0}, tx->vout.size())) {
            if (scriptpubkeyindex::Sha256ScriptPubKey(tx->vout[index].scriptPubKey) == script_hash) {
                outputs.push_back(index);
            }
        }
        if (outputs.empty()) continue;
        on_match(*tx, block_hash);
        matches.emplace_back(std::move(tx), block_hash, height, std::move(outputs), key.tx_offset_in_block);
    }
    return matches;
}
