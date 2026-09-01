// Copyright (c) The Bitcoin Core developers
// Distributed under the MIT software license, see the accompanying
// file COPYING or http://www.opensource.org/licenses/mit-license.php.

#ifndef BITCOIN_INDEX_SCRIPTPUBKEYINDEX_H
#define BITCOIN_INDEX_SCRIPTPUBKEYINDEX_H

#include <index/base.h>
#include <primitives/transaction.h>
#include <uint256.h>

#include <cstddef>
#include <cstdint>
#include <memory>
#include <optional>
#include <string>
#include <vector>

class CScript;

namespace interfaces {
class Chain;
}

inline constexpr bool DEFAULT_SCRIPTPUBKEYINDEX{false};

struct ScriptPubKeyIndexMatch {
    CTransactionRef tx;
    uint256 block_hash;
    int height;
    //! Output indexes in tx whose scriptPubKey hashes to the queried script hash.
    std::vector<uint32_t> vouts;
};

/**
 * ScriptPubKeyIndex maps SHA256(scriptPubKey) to confirmed transactions that
 * create at least one matching output. Keys store a 5-byte SipHash prefix, the
 * block height, and the transaction's byte offset in the block.
 */
class ScriptPubKeyIndex final : public BaseIndex
{
protected:
    class DB;

private:
    const std::unique_ptr<DB> m_db;

    bool AllowPrune() const override { return true; }

protected:
    interfaces::Chain::NotifyOptions CustomOptions() override;

    bool CustomAppend(const interfaces::BlockInfo& block) override;

    bool CustomRemove(const interfaces::BlockInfo& block) override;

    BaseIndex::DB& GetDB() const override;

public:
    explicit ScriptPubKeyIndex(std::unique_ptr<interfaces::Chain> chain, size_t n_cache_size, bool f_memory = false, bool f_wipe = false);

    ~ScriptPubKeyIndex() override;

    //! Find confirmed transactions paying to a script whose SHA256 is script_hash,
    //! with height in [start_height, end_height] inclusive.
    std::vector<ScriptPubKeyIndexMatch> Find(const uint256& script_hash, int start_height, int end_height) const;

    std::vector<ScriptPubKeyIndexMatch> Find(const CScript& script_pubkey, int start_height, int end_height) const;
};

extern std::unique_ptr<ScriptPubKeyIndex> g_scriptpubkeyindex;

#endif // BITCOIN_INDEX_SCRIPTPUBKEYINDEX_H
