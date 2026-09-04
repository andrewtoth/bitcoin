// Copyright (c) The Bitcoin Core developers
// Distributed under the MIT software license, see the accompanying
// file COPYING or http://www.opensource.org/licenses/mit-license.php.

#ifndef BITCOIN_INDEX_SCRIPTPUBKEYINDEX_H
#define BITCOIN_INDEX_SCRIPTPUBKEYINDEX_H

#include <crypto/sha256.h>
#include <index/base.h>
#include <primitives/transaction.h>
#include <script/script.h>
#include <uint256.h>
#include <util/expected.h>

#include <cstddef>
#include <cstdint>
#include <functional>
#include <memory>
#include <string>
#include <vector>

namespace scriptpubkeyindex {
inline uint256 Sha256ScriptPubKey(const CScript& script)
{
    uint256 hash;
    CSHA256().Write(script.data(), script.size()).Finalize(hash.data());
    return hash;
}
} // namespace scriptpubkeyindex

namespace interfaces {
class Chain;
}

namespace scriptpubkeyindex_tests {
class ScriptPubKeyIndexTest;
}

inline constexpr bool DEFAULT_SCRIPTPUBKEYINDEX{false};

//! Result of a scriptpubkeyindex prefix lookup.
//! `tx` is null when a matching key was found but the block has been pruned.
struct ScriptHistoryMatch {
    CTransactionRef tx;
    uint256 block_hash;
    int32_t height;
    //! Output indexes in tx whose scriptPubKey hashes to the queried script hash.
    //! Empty when `tx` is null.
    std::vector<uint32_t> outputs;
    //! Transaction byte offset within the block; zero for pruned-block placeholders.
    uint32_t tx_offset{0};
};

/**
 * ScriptPubKeyIndex is used to look up transactions creating outputs with a given script.
 * The index is written to a LevelDB database and, for each distinct script of each transaction in a block,
 * records the SHA256(script), along with the block height and position of the transaction in the block.
 */
class ScriptPubKeyIndex final : public BaseIndex
{
    friend class scriptpubkeyindex_tests::ScriptPubKeyIndexTest;

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
    explicit ScriptPubKeyIndex(std::unique_ptr<interfaces::Chain> chain, size_t cache_size, bool memory = false, bool wipe = false);

    ~ScriptPubKeyIndex() override;

    /**
     * Search the index for confirmed transactions with outputs matching the given script hash.
     *
     * @param[in]     script_hash   SHA256 of the scriptPubKey to search for.
     * @param[in]     start_height  First block height to search (inclusive).
     * @param[in]     end_height    Last block height to search (inclusive).
     * @param[in]     on_match      Callback invoked with each verified matching transaction and its block hash.
     *
     * @return  Matches in block height and transaction position order (empty if none).
     *          util::Unexpected{error} if an index key or transaction could not be read.
     *
     * Exceptions thrown by on_match propagate to the caller unchanged.
     */
    util::Expected<std::vector<ScriptHistoryMatch>, std::string> Find(const uint256& script_hash, int32_t start_height, int32_t end_height, const std::function<void(const CTransaction&, const uint256&)>& on_match) const;
};

extern std::unique_ptr<ScriptPubKeyIndex> g_scriptpubkeyindex;

#endif // BITCOIN_INDEX_SCRIPTPUBKEYINDEX_H
