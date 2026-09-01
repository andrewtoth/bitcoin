// Copyright (c) The Bitcoin Core developers
// Distributed under the MIT software license, see the accompanying
// file COPYING or http://www.opensource.org/licenses/mit-license.php.

#ifndef BITCOIN_INDEX_TXOSPENDERINDEX_H
#define BITCOIN_INDEX_TXOSPENDERINDEX_H

#include <index/base.h>
#include <primitives/transaction.h>
#include <uint256.h>
#include <util/expected.h>

#include <cstddef>
#include <memory>
#include <optional>
#include <string>

class COutPoint;
namespace interfaces {
class Chain;
}
namespace txospenderindex_tests {
class TxoSpenderIndexTest;
}

inline constexpr bool DEFAULT_TXOSPENDERINDEX{false};

struct TxoSpender {
    //! Spending transaction. Null if the containing block has been pruned.
    CTransactionRef tx;
    uint256 block_hash;
};

/**
 * TxoSpenderIndex is used to look up which transaction spent a given output.
 * The index is written to a LevelDB database and, for each input of each
 * transaction in a block, records a siphash prefix of the spent outpoint plus
 * the spending transaction's height and serialized block offset.
 */
class TxoSpenderIndex final : public BaseIndex
{
protected:
    class DB;

private:
    friend class txospenderindex_tests::TxoSpenderIndexTest;
    const std::unique_ptr<DB> m_db;

    bool AllowPrune() const override;

    util::Expected<std::optional<TxoSpender>, std::string> FindLegacySpender(const COutPoint& txo) const;

protected:
    interfaces::Chain::NotifyOptions CustomOptions() override;

    bool CustomAppend(const interfaces::BlockInfo& block) override;

    bool CustomRemove(const interfaces::BlockInfo& block) override;

    BaseIndex::DB& GetDB() const override;

public:
    explicit TxoSpenderIndex(std::unique_ptr<interfaces::Chain> chain, size_t n_cache_size, bool f_memory = false, bool f_wipe = false);

    ~TxoSpenderIndex() override;

    //! True if the on-disk database still contains pre-hashing ('s' prefix) entries.
    bool HasLegacyEntries() const;

    /**
     * Search the index for a transaction that spends the given outpoint.
     *
     * @param[in] txo  The outpoint to search for.
     *
     * @return  std::nullopt               if the outpoint has not been spent on-chain.
     *          std::optional{TxoSpender}  if the output has been spent on-chain. tx is null when
     *                                     the spending block is pruned; block_hash is still set.
     *          util::Unexpected{error}    if something unexpected happened (i.e. disk or deserialization error).
     */
    util::Expected<std::optional<TxoSpender>, std::string> FindSpender(const COutPoint& txo) const;
};

/// The global txo spender index. May be null.
extern std::unique_ptr<TxoSpenderIndex> g_txospenderindex;


#endif // BITCOIN_INDEX_TXOSPENDERINDEX_H
