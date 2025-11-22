// Copyright (c) The Bitcoin Core developers
// Distributed under the MIT software license, see the accompanying
// file COPYING or http://www.opensource.org/licenses/mit-license.php.

#include <coins.h>
#include <common/system.h>
#include <inputfetcher.h>
#include <primitives/block.h>
#include <primitives/transaction.h>
#include <primitives/transaction_identifier.h>
#include <test/util/random.h>
#include <test/util/setup_common.h>
#include <uint256.h>

#include <boost/test/unit_test.hpp>

#include <cstdint>
#include <memory>
#include <stdexcept>
#include <unordered_set>

BOOST_AUTO_TEST_SUITE(inputfetcher_tests)

struct NoAccessCoinsView : CCoinsView {
    std::optional<Coin> GetCoin(const COutPoint&) const override { abort(); }
};

struct InputFetcherTest : BasicTestingSetup {
private:
    std::unique_ptr<InputFetcher> m_fetcher{nullptr};
    std::unique_ptr<CBlock> m_block{nullptr};

    CBlock CreateBlock(int32_t num_txs)
    {
        CBlock block;
        CMutableTransaction coinbase;
        coinbase.vin.emplace_back();
        block.vtx.push_back(MakeTransactionRef(coinbase));

        Txid prevhash{Txid::FromUint256(uint256(1))};

        for (auto i{1}; i < num_txs; ++i) {
            CMutableTransaction tx;
            const auto txid{m_rng.randbool() ? Txid::FromUint256(uint256(i)) : prevhash};
            tx.vin.emplace_back(COutPoint(txid, 0));
            prevhash = tx.GetHash();
            block.vtx.push_back(MakeTransactionRef(tx));
        }

        return block;
    }

public:
    explicit InputFetcherTest(const ChainType chainType = ChainType::MAIN,
                              TestOpts opts = {})
        : BasicTestingSetup{chainType, opts}
    {
        SeedRandomForTest(SeedRand::FIXED_SEED);

        const auto cores{GetNumCores()};
        const auto num_txs{m_rng.randrange(cores * 10)};
        m_block = std::make_unique<CBlock>(CreateBlock(num_txs));
    }

    const CBlock& getBlock() { return *m_block; }
};

void PopulateCache(const CBlock& block, CCoinsViewCache& cache, bool spent = false)
{
    for (const auto& tx : block.vtx) {
        for (const auto& in : tx->vin) {
            auto outpoint{in.prevout};
            Coin coin{};
            if (!spent) coin.out.nValue = 1;
            BOOST_CHECK(spent ? coin.IsSpent() : !coin.IsSpent());
            cache.EmplaceCoinInternalDANGER(std::move(outpoint), std::move(coin));
        }
    }
}

void CheckCache(const CBlock& block, const CCoinsViewCache& cache)
{
    uint32_t counter{0};
    std::unordered_set<Txid, SaltedTxidHasher> txids{};
    txids.reserve(block.vtx.size() - 1);

    for (const auto& tx : block.vtx) {
        if (tx->IsCoinBase()) {
            BOOST_CHECK(!cache.GetPossiblySpentCoinFromCache(tx->vin[0].prevout));
        } else {
            for (const auto& in : tx->vin) {
                const auto& outpoint{in.prevout};
                const auto should_have{!txids.contains(outpoint.hash)};
                if (should_have) {
                    cache.AccessCoin(outpoint);
                    ++counter;
                }
                const auto have{cache.GetPossiblySpentCoinFromCache(outpoint)};
                BOOST_CHECK(should_have ? !!have : !have);
            }
            txids.emplace(tx->GetHash());
        }
    }
    BOOST_CHECK(cache.GetCacheSize() == counter);
}


BOOST_FIXTURE_TEST_CASE(fetch_inputs_from_db, InputFetcherTest)
{
    const auto& block{getBlock()};
    NoAccessCoinsView dummy;
    CCoinsViewCache db(&dummy);
    PopulateCache(block, db);
    CCoinsViewCache main_cache(&dummy);
    InputFetcher view{1, main_cache, db};
    for (auto i{0}; i < 3; ++i) {
        view.StartFetching(block);
        CheckCache(block, view);
        view.Reset();
    }
}

BOOST_FIXTURE_TEST_CASE(fetch_inputs_from_cache, InputFetcherTest)
{
    const auto& block{getBlock()};
    NoAccessCoinsView dummy;
    CCoinsViewCache db(&dummy);
    CCoinsViewCache main_cache(&dummy);
    PopulateCache(block, main_cache);
    InputFetcher view{1, main_cache, db};
    for (auto i{0}; i < 3; ++i) {
        view.StartFetching(block);
        CheckCache(block, view);
        view.Reset();
    }
}

// Test for the case where a block spends coins that are spent in the cache, but
// the spentness has not been flushed to the db.
BOOST_FIXTURE_TEST_CASE(fetch_no_double_spend, InputFetcherTest)
{
    const auto& block{getBlock()};
    NoAccessCoinsView dummy;
    CCoinsViewCache db(&dummy);
    PopulateCache(block, db);
    CCoinsViewCache main_cache(&dummy);
    // Add all inputs as spent already in cache
    PopulateCache(block, main_cache, /*spent=*/true);
    InputFetcher view{1, main_cache, db};
    for (auto i{0}; i < 3; ++i) {
        view.StartFetching(block);
        for (const auto& tx : block.vtx) {
            for (const auto& in : tx->vin) view.AccessCoin(in.prevout);
        }
        // Coins are not added to the view, even though they exist unspent in the parent db
        BOOST_CHECK(view.GetCacheSize() == 0);
        view.Reset();
    }
}

BOOST_FIXTURE_TEST_CASE(fetch_no_inputs, InputFetcherTest)
{
    const auto& block{getBlock()};
    CCoinsView db;
    CCoinsViewCache main_cache(&db);
    InputFetcher view{1, main_cache, db};
    for (auto i{0}; i < 3; ++i) {
        view.StartFetching(block);
        for (const auto& tx : block.vtx) {
            for (const auto& in : tx->vin) view.AccessCoin(in.prevout);
        }
        BOOST_CHECK(view.GetCacheSize() == 0);
        view.Reset();
    }
}

BOOST_FIXTURE_TEST_CASE(fetch_with_zero_workers, InputFetcherTest)
{
    const auto& block{getBlock()};
    NoAccessCoinsView dummy;
    CCoinsViewCache db(&dummy);
    PopulateCache(block, db);
    CCoinsViewCache main_cache(&dummy);
    InputFetcher view{0, main_cache, db};
    for (auto i{0}; i < 3; ++i) {
        view.StartFetching(block);
        CheckCache(block, view);
        view.Reset();
    }
}

BOOST_AUTO_TEST_SUITE_END()
