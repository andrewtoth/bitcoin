// Copyright (c) The Bitcoin Core developers
// Distributed under the MIT software license, see the accompanying
// file COPYING or http://www.opensource.org/licenses/mit-license.php.

#include <bench/bench.h>
#include <bench/data/block413567.raw.h>
#include <coins.h>
#include <common/system.h>
#include <inputfetcher.h>
#include <primitives/block.h>
#include <serialize.h>
#include <streams.h>
#include <util/time.h>

static constexpr auto DELAY{1ms};

//! Simulates a DB by adding a delay when calling GetCoin
struct DelayedCoinsView : CCoinsView {
    std::optional<Coin> GetCoin(const COutPoint&) const override
    {
        UninterruptibleSleep(DELAY);
        Coin coin{};
        coin.out.nValue = 1;
        return coin;
    }
};

static void InputFetcherBenchmark(benchmark::Bench& bench)
{
    CBlock block;
    DataStream{benchmark::data::block413567} >> TX_WITH_WITNESS(block);

    DelayedCoinsView db{};
    CCoinsViewCache main_cache(&db);
    InputFetcher fetcher{3, main_cache, db};

    bench.run([&] {
        fetcher.StartFetching(block);
        fetcher.Reset();
    });
}

BENCHMARK(InputFetcherBenchmark, benchmark::PriorityLevel::HIGH);
