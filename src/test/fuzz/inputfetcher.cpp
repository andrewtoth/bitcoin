// Copyright (c) 2024-present The Bitcoin Core developers
// Distributed under the MIT software license, see the accompanying
// file COPYING or http://www.opensource.org/licenses/mit-license.php.

#include <inputfetcher.h>
#include <test/fuzz/FuzzedDataProvider.h>
#include <test/fuzz/fuzz.h>
#include <test/fuzz/util.h>
#include <util/transaction_identifier.h>

#include <cstdint>
#include <map>
#include <optional>
#include <stdexcept>
#include <utility>

using DbMap = std::map<const COutPoint, std::pair<std::optional<const Coin>, bool>>;

class FuzzCoinsView : public CCoinsView
{
private:
    DbMap& m_map;

public:
    FuzzCoinsView(DbMap& map) : m_map(map) {}

    std::optional<Coin> GetCoin(const COutPoint& outpoint) const override
    {
        const auto it{m_map.find(outpoint)};
        assert(it != m_map.end());
        const auto [coin, err] = it->second;
        if (err) {
            throw std::runtime_error("database error");
        }
        return coin;
    }
};

FUZZ_TARGET(inputfetcher)
{
    FuzzedDataProvider fuzzed_data_provider(buffer.data(), buffer.size());

    const auto batch_size{
        fuzzed_data_provider.ConsumeIntegralInRange<int32_t>(0, 1024)};
    const auto worker_threads{
        fuzzed_data_provider.ConsumeIntegralInRange<int32_t>(2, 4)};
    InputFetcher fetcher{batch_size, worker_threads};

    CBlock block;
    Txid prevhash{Txid::FromUint256(ConsumeUInt256(fuzzed_data_provider))};

    DbMap db_map{};
    std::map<const COutPoint, const Coin> cache_map{};

    FuzzCoinsView db(db_map);
    CCoinsViewCache cache(&db);

    LIMITED_WHILE(fuzzed_data_provider.ConsumeBool(), static_cast<uint32_t>(batch_size * worker_threads * 2)) {
        CMutableTransaction tx;

        LIMITED_WHILE(fuzzed_data_provider.ConsumeBool(), 10) {
            const auto txid{fuzzed_data_provider.ConsumeBool()
                ? Txid::FromUint256(ConsumeUInt256(fuzzed_data_provider))
                : prevhash};
            const auto index{fuzzed_data_provider.ConsumeIntegral<uint32_t>()};
            const COutPoint outpoint(txid, index);

            tx.vin.emplace_back(outpoint);

            std::optional<Coin> maybe_coin;
            if (fuzzed_data_provider.ConsumeBool()) {
                Coin coin{};
                coin.out.nValue = ConsumeMoney(fuzzed_data_provider);
                maybe_coin = coin;
            } else {
                maybe_coin = std::nullopt;
            }
            db_map.try_emplace(outpoint, std::make_pair(
                maybe_coin,
                fuzzed_data_provider.ConsumeBool()));

            // Add the coin to the cache
            if (fuzzed_data_provider.ConsumeBool()) {
                Coin coin{};
                coin.out.nValue = ConsumeMoney(fuzzed_data_provider);
                cache_map.try_emplace(outpoint, coin);
                cache.EmplaceCoinInternalDANGER(
                    COutPoint(outpoint),
                    std::move(coin),
                    /*set_dirty=*/fuzzed_data_provider.ConsumeBool());
            }
        }

        prevhash = tx.GetHash();
        block.vtx.push_back(MakeTransactionRef(tx));
    }

    if (block.vtx.empty()) {
        return;
    }

    fetcher.FetchInputs(cache, db, block);

    // Check pre-existing coins in the cache have not been updated
    for (const auto& [outpoint, coin] : cache_map) {
        const auto maybe_coin{cache.GetCoin(outpoint)};
        if (coin.IsSpent()) {
            assert(!maybe_coin);
        } else {
            const auto& other_coin{maybe_coin.value()};
            assert(other_coin.out == coin.out);
        }
    }

    // Check any newly added coins in the cache are the same as the db
    for (const auto& [outpoint, pair] : db_map) {
        const auto& maybe_coin{pair.first};
        if (!maybe_coin) {
            continue;
        }
        if (const auto cache_coin{cache.GetCoin(outpoint)}; cache_coin) {
            if (cache_map.find(outpoint) == cache_map.end()) {
                assert((*cache_coin).out == (*maybe_coin).out);
            }
        }
    }
}
