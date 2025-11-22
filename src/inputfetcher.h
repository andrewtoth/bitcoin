// Copyright (c) The Bitcoin Core developers
// Distributed under the MIT software license, see the accompanying
// file COPYING or http://www.opensource.org/licenses/mit-license.php.

#ifndef BITCOIN_INPUTFETCHER_H
#define BITCOIN_INPUTFETCHER_H

#include <attributes.h>
#include <coins.h>
#include <logging.h>
#include <primitives/block.h>
#include <primitives/transaction.h>
#include <tinyformat.h>
#include <util/threadnames.h>

#include <algorithm>
#include <atomic>
#include <barrier>
#include <cstdint>
#include <optional>
#include <stdexcept>
#include <thread>
#include <unordered_map>
#include <utility>
#include <vector>

/**
 * Helper for fetching inputs from the CoinsDB and CoinsTip and inserting them
 * into the ephemeral cache used in ConnectBlock.
 *
 * It spawns a fixed set of worker threads that fetch Coins for each input
 * in a block. The Coin is moved into the Input struct and then the ready flag
 * is atomically updated to true. The main thread waits on the ready flag
 * until it is true and then inserts it into the temporary cache.
 *
 * Worker threads are synchronized with the main thread using a barrier, which
 * is used at the beginning of fetching to start the workers and at the end to
 * make sure all workers have exited the work loop.
 */
class InputFetcher : public CCoinsViewCache
{
private:
    //! The latest input being fetched. Workers atomically increment this when fetching.
    mutable std::atomic_uint32_t m_input_counter{0};

    //! The inputs of the block which is being fetched.
    struct InputToFetch {
        //! Workers set this after setting the coin. The main thread tests this before reading the coin.
        std::atomic_flag ready{};
        //! The outpoint of the input to fetch;
        const COutPoint& outpoint;
        //! The coin that workers will fetch and main thread will insert into cache.
        std::optional<Coin> coin{std::nullopt};

        /**
         * We only move when m_inputs reallocates during setup.
         * We never move after work begins, so we don't have to copy other members.
         */
        InputToFetch(InputToFetch&& other) noexcept : outpoint{other.outpoint} {}
        explicit InputToFetch(const COutPoint& o LIFETIMEBOUND) noexcept : outpoint{o} {}
    };
    mutable std::vector<InputToFetch> m_inputs{};
    std::unordered_map<std::reference_wrapper<const COutPoint>, uint32_t, SaltedOutpointHasher> m_inputs_map{};

    /**
     * The set of first 8 bytes of txids of all txs in the block being fetched. This is used to filter out inputs that
     * are created and spent in the same block, since they will not be in the db or the cache.
     */
    std::vector<uint64_t> m_txids{};

    //! DB coins view to fetch from.
    const CCoinsView& m_db;

    /**
     * Fetches the next input in the queue. Safe to call from any thread once inside the barrier.
     *
     * @return true if there are more inputs in the queue to fetch
     * @return false if there are no more inputs in the queue to fetch
     */
    bool FetchCoinInBackground() const noexcept
    {
        const auto i{m_input_counter.fetch_add(1, std::memory_order_relaxed)};
        if (i >= m_inputs.size()) [[unlikely]] return false;
        auto& input{m_inputs[i]};
        // Inputs spending a coin from a tx earlier in the block won't be in the cache or db
        if (std::ranges::binary_search(m_txids, input.outpoint.hash.ToUint256().GetUint64(0))) {
            // We can use relaxed ordering here since we don't write the coin.
            input.ready.test_and_set(std::memory_order_relaxed);
            input.ready.notify_one();
            return true;
        }
        auto coin{static_cast<CCoinsViewCache*>(base)->GetPossiblySpentCoinFromCache(input.outpoint)};
        if (!coin) {
            try {
                coin = m_db.GetCoin(input.outpoint);
            } catch (const std::runtime_error& e) {
                LogPrintLevel(BCLog::VALIDATION, BCLog::Level::Warning, "InputFetcher failed to fetch input: %s.", e.what());
            }
        }
        if (coin && !coin->IsSpent()) [[likely]] input.coin.emplace(std::move(*coin));
        // We need release here, so writing coin in the line above happens before the main thread acquires.
        input.ready.test_and_set(std::memory_order_release);
        input.ready.notify_one();
        return true;
    }

    CCoinsMap::iterator FetchCoin(const COutPoint &outpoint) const override
    {
        const auto [ret, inserted] = cacheCoins.try_emplace(outpoint);
        if (inserted) {
            if (const auto& it{m_inputs_map.find(outpoint)}; it != m_inputs_map.end()) [[likely]] {
                auto& input{m_inputs[it->second]};
                // Check if the coin is ready to be read. We need to acquire to match the worker thread's release.
                while (!input.ready.test(std::memory_order_acquire)) {
                    // Work instead of waiting if the coin is not ready
                    if (!FetchCoinInBackground()) {
                        // No more work, just wait
                        input.ready.wait(/*old=*/false, std::memory_order_acquire);
                        break;
                    }
                }
                if (input.coin) [[likely]] ret->second.coin = std::move(*input.coin);
            }
            if (ret->second.coin.IsSpent()) [[unlikely]] {
                // We will only get in here for BIP30 checks, txid collisions, or a block with missing or spent inputs.
                // We need to wait for all threads to finish before we can mutate base.
                FinishFetching();
                if (auto coin{base->GetCoin(outpoint)}) {
                    ret->second.coin = std::move(*coin);
                } else {
                    cacheCoins.erase(ret);
                    return cacheCoins.end();
                }
            }
            cachedCoinsUsage += ret->second.coin.DynamicMemoryUsage();
        }
        return ret;
    }

    std::vector<std::thread> m_worker_threads{};
    mutable std::barrier<> m_barrier;
    bool m_request_stop{false};

    mutable bool m_finished_fetching{false};
    void FinishFetching() const noexcept
    {
        if (m_finished_fetching) return;
        while (FetchCoinInBackground()) {}
        m_barrier.arrive_and_wait();
        m_finished_fetching = true;
    }

public:
    //! Start fetching all block inputs in parallel.
    void StartFetching(const CBlock& block) noexcept
    {
        // Loop through the inputs of the block and set them in the queue.
        // Construct the set of txids to filter, and count the outputs to reserve in cacheCoins.
        auto outputs{0};
        for (const auto& tx : block.vtx) {
            outputs += tx->vout.size();
            if (tx->IsCoinBase()) continue;
            m_txids.emplace_back(tx->GetHash().ToUint256().GetUint64(0));
            for (const auto& input : tx->vin) {
                m_inputs_map.try_emplace(std::ref(input.prevout), m_inputs.size());
                m_inputs.emplace_back(input.prevout);
            }
        }
        std::ranges::sort(m_txids);
        // Start workers.
        m_barrier.arrive_and_wait();
        cacheCoins.reserve(outputs + m_inputs.size());
    }

    void Reset() noexcept
    {
        FinishFetching();
        m_input_counter.store(0, std::memory_order_relaxed);
        m_txids.clear();
        m_inputs.clear();
        m_inputs_map.clear();
        m_finished_fetching = false;
        cacheCoins.clear();
        cachedCoinsUsage = 0;
        hashBlock = uint256::ZERO;
    }

    explicit InputFetcher(int32_t worker_thread_count, CCoinsViewCache& cache, const CCoinsView& db) noexcept
        : CCoinsViewCache{&cache}, m_db{db}, m_barrier{worker_thread_count + 1}
    {
        for (auto n{0}; n < worker_thread_count; ++n) {
            m_worker_threads.emplace_back([this, n] {
                util::ThreadRename(strprintf("inputfetch.%i", n));
                while (true) {
                    m_barrier.arrive_and_wait();
                    if (m_request_stop) [[unlikely]] return;
                    while (FetchCoinInBackground()) {}
                    m_barrier.arrive_and_wait();
                }
            });
        }
    }

    ~InputFetcher()
    {
        m_request_stop = true;
        m_barrier.arrive_and_drop();
        for (auto& t : m_worker_threads) t.join();
    }
};

#endif // BITCOIN_INPUTFETCHER_H
