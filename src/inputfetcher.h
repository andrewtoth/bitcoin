// Copyright (c) 2024-present The Bitcoin Core developers
// Distributed under the MIT software license, see the accompanying
// file COPYING or http://www.opensource.org/licenses/mit-license.php.

#ifndef BITCOIN_INPUTFETCHER_H
#define BITCOIN_INPUTFETCHER_H

#include <coins.h>
#include <logging.h>
#include <primitives/transaction_identifier.h>
#include <sync.h>
#include <tinyformat.h>
#include <txdb.h>
#include <util/hasher.h>
#include <util/threadnames.h>
#include <util/time.h>

#include <cstdint>
#include <stdexcept>
#include <thread>
#include <unordered_set>
#include <vector>

/**
 * Input fetcher for fetching inputs from the CoinsDB and inserting
 * into the CoinsTip.
 *
 * The main thread loops through the block and writes all input prevouts to a
 * global vector. It then wakes all workers and starts working as well. Each
 * thread assigns itself a range of outpoints from the shared vector, and
 * fetches the coins from disk. The outpoint and coin pairs are written to a
 * thread local vector of pairs. Once all outpoints are fetched, the main thread
 * loops through all thread local vectors and writes the pairs to the cache.
 */
class InputFetcher
{
private:
    //! Mutex to protect the inner state
    Mutex m_mutex{};
    //! Worker threads block on this when out of work
    std::condition_variable m_worker_cv{};
    //! Main thread blocks on this when out of work
    std::condition_variable m_main_cv{};

    /**
     * The index of the last outpoint that is being fetched. Workers assign
     * themselves a range of outpoints to fetch from m_outpoints. They will use
     * this index as the end of their range, and then set this index to the
     * beginning of the range they take for the next worker. Once it gets to
     * zero, all outpoints have been assigned and the next worker will wait.
     */
    std::atomic<int64_t> m_last_tx_index{0};

    //! The set of txids of the transactions in the current block being fetched.
    std::unordered_set<Txid, SaltedTxidHasher> m_txids{};
    //! The vector of thread local vectors of pairs to be written to the cache.
    std::vector<std::vector<std::pair<COutPoint, Coin>>> m_pairs{};

    //! The number of worker threads that are waiting on m_worker_cv
    std::atomic<size_t> m_idle_worker_count{0};
    //! The maximum number of outpoints to be assigned in one batch
    const size_t m_batch_size;
    //! DB coins view to fetch from.
    const CCoinsView* m_db{nullptr};
    //! The cache to check if we already have this input.
    const CCoinsViewCache* m_cache{nullptr};
    const CBlock* m_block{nullptr};

    std::vector<std::thread> m_worker_threads;
    bool m_request_stop GUARDED_BY(m_mutex){false};

    //! Internal function that does the fetching from disk.
    void Loop(int32_t index, bool is_main_thread = false) noexcept EXCLUSIVE_LOCKS_REQUIRED(!m_mutex)
    {
        auto& cond{is_main_thread ? m_main_cv : m_worker_cv};
        do {
            auto start{m_last_tx_index.fetch_sub(m_batch_size, std::memory_order_relaxed)};
            while (start <= 1) {
                if (is_main_thread && m_idle_worker_count.load(std::memory_order_relaxed) == m_worker_threads.size()) {
                    return;
                }
                {
                    WAIT_LOCK(m_mutex, lock);
                    if (m_request_stop) {
                        return;
                    }
                    if (!is_main_thread) {
                        const auto idle_worker_count{m_idle_worker_count.fetch_add(1, std::memory_order_relaxed)};
                        if (m_idle_worker_count == m_worker_threads.size() - 1) {
                            m_main_cv.notify_one();
                        }
                    }
                    cond.wait(lock);
                    if (m_request_stop) {
                        return;
                    }
                }
                if (is_main_thread) {
                    return;
                } else {
                    m_idle_worker_count.fetch_sub(1, std::memory_order_relaxed)
                }
                start = m_last_tx_index.fetch_sub(m_batch_size, std::memory_order_relaxed);
            }

            auto& local_pairs{m_pairs[index]};
            try {
                for (auto i{start - 1}; i >= std::max<int64_t>(start - m_batch_size, 1); --i) {
                    const auto& tx{m_block->vtx[i]};
                    for (size_t j{0}; j < tx->vin.size(); ++j) {
                        const auto& outpoint{tx->vin[j].prevout};
                        // If an input spends an outpoint from earlier in the
                        // block, it won't be in the cache yet but it also won't be
                        // in the db either.
                        if (m_txids.contains(outpoint.hash)) {
                            continue;
                        }
                        if (m_cache->HaveCoinInCache(outpoint)) {
                            continue;
                        }
                        if (auto coin{m_db->GetCoin(outpoint)}; coin) {
                            local_pairs.emplace_back(outpoint, std::move(*coin));
                        } else {
                            // Missing an input. This block will fail validation.
                            // Skip remaining outpoints and continue so main thread
                            // can proceed.
                            m_last_tx_index.store(0, std::memory_order_relaxed);
                            i = 0;
                            break;
                        }
                    }
                }
            } catch (const std::runtime_error&) {
                // Database error. This will be handled later in validation.
                // Skip remaining outpoints and continue so main thread
                // can proceed.
                m_last_tx_index.store(0, std::memory_order_relaxed);
            }
        } while (true);
    }

public:

    //! Create a new input fetcher
    explicit InputFetcher(int32_t batch_size, int32_t worker_thread_count) noexcept
        : m_batch_size(batch_size)
    {
        if (worker_thread_count < 1) {
            // Don't do anything if there are no worker threads.
            return;
        }
        m_pairs.reserve(worker_thread_count + 1);
        for (auto n{0}; n < worker_thread_count + 1; ++n) {
            m_pairs.emplace_back();
        }
        m_worker_threads.reserve(worker_thread_count);
        for (auto n{0}; n < worker_thread_count; ++n) {
            m_worker_threads.emplace_back([this, n]() {
                util::ThreadRename(strprintf("inputfetch.%i", n));
                Loop(n);
            });
        }
    }

    // Since this class manages its own resources, which is a thread
    // pool `m_worker_threads`, copy and move operations are not appropriate.
    InputFetcher(const InputFetcher&) = delete;
    InputFetcher& operator=(const InputFetcher&) = delete;
    InputFetcher(InputFetcher&&) = delete;
    InputFetcher& operator=(InputFetcher&&) = delete;

    //! Fetch all block inputs from db, and insert into cache.
    void FetchInputs(CCoinsViewCache& cache,
                     const CCoinsView& db,
                     const CBlock& block) noexcept
        EXCLUSIVE_LOCKS_REQUIRED(!m_mutex)
    {
        if (m_worker_threads.empty() || block.vtx.size() <= m_batch_size) {
            return;
        }

        // Set the db and cache to use for this block.
        m_db = &db;
        m_cache = &cache;
        m_block = &block;

        // Timing variables for debug bench logs
        const auto time_start{SteadyClock::now()};
        // Loop through the inputs of the block and add them to the queue
        const auto time_build_vectors_start{SteadyClock::now()};
        m_txids.reserve(block.vtx.size() - 1);
        for (size_t i{1}; i < block.vtx.size(); ++i) {
            const auto& tx = block.vtx[i];
            m_txids.emplace(tx->GetHash());
        }
        const auto time_build_vectors_end{SteadyClock::now()};
        LogDebug(BCLog::BENCH, "    - Build m_txids and m_outpoints vectors: %.2fms\n",
                 Ticks<MillisecondsDouble>(time_build_vectors_end - time_build_vectors_start));

        {
            LOCK(m_mutex);
            m_last_tx_index.store(block.vtx.size(), std::memory_order_relaxed);
        }
        m_worker_cv.notify_all();

        // Have the main thread work too while we wait for other threads
        const auto time_loop_start{SteadyClock::now()};
        Loop(m_worker_threads.size(), /*is_main_thread=*/true);
        const auto time_loop_end{SteadyClock::now()};
        LogDebug(BCLog::BENCH, "    - Perform Loop operation: %.2fms\n",
                 Ticks<MillisecondsDouble>(time_loop_end - time_loop_start));

        // At this point all threads are done writing to m_pairs, so we can
        // safely read from it and insert the fetched coins into the cache.
        const auto time_cache_insert_start{SteadyClock::now()};
        for (auto& local_pairs : m_pairs) {
            for (auto&& [outpoint, coin] : local_pairs) {
                cache.EmplaceCoinInternalDANGER(std::move(outpoint),
                                                std::move(coin),
                                                /*set_dirty=*/false);
            }
            local_pairs.clear();
        }
        const auto time_cache_insert_end{SteadyClock::now()};
        LogDebug(BCLog::BENCH, "    - Insert m_pairs into cache: %.2fms\n",
                 Ticks<MillisecondsDouble>(time_cache_insert_end - time_cache_insert_start));
        
        const auto time_end{SteadyClock::now()};
        LogDebug(BCLog::BENCH, "  - FetchInputs total: %.2fms\n",
                 Ticks<MillisecondsDouble>(time_end - time_start));
        
        m_txids.clear();
    }

    ~InputFetcher()
    {
        WITH_LOCK(m_mutex, m_request_stop = true);
        m_worker_cv.notify_all();
        for (std::thread& t : m_worker_threads) {
            t.join();
        }
    }
};

#endif // BITCOIN_INPUTFETCHER_H
