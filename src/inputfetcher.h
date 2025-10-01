// Copyright (c) 2024-present The Bitcoin Core developers
// Distributed under the MIT software license, see the accompanying
// file COPYING or http://www.opensource.org/licenses/mit-license.php.

#ifndef BITCOIN_INPUTFETCHER_H
#define BITCOIN_INPUTFETCHER_H

#include <coins.h>
#include <logging.h>
#include <primitives/transaction_identifier.h>
#include <tinyformat.h>
#include <txdb.h>
#include <util/hasher.h>
#include <util/threadnames.h>
#include <util/time.h>

#include <cstdint>
#include <semaphore>
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
    //! Counting semaphore to coordinate work between threads
    std::counting_semaphore<> m_work_semaphore{0};
    std::counting_semaphore<> m_complete_semaphore{0};
    std::vector<std::pair<size_t, size_t>> m_outpoints{};

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

    //! The maximum number of outpoints to be assigned in one batch
    const size_t m_batch_size;
    //! DB coins view to fetch from.
    const CCoinsView* m_db{nullptr};
    //! The cache to check if we already have this input.
    const CCoinsViewCache* m_cache{nullptr};
    const CBlock* m_block{nullptr};

    std::vector<std::thread> m_worker_threads;
    std::atomic<bool> m_request_stop{false};

    //! Internal function that does the fetching from disk.
    void Loop(int32_t index, bool is_main_thread = false) noexcept
    {
        do {
            // Try to acquire work from the semaphore
            if (!is_main_thread) {
                m_work_semaphore.acquire();
                if (m_request_stop.load(std::memory_order_relaxed)) {
                    return;
                }
            }
            
            while (true) {
                const auto start{m_last_tx_index.fetch_sub(m_batch_size, std::memory_order_relaxed)};
                if (start <= 0) {
                    // No more work available
                    if (is_main_thread) {
                        return;
                    } else {
                        // Worker thread is done, signal completion
                        m_complete_semaphore.release();
                        break;
                    }
                }

                auto& local_pairs{m_pairs[index]};
                try {
                    for (auto i{start - 1}; i >= std::max<int64_t>(start - m_batch_size, 0); --i) {
                        const auto [tx_index, vin_index] = m_outpoints[i];
                        const auto& outpoint{m_block->vtx[tx_index]->vin[vin_index].prevout};
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
                            break;
                        }
                    }
                } catch (const std::runtime_error&) {
                    // Database error. This will be handled later in validation.
                    // Skip remaining outpoints and continue so main thread
                    // can proceed.
                    m_last_tx_index.store(0, std::memory_order_relaxed);
                }
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
            for (size_t j{0}; j < tx->vin.size(); ++j) {
                m_outpoints.emplace_back(i, j);
            }
            m_txids.emplace(tx->GetHash());
        }
        const auto time_build_vectors_end{SteadyClock::now()};
        LogDebug(BCLog::BENCH, "    - Build m_txids and m_outpoints vectors: %.2fms\n",
                 Ticks<MillisecondsDouble>(time_build_vectors_end - time_build_vectors_start));

        // Initialize work counter and completion tracking
        m_last_tx_index.store(m_outpoints.size(), std::memory_order_relaxed);
        
        // Signal all worker threads that work is available
        // We need to signal enough times for all worker threads + main thread
        const size_t total_threads = m_worker_threads.size();
        for (size_t i = 0; i < total_threads; ++i) {
            m_work_semaphore.release();
        }

        // Have the main thread work too while we wait for other threads
        const auto time_loop_start{SteadyClock::now()};
        Loop(m_worker_threads.size(), /*is_main_thread=*/true);
        
        // Wait for all worker threads to complete
        for (size_t i = 0; i < total_threads; ++i) {
            m_complete_semaphore.acquire();
        }
        
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
        m_outpoints.clear();
    }

    ~InputFetcher()
    {
        // Signal all threads to stop
        m_request_stop.store(true, std::memory_order_relaxed);
        
        // Signal all worker threads to wake up and check the stop flag
        const size_t total_threads = m_worker_threads.size();
        for (size_t i = 0; i < total_threads; ++i) {
            m_work_semaphore.release();
        }
        
        for (std::thread& t : m_worker_threads) {
            t.join();
        }
    }
};

#endif // BITCOIN_INPUTFETCHER_H
