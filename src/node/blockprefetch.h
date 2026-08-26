// Copyright (c) 2026-present The Bitcoin Core developers
// Distributed under the MIT software license, see the accompanying
// file COPYING or http://www.opensource.org/licenses/mit-license.php.

#ifndef BITCOIN_NODE_BLOCKPREFETCH_H
#define BITCOIN_NODE_BLOCKPREFETCH_H

#include <chain.h>
#include <kernel/cs_main.h>
#include <node/blockstorage.h>
#include <primitives/block.h>
#include <sync.h>
#include <uint256.h>
#include <util/threadpool.h>

#include <deque>
#include <future>
#include <memory>
#include <string>
#include <utility>

namespace node {

/**
 * Queues sequential block reads on a single worker so a consumer can overlap
 * disk I/O with CPU work.
 *
 * Used by chain activation and by index catch-up. Each consumer owns its own
 * instance; sharing one queue would mix heights. Destruction waits for any
 * queued reads.
 */
class BlockPrefetcher
{
    static constexpr uint32_t QUEUE_SIZE{2};

    const BlockManager& m_blockman;
    ThreadPool m_pool;
    std::deque<std::future<std::shared_ptr<const CBlock>>> m_followups GUARDED_BY(::cs_main);

    static bool ShouldEnqueue(const CBlockIndex* index) EXCLUSIVE_LOCKS_REQUIRED(::cs_main)
    {
        return index && (index->nStatus & BLOCK_HAVE_DATA);
    }

    std::shared_ptr<const CBlock> PopFollowup() EXCLUSIVE_LOCKS_REQUIRED(::cs_main)
    {
        if (m_followups.empty()) return nullptr;
        auto followup{std::move(m_followups.front())};
        m_followups.pop_front();
        return followup.get();
    }

    bool Enqueue(const CBlockIndex& index) EXCLUSIVE_LOCKS_REQUIRED(::cs_main)
    {
        if (m_pool.WorkersCount() == 0) m_pool.Start(1);
        auto followup{m_pool.Submit([&blockman = m_blockman, hash = index.GetBlockHash(), pos = index.GetBlockPos()]() -> std::shared_ptr<const CBlock> {
            if (auto block{std::make_shared<CBlock>()}; blockman.ReadBlock(*block, pos, hash)) return block;
            return nullptr;
        })};
        if (followup) m_followups.emplace_back(std::move(*followup));
        return !!followup;
    }

public:
    explicit BlockPrefetcher(const BlockManager& blockman, std::string thread_name = "blockread")
        : m_blockman{blockman}, m_pool{std::move(thread_name)} {}

    void Clear() EXCLUSIVE_LOCKS_REQUIRED(::cs_main) { m_followups.clear(); }

    std::shared_ptr<const CBlock> Load(const uint256& hash) EXCLUSIVE_LOCKS_REQUIRED(::cs_main)
    {
        if (auto block{PopFollowup()}; block && block->GetHash() == hash) return block;
        return nullptr;
    }

    /** Queue ancestors of last_index starting at next_height, up to QUEUE_SIZE. */
    void FillQueue(const CBlockIndex& last_index, int next_height) EXCLUSIVE_LOCKS_REQUIRED(::cs_main)
    {
        AssertLockHeld(::cs_main);
        for (size_t i{m_followups.size()}; i < QUEUE_SIZE; ++i) {
            const auto* next{last_index.GetAncestor(next_height + i)};
            if (!ShouldEnqueue(next) || !Enqueue(*next)) break;
        }
    }
};

} // namespace node

#endif // BITCOIN_NODE_BLOCKPREFETCH_H
