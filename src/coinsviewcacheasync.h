// Copyright (c) The Bitcoin Core developers
// Distributed under the MIT software license, see the accompanying
// file COPYING or http://www.opensource.org/licenses/mit-license.php.

#ifndef BITCOIN_COINSVIEWCACHEASYNC_H
#define BITCOIN_COINSVIEWCACHEASYNC_H

#include <attributes.h>
#include <coins.h>
#include <primitives/block.h>
#include <primitives/transaction.h>

#include <cstdint>
#include <optional>
#include <ranges>
#include <utility>
#include <vector>

/**
 * CCoinsViewCache subclass that fetches all block inputs before ConnectBlock without mutating the base cache.
 * Only used in ConnectBlock to pass as an ephemeral view that can be reset if the block is invalid.
 * It provides the same interface as CCoinsViewCache, overriding the FetchCoin private method and Reset.
 * It adds an additional StartFetching method to provide the block.
 */
class CoinsViewCacheAsync : public CCoinsViewCache
{
public:
    /**
     * RAII-style controller that guarantees fetching is stopped when it goes out of scope.
     * Returned by StartFetching() and bound to the lifetime of the block.
     * Non-copyable and non-movable to prevent scope escape.
     */
    class FetchControl
    {
    private:
        friend class CoinsViewCacheAsync;
        CoinsViewCacheAsync& m_cache;
        explicit FetchControl(CoinsViewCacheAsync& cache LIFETIMEBOUND) noexcept : m_cache{cache} {}

    public:
        FetchControl(const FetchControl&) = delete;
        FetchControl& operator=(const FetchControl&) = delete;
        FetchControl(FetchControl&&) = delete;
        FetchControl& operator=(FetchControl&&) = delete;

        ~FetchControl()
        {
            m_cache.StopFetching();
        }
    };

private:
    //! The latest input not yet being fetched.
    mutable uint32_t m_input_head{0};
    //! The latest input not yet accessed by a consumer.
    mutable uint32_t m_input_tail{0};

    //! The inputs of the block which is being fetched.
    struct InputToFetch {
        //! The outpoint of the input to fetch.
        const COutPoint& outpoint;
        //! The coin that will be fetched.
        std::optional<Coin> coin{std::nullopt};

        /**
         * We only move when m_inputs reallocates during setup.
         * We never move after work begins, so we don't have to copy other members.
         */
        InputToFetch(InputToFetch&& other) noexcept : outpoint{other.outpoint} {}
        explicit InputToFetch(const COutPoint& o LIFETIMEBOUND) noexcept : outpoint{o} {}
    };
    mutable std::vector<InputToFetch> m_inputs{};

    /**
     * Claim and fetch the next input in the queue.
     *
     * @return true if there are more inputs in the queue to fetch
     * @return false if there are no more inputs in the queue to fetch
     */
    bool ProcessInput() const noexcept
    {
        const auto i{m_input_head++};
        if (i >= m_inputs.size()) [[unlikely]] return false;

        auto& input{m_inputs[i]};
        if (auto coin{base->PeekCoin(input.outpoint)}) [[likely]] input.coin.emplace(std::move(*coin));
        return true;
    }

    std::optional<Coin> GetCoinFromBase(const COutPoint& outpoint) const override
    {
        // This assumes ConnectBlock accesses all inputs in the same order as they are added to m_inputs
        // in StartFetching. Some outpoints are not accessed because they are created by the block, so we scan until we
        // come across the requested input.
        for (const auto i : std::views::iota(m_input_tail, m_inputs.size())) [[likely]] {
            auto& input{m_inputs[i]};
            if (input.outpoint != outpoint) continue;
            // We advance the tail since the input is cached and not accessed through this method again.
            m_input_tail = i + 1;
            // We can move the coin since we won't access this input again.
            if (input.coin) [[likely]] return std::move(*input.coin);
            // This block has missing or spent inputs.
            break;
        }

        // We will only get in here for BIP30 checks or a block with missing or spent inputs.
        return base->PeekCoin(outpoint);
    }

    //! Stop fetching and clear state.
    void StopFetching() noexcept
    {
        m_inputs.clear();
        m_input_head = 0;
        m_input_tail = 0;
    }

public:
    //! Start fetching all block inputs and return RAII guard that stops fetching on destruction.
    [[nodiscard]] FetchControl StartFetching(const CBlock& block LIFETIMEBOUND) noexcept
    {
        StopFetching();
        // Loop through the inputs of the block and set them in the queue.
        for (const auto& tx : block.vtx | std::views::drop(1)) [[likely]] {
            for (const auto& input : tx->vin) [[likely]] m_inputs.emplace_back(input.prevout);
        }
        // Don't start if there's nothing to fetch.
        if (!m_inputs.empty()) [[likely]] {
            while (ProcessInput()) [[likely]] {}
        }
        return FetchControl{*this};
    }

    void Reset() noexcept override
    {
        StopFetching();
        CCoinsViewCache::Reset();
    }

    using CCoinsViewCache::CCoinsViewCache;
};

#endif // BITCOIN_COINSVIEWCACHEASYNC_H
