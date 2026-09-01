// Copyright (c) The Bitcoin Core developers
// Distributed under the MIT software license, see the accompanying
// file COPYING or http://www.opensource.org/licenses/mit-license.php.

#ifndef BITCOIN_INDEX_TXOSPENDERINDEX_KEY_H
#define BITCOIN_INDEX_TXOSPENDERINDEX_KEY_H

#include <crypto/siphash.h>
#include <index/disktxpos.h>
#include <index/txindex_key.h>
#include <primitives/transaction.h>
#include <serialize.h>

#include <cstdint>
#include <ios>
#include <string>

namespace txospenderindex {
/*
 * Database layout:
 *
 *   ['x', hash prefix, height, tx offset] -> (empty)   // hashed, same encoding as txindex::DBKey
 *   ["outpoint_hash_salt"]                -> hasher salt
 *   ["best_block_v2"]                     -> current sync locator
 *   ['s', siphash(outpoint), CDiskTxPos]  -> legacy empty value
 *   ["siphash_key"]                       -> legacy hasher salt
 *   ['B']                                 -> legacy sync locator
 *
 * Hash prefix is SipHash-1-3-UJ of (txid jumbo, vout), truncated to 5 bytes.
 * Height is stored in txindex::BlockTxPosition::block_seq. This index removes
 * disconnected blocks, so that field is the active-chain height.
 */

//! Prefix of a legacy (pre-hashing) spender-index row.
constexpr uint8_t DB_TXOSPENDERINDEX{'s'};
inline const std::string DB_OUTPOINT_HASH_SALT{"outpoint_hash_salt"};
inline const std::string DB_BEST_BLOCK_V2{"best_block_v2"};
inline const std::string DB_LEGACY_SIPHASH_KEY{"siphash_key"};

inline txindex::TxHashKeyPrefix CreateKeyPrefix(const SipHasher13UJ& hasher, const COutPoint& outpoint)
{
    return hasher.Hash(outpoint.hash.ToUint256(), outpoint.n) >> (8 * (sizeof(txindex::TxHashKeyPrefix) - txindex::HASH_PREFIX_SIZE));
}

//! Key of a legacy spender-index row: full 8-byte siphash of the outpoint plus CDiskTxPos.
struct LegacyDBKey {
    uint64_t hash{0};
    CDiskTxPos pos;

    SERIALIZE_METHODS(LegacyDBKey, obj)
    {
        uint8_t prefix{DB_TXOSPENDERINDEX};
        READWRITE(prefix);
        if (ser_action.ForRead() && prefix != DB_TXOSPENDERINDEX) {
            throw std::ios_base::failure("Invalid format for spender index DB key");
        }
        READWRITE(obj.hash);
        READWRITE(obj.pos);
    }
};

} // namespace txospenderindex

#endif // BITCOIN_INDEX_TXOSPENDERINDEX_KEY_H
