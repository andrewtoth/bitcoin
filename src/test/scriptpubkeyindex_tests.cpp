// Copyright (c) The Bitcoin Core developers
// Distributed under the MIT software license, see the accompanying
// file COPYING or http://www.opensource.org/licenses/mit-license.php.

#include <addresstype.h>
#include <consensus/amount.h>
#include <index/ordered_varint.h>
#include <index/scriptpubkeyindex.h>
#include <index/scriptpubkeyindex_key.h>
#include <interfaces/chain.h>
#include <key.h>
#include <primitives/transaction.h>
#include <script/interpreter.h>
#include <script/script.h>
#include <streams.h>
#include <test/util/setup_common.h>
#include <uint256.h>
#include <util/byte_units.h>
#include <validation.h>

#include <algorithm>
#include <cstdint>
#include <vector>

#include <boost/test/unit_test.hpp>

BOOST_AUTO_TEST_SUITE(scriptpubkeyindex_tests)

static std::vector<std::byte> EncodeOrderedVarInt(uint64_t n)
{
    DataStream ss;
    ss << Using<OrderedVarIntFormatter>(n);
    return {ss.begin(), ss.end()};
}

static uint64_t DecodeOrderedVarInt(const std::vector<std::byte>& bytes)
{
    DataStream ss{bytes};
    uint64_t n{0};
    ss >> Using<OrderedVarIntFormatter>(n);
    BOOST_CHECK(ss.empty());
    return n;
}

BOOST_AUTO_TEST_CASE(ordered_varint_size_and_roundtrip)
{
    const struct {
        uint64_t value;
        size_t encoded_size;
    } cases[]{
        {0, 1},
        {127, 1},
        {128, 2},
        {16511, 2},
        {16512, 3},
        {2'000'000, 3},
        {2'113'663, 3},
        {2'113'664, 4},
        {270'549'119, 4},
        {270'549'120, 5},
        {4'000'000'000ULL, 5},
    };
    for (const auto& [value, encoded_size] : cases) {
        const auto encoded{EncodeOrderedVarInt(value)};
        BOOST_CHECK_EQUAL(encoded.size(), encoded_size);
        BOOST_CHECK_EQUAL(DecodeOrderedVarInt(encoded), value);
    }
}

BOOST_AUTO_TEST_CASE(ordered_varint_lexicographic_order)
{
    const uint64_t values[]{
        0, 1, 126, 127, 128, 255, 16'511, 16'512, 16'513,
        2'000'000, 2'113'663, 2'113'664, 270'549'119, 270'549'120, 4'000'000'000ULL,
    };
    for (size_t i{0}; i < std::size(values); ++i) {
        for (size_t j{0}; j < std::size(values); ++j) {
            const auto left{EncodeOrderedVarInt(values[i])};
            const auto right{EncodeOrderedVarInt(values[j])};
            BOOST_CHECK_EQUAL(left < right, values[i] < values[j]);
            BOOST_CHECK_EQUAL(left == right, values[i] == values[j]);
        }
    }
}

BOOST_FIXTURE_TEST_CASE(scriptpubkeyindex_initial_sync, TestChain100Setup)
{
    const CScript& coinbase_script{m_coinbase_txns[0]->vout[0].scriptPubKey};
    const uint256 script_hash{scriptpubkeyindex::Sha256ScriptPubKey(coinbase_script)};
    const uint256 tip_hash{WITH_LOCK(::cs_main, return m_node.chainman->ActiveTip()->GetBlockHash())};
    const int tip_height{WITH_LOCK(::cs_main, return m_node.chainman->ActiveHeight())};

    ScriptPubKeyIndex index(interfaces::MakeChain(m_node), 1_MiB, /*f_memory=*/true);
    BOOST_REQUIRE(index.Init());
    BOOST_CHECK(!index.BlockUntilSyncedToCurrentChain());
    BOOST_CHECK(index.Find(script_hash, /*start_height=*/0, tip_height).empty());

    index.Sync();
    BOOST_CHECK(index.GetSummary().best_block_hash == tip_hash);

    const auto all{index.Find(script_hash, /*start_height=*/0, tip_height)};
    BOOST_CHECK_EQUAL(all.size(), m_coinbase_txns.size());
    for (const auto& match : all) {
        BOOST_CHECK_EQUAL(match.vouts.size(), 1);
        BOOST_CHECK(match.tx->vout[match.vouts[0]].scriptPubKey == coinbase_script);
    }

    const auto ranged{index.Find(script_hash, /*start_height=*/90, /*end_height=*/95)};
    BOOST_REQUIRE_EQUAL(ranged.size(), 6);
    for (const auto& match : ranged) {
        BOOST_CHECK_GE(match.height, 90);
        BOOST_CHECK_LE(match.height, 95);
    }

    const auto by_script{index.Find(coinbase_script, /*start_height=*/90, /*end_height=*/95)};
    BOOST_CHECK_EQUAL(by_script.size(), ranged.size());

    index.Stop();
}

BOOST_FIXTURE_TEST_CASE(scriptpubkeyindex_multi_output_and_spend, TestChain100Setup)
{
    const CScript& coinbase_script{m_coinbase_txns[0]->vout[0].scriptPubKey};
    const CScript dest_script{GetScriptForDestination(PKHash(GenerateRandomKey().GetPubKey()))};

    CMutableTransaction tx;
    tx.version = 1;
    tx.vin.resize(1);
    tx.vin[0].prevout = COutPoint{m_coinbase_txns[0]->GetHash(), 0};
    tx.vout.resize(2);
    tx.vout[0].nValue = 25 * COIN;
    tx.vout[0].scriptPubKey = dest_script;
    tx.vout[1].nValue = 24 * COIN;
    tx.vout[1].scriptPubKey = dest_script;
    std::vector<unsigned char> vchSig;
    const uint256 hash{SignatureHash(coinbase_script, tx, 0, SIGHASH_ALL, 0, SigVersion::BASE)};
    BOOST_REQUIRE(coinbaseKey.Sign(hash, vchSig));
    vchSig.push_back(static_cast<unsigned char>(SIGHASH_ALL));
    tx.vin[0].scriptSig << vchSig;

    const uint256 tip_hash{CreateAndProcessBlock({tx}, coinbase_script).GetHash()};
    m_node.validation_signals->SyncWithValidationInterfaceQueue();

    ScriptPubKeyIndex index(interfaces::MakeChain(m_node), 1_MiB, /*f_memory=*/true);
    BOOST_REQUIRE(index.Init());
    index.Sync();
    BOOST_CHECK(index.GetSummary().best_block_hash == tip_hash);

    const int tip_height{WITH_LOCK(::cs_main, return m_node.chainman->ActiveHeight())};
    const auto matches{index.Find(dest_script, /*start_height=*/0, tip_height)};
    BOOST_REQUIRE_EQUAL(matches.size(), 1);
    BOOST_CHECK(matches[0].tx->GetHash() == tx.GetHash());
    BOOST_REQUIRE_EQUAL(matches[0].vouts.size(), 2);
    BOOST_CHECK_EQUAL(matches[0].vouts[0], 0);
    BOOST_CHECK_EQUAL(matches[0].vouts[1], 1);

    index.Stop();
}

BOOST_AUTO_TEST_SUITE_END()
