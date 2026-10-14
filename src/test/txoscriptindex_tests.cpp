// Copyright (c) The Bitcoin Core developers
// Distributed under the MIT software license, see the accompanying
// file COPYING or http://www.opensource.org/licenses/mit-license.php.

#include <addresstype.h>
#include <chain.h>
#include <consensus/amount.h>
#include <crypto/hex_base.h>
#include <dbwrapper.h>
#include <index/ordered_varint.h>
#include <index/txoscriptindex.h>
#include <index/txoscriptindex_key.h>
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
#include <ios>
#include <iterator>
#include <limits>
#include <string>
#include <utility>
#include <vector>

#include <boost/test/unit_test.hpp>

BOOST_AUTO_TEST_SUITE(txoscriptindex_tests)

class TxoScriptIndexTest
{
public:
    static CDBWrapper& GetDB(const TxoScriptIndex& index) { return index.GetDB(); }
};

static std::vector<std::byte> EncodeOrderedVarInt(uint64_t value)
{
    DataStream stream;
    stream << Using<OrderedVarIntFormatter>(value);
    return {stream.begin(), stream.end()};
}

static uint64_t DecodeOrderedVarInt(const std::vector<std::byte>& bytes)
{
    DataStream stream{bytes};
    uint64_t value{0};
    stream >> Using<OrderedVarIntFormatter>(value);
    BOOST_CHECK(stream.empty());
    return value;
}

static void IgnoreMatch(const CTransaction&, const uint256&) {}

BOOST_AUTO_TEST_CASE(ordered_varint_exact_encoding_and_roundtrip)
{
    const struct {
        uint64_t value;
        std::string encoded;
    } cases[]{
        {0, "00"},
        {127, "7f"},
        {128, "8000"},
        {16511, "bfff"},
        {16512, "c00000"},
        {2'000'000, "de4400"},
        {2'113'663, "dfffff"},
        {2'113'664, "e0000000"},
        {270'549'119, "efffffff"},
        {270'549'120, "f000000000"},
        {4'000'000'000ULL, "f0de4ae780"},
        {std::numeric_limits<uint32_t>::max(), "f0efdfbf7f"},
        {OrderedVarIntFormatter::MAX_5, "f7ffffffff"},
    };
    for (const auto& [value, encoded] : cases) {
        const auto bytes{EncodeOrderedVarInt(value)};
        BOOST_CHECK_EQUAL(HexStr(bytes), encoded);
        BOOST_CHECK_EQUAL(DecodeOrderedVarInt(bytes), value);
    }

    // Pin the complete persisted key format as well as the height encoding.
    BOOST_CHECK_EQUAL(HexStr(DataStream{} << txoscriptindex::DBKey{0x0102030405, 128, 81}), "7801020304058000000051");
}

BOOST_AUTO_TEST_CASE(ordered_varint_invalid_encodings)
{
    // Exercise every truncation length for each supported encoding size.
    for (const uint64_t value : {uint64_t{0}, uint64_t{128}, uint64_t{16512}, uint64_t{2'113'664}, OrderedVarIntFormatter::MAX_5}) {
        const auto bytes{EncodeOrderedVarInt(value)};
        for (size_t size{0}; size < bytes.size(); ++size) {
            BOOST_CHECK_THROW(DecodeOrderedVarInt({bytes.begin(), bytes.begin() + size}), std::ios_base::failure);
        }
    }
    for (uint32_t prefix{0xf8}; prefix <= 0xff; ++prefix) {
        BOOST_CHECK_THROW(DecodeOrderedVarInt({static_cast<std::byte>(prefix), std::byte{0}, std::byte{0}, std::byte{0}, std::byte{0}}), std::ios_base::failure);
    }
    BOOST_CHECK_THROW(EncodeOrderedVarInt(OrderedVarIntFormatter::MAX_5 + 1), std::ios_base::failure);
    BOOST_CHECK_THROW(EncodeOrderedVarInt(std::numeric_limits<uint64_t>::max()), std::ios_base::failure);

    uint32_t decoded{0};
    DataStream maximum{EncodeOrderedVarInt(std::numeric_limits<uint32_t>::max())};
    maximum >> Using<OrderedVarIntFormatter>(decoded);
    BOOST_CHECK_EQUAL(decoded, std::numeric_limits<uint32_t>::max());
    DataStream overflow{EncodeOrderedVarInt(uint64_t{std::numeric_limits<uint32_t>::max()} + 1)};
    BOOST_CHECK_THROW(overflow >> Using<OrderedVarIntFormatter>(decoded), std::ios_base::failure);
    BOOST_CHECK_EQUAL(decoded, std::numeric_limits<uint32_t>::max());
}

BOOST_AUTO_TEST_CASE(ordered_varint_lexicographic_order)
{
    const uint64_t values[]{
        0, 1, 126, 127, 128, 255, 16'511, 16'512, 16'513,
        2'000'000, 2'113'663, 2'113'664, 270'549'119, 270'549'120, 4'000'000'000ULL,
        std::numeric_limits<uint32_t>::max(), OrderedVarIntFormatter::MAX_5,
    };
    auto previous{EncodeOrderedVarInt(values[0])};
    for (size_t i{1}; i < std::size(values); ++i) {
        auto encoded{EncodeOrderedVarInt(values[i])};
        BOOST_CHECK(previous < encoded);
        previous = std::move(encoded);
    }
}

BOOST_FIXTURE_TEST_CASE(txoscriptindex_initial_sync, TestChain100Setup)
{
    const CScript& coinbase_script{m_coinbase_txns[0]->vout[0].scriptPubKey};
    const uint256 script_hash{txoscriptindex::Sha256ScriptPubKey(coinbase_script)};
    const int32_t tip_height{WITH_LOCK(::cs_main, return m_node.chainman->ActiveHeight())};

    TxoScriptIndex index{interfaces::MakeChain(m_node), 1_MiB, /*memory=*/true};
    BOOST_REQUIRE(index.Init());
    BOOST_CHECK(index.Find(script_hash, /*start_height=*/0, tip_height, IgnoreMatch).value().empty());

    index.Sync();
    const auto all{index.Find(script_hash, /*start_height=*/0, tip_height, IgnoreMatch).value()};
    BOOST_CHECK_EQUAL(all.size(), m_coinbase_txns.size());
    for (const auto& match : all) {
        BOOST_REQUIRE(match.tx);
        BOOST_REQUIRE_EQUAL(match.outputs.size(), 1);
        BOOST_CHECK(match.tx->vout[match.outputs[0]].scriptPubKey == coinbase_script);
    }

    const auto ranged{index.Find(script_hash, /*start_height=*/90, /*end_height=*/95, IgnoreMatch).value()};
    BOOST_REQUIRE_EQUAL(ranged.size(), 6);
    for (const auto& match : ranged) {
        BOOST_CHECK_GE(match.height, 90);
        BOOST_CHECK_LE(match.height, 95);
    }

    BOOST_CHECK(index.Find(script_hash, -1, 95, IgnoreMatch).value().empty());
    BOOST_CHECK(index.Find(script_hash, 95, 90, IgnoreMatch).value().empty());

    index.Stop();
}

BOOST_FIXTURE_TEST_CASE(txoscriptindex_collisions_and_pruned_blocks, TestChain100Setup)
{
    const CScript target_script{CScript() << OP_TRUE};
    const uint256 script_hash{txoscriptindex::Sha256ScriptPubKey(target_script)};
    const auto first_block{CreateAndProcessBlock({}, target_script)};
    const auto second_block{CreateAndProcessBlock({}, target_script)};
    m_node.validation_signals->SyncWithValidationInterfaceQueue();

    size_t callbacks{0};
    const auto on_match{[&](const CTransaction& tx, const uint256& block_hash) {
        BOOST_CHECK(tx.GetHash() == first_block.vtx[0]->GetHash() || tx.GetHash() == second_block.vtx[0]->GetHash());
        BOOST_CHECK(block_hash == first_block.GetHash() || block_hash == second_block.GetHash());
        ++callbacks;
    }};
    TxoScriptIndex index{interfaces::MakeChain(m_node), 1_MiB, /*memory=*/true};
    BOOST_REQUIRE(index.Init());
    index.Sync();
    CDBWrapper& db{TxoScriptIndexTest::GetDB(index)};
    std::pair<uint64_t, uint64_t> salt;
    BOOST_REQUIRE(db.Read(txoscriptindex::DB_SPK_HASH_SALT, salt));
    const auto prefix{txoscriptindex::CreateKeyPrefix(SipHasher13UJ{salt.first, salt.second}, script_hash)};

    // Inject prefix collisions pointing at unrelated coinbases before the real
    // matches. Their full script hashes must be checked before returning them.
    for (uint32_t height{1}; height <= 3; ++height) {
        db.Write(txoscriptindex::DBKey{prefix, height, 81}, txoscriptindex::EMPTY_VALUE);
    }
    // Two candidate transactions in one pruned block must produce one warning.
    db.Write(txoscriptindex::DBKey{prefix, 3, 82}, txoscriptindex::EMPTY_VALUE);
    CBlockIndex* pruned_block{WITH_LOCK(cs_main, return m_node.chainman->ActiveChain()[3])};
    const auto original_status{WITH_LOCK(cs_main, return pruned_block->nStatus)};
    WITH_LOCK(cs_main, pruned_block->nStatus &= ~BLOCK_HAVE_DATA);
    const auto matches{index.Find(script_hash, 0, 102, on_match).value()};
    BOOST_REQUIRE_EQUAL(matches.size(), 3);
    BOOST_CHECK(!matches[0].tx);
    BOOST_CHECK(matches[0].block_hash == pruned_block->GetBlockHash());
    BOOST_CHECK_EQUAL(matches[0].height, 3);
    BOOST_CHECK_EQUAL(matches[0].tx_offset, 0);
    BOOST_CHECK(matches[0].outputs.empty());
    BOOST_CHECK(matches[1].tx->GetHash() == first_block.vtx[0]->GetHash());
    BOOST_CHECK(matches[2].tx->GetHash() == second_block.vtx[0]->GetHash());
    BOOST_CHECK_EQUAL(callbacks, 2);
    BOOST_CHECK_EQUAL(index.Find(script_hash, 0, 100, on_match).value().size(), 1);
    BOOST_CHECK_EQUAL(callbacks, 2);
    WITH_LOCK(cs_main, pruned_block->nStatus = original_status);
    index.Stop();
}

BOOST_FIXTURE_TEST_CASE(txoscriptindex_read_failures, TestChain100Setup)
{
    const uint256 script_hash{txoscriptindex::Sha256ScriptPubKey(m_coinbase_txns[0]->vout[0].scriptPubKey)};
    TxoScriptIndex index{interfaces::MakeChain(m_node), 1_MiB, /*memory=*/true};
    BOOST_REQUIRE(index.Init());
    index.Sync();
    CBlockIndex* block{WITH_LOCK(cs_main, return m_node.chainman->ActiveChain()[1])};
    const auto original_file{WITH_LOCK(cs_main, return block->nFile)};
    WITH_LOCK(cs_main, block->nFile = std::numeric_limits<decltype(block->nFile)>::max());
    {
        const auto result{index.Find(script_hash, 1, 1, IgnoreMatch)};
        BOOST_REQUIRE(!result);
        BOOST_CHECK(result.error().starts_with("Unable to read txoscriptindex transaction in block " + block->GetBlockHash().ToString() + ": "));
    }
    WITH_LOCK(cs_main, block->nFile = original_file);

    // A position beyond the block file must fail explicitly, not look like an
    // empty history. Changing the data position simulates a corrupt index row.
    const auto original_pos{WITH_LOCK(cs_main, return block->nDataPos)};
    WITH_LOCK(cs_main, block->nDataPos = 1U << 30);
    {
        const auto result{index.Find(script_hash, 1, 1, IgnoreMatch)};
        BOOST_REQUIRE(!result);
        BOOST_CHECK(result.error().starts_with("Unable to read txoscriptindex transaction in block " + block->GetBlockHash().ToString() + ": "));
    }
    WITH_LOCK(cs_main, block->nDataPos = original_pos);
    BOOST_CHECK_EQUAL(index.Find(script_hash, 1, 1, IgnoreMatch).value().size(), 1);
    index.Stop();
}

BOOST_FIXTURE_TEST_CASE(txoscriptindex_multi_output, TestChain100Setup)
{
    const CScript& coinbase_script{m_coinbase_txns[0]->vout[0].scriptPubKey};
    const CScript dest_script{GetScriptForDestination(PKHash(GenerateRandomKey().GetPubKey()))};

    CMutableTransaction tx;
    tx.version = 1;
    tx.vin.resize(1);
    tx.vin[0].prevout = COutPoint{m_coinbase_txns[0]->GetHash(), 0};
    tx.vout.emplace_back(25 * COIN, dest_script);
    tx.vout.emplace_back(24 * COIN, dest_script);
    tx.vout.emplace_back(0, CScript() << OP_TRUE);
    std::vector<uint8_t> signature;
    const uint256 hash{SignatureHash(coinbase_script, tx, 0, SIGHASH_ALL, 0, SigVersion::BASE)};
    BOOST_REQUIRE(coinbaseKey.Sign(hash, signature));
    signature.push_back(static_cast<uint8_t>(SIGHASH_ALL));
    tx.vin[0].scriptSig << signature;

    const auto block{CreateAndProcessBlock({tx}, coinbase_script)};
    const uint256 tip_hash{block.GetHash()};
    m_node.validation_signals->SyncWithValidationInterfaceQueue();

    TxoScriptIndex index{interfaces::MakeChain(m_node), 1_MiB, /*memory=*/true};
    BOOST_REQUIRE(index.Init());
    index.Sync();
    const int32_t tip_height{WITH_LOCK(::cs_main, return m_node.chainman->ActiveHeight())};
    const auto matches{index.Find(txoscriptindex::Sha256ScriptPubKey(dest_script), /*start_height=*/0, tip_height, IgnoreMatch).value()};
    BOOST_REQUIRE_EQUAL(matches.size(), 1);
    BOOST_REQUIRE(matches[0].tx);
    BOOST_CHECK(matches[0].tx->GetHash() == tx.GetHash());
    BOOST_CHECK_EQUAL(matches[0].tx_offset, txoscriptindex::BLOCK_HEADER_SIZE + GetSizeOfCompactSize(block.vtx.size()) + block.vtx[0]->ComputeTotalSize());
    BOOST_REQUIRE_EQUAL(matches[0].outputs.size(), 2);
    BOOST_CHECK_EQUAL(matches[0].outputs[0], 0);
    BOOST_CHECK_EQUAL(matches[0].outputs[1], 1);

    // Each query must invoke the callback, even for another script in the same
    // transaction or a repeated lookup.
    size_t callbacks{0};
    const auto on_match{[&](const CTransaction& match, const uint256& block_hash) {
        BOOST_CHECK(match.GetHash() == tx.GetHash());
        BOOST_CHECK(block_hash == tip_hash);
        ++callbacks;
    }};
    const auto repeated{index.Find(txoscriptindex::Sha256ScriptPubKey(dest_script), 0, tip_height, on_match).value()};
    BOOST_REQUIRE_EQUAL(repeated.size(), 1);
    BOOST_CHECK(repeated[0].outputs == matches[0].outputs);
    const auto other_script{index.Find(txoscriptindex::Sha256ScriptPubKey(tx.vout[2].scriptPubKey), 0, tip_height, on_match).value()};
    BOOST_REQUIRE_EQUAL(other_script.size(), 1);
    BOOST_REQUIRE_EQUAL(other_script[0].outputs.size(), 1);
    BOOST_CHECK_EQUAL(other_script[0].outputs[0], 2);
    BOOST_CHECK_EQUAL(callbacks, 2);
    index.Stop();
}

BOOST_FIXTURE_TEST_CASE(txoscriptindex_callback_stops_lookup, TestChain100Setup)
{
    const uint256 script_hash{txoscriptindex::Sha256ScriptPubKey(m_coinbase_txns[0]->vout[0].scriptPubKey)};
    TxoScriptIndex index{interfaces::MakeChain(m_node), 1_MiB, /*memory=*/true};
    BOOST_REQUIRE(index.Init());
    index.Sync();

    // Reading the third transaction would fail. A callback throwing on the
    // second must stop before that read and propagate the original exception.
    CBlockIndex* unreadable{WITH_LOCK(cs_main, return m_node.chainman->ActiveChain()[3])};
    const auto original_pos{WITH_LOCK(cs_main, return unreadable->nDataPos)};
    WITH_LOCK(cs_main, unreadable->nDataPos = 1U << 30);
    struct StopLookup {};
    size_t callbacks{0};
    const auto on_match{[&](const CTransaction& tx, const uint256& block_hash) {
        BOOST_CHECK(tx.GetHash() == m_coinbase_txns[callbacks]->GetHash());
        BOOST_CHECK(block_hash == WITH_LOCK(cs_main, return m_node.chainman->ActiveChain()[callbacks + 1]->GetBlockHash()));
        if (++callbacks == 2) throw StopLookup{};
    }};
    BOOST_CHECK_THROW(index.Find(script_hash, 1, 100, on_match), StopLookup);
    BOOST_CHECK_EQUAL(callbacks, 2);
    const auto result{index.Find(script_hash, 1, 100, IgnoreMatch)};
    BOOST_REQUIRE(!result);
    WITH_LOCK(cs_main, unreadable->nDataPos = original_pos);
    BOOST_CHECK_EQUAL(index.Find(script_hash, 1, 100, IgnoreMatch).value().size(), 100);
    index.Stop();
}

BOOST_AUTO_TEST_SUITE_END()
