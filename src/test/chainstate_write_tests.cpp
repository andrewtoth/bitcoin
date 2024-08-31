// Copyright (c) The Bitcoin Core developers
// Distributed under the MIT software license, see the accompanying
// file COPYING or http://www.opensource.org/licenses/mit-license.php.

#include <test/util/setup_common.h>
#include <validation.h>
#include <validationinterface.h>

#include <boost/test/unit_test.hpp>

BOOST_AUTO_TEST_SUITE(chainstate_write_tests)

struct TestSubscriber final : public CValidationInterface {
    bool m_did_flush{false};

    TestSubscriber() = default;

    void ChainStateFlushed(ChainstateRole role, const CBlockLocator& locator) override
    {
        m_did_flush = true;
    }
};

BOOST_FIXTURE_TEST_CASE(chainstate_write_interval, TestingSetup)
{
    const auto sub{std::make_shared<TestSubscriber>()};
    m_node.validation_signals->RegisterSharedValidationInterface(sub);
    Chainstate& chainstate{Assert(m_node.chainman)->ActiveChainstate()};
    BlockValidationState state_dummy{};

    // The first periodic flush sets m_last_flush and does not flush
    chainstate.FlushStateToDisk(state_dummy, FlushStateMode::PERIODIC);
    m_node.validation_signals->SyncWithValidationInterfaceQueue();
    BOOST_CHECK(!sub->m_did_flush);

    SetMockTime(GetTime<std::chrono::minutes>() + 23h + 59min);
    chainstate.FlushStateToDisk(state_dummy, FlushStateMode::PERIODIC);
    m_node.validation_signals->SyncWithValidationInterfaceQueue();
    BOOST_CHECK(!sub->m_did_flush);

    SetMockTime(GetTime<std::chrono::minutes>() + 24h);
    chainstate.FlushStateToDisk(state_dummy, FlushStateMode::PERIODIC);
    m_node.validation_signals->SyncWithValidationInterfaceQueue();
    BOOST_CHECK(sub->m_did_flush);
}

BOOST_AUTO_TEST_SUITE_END()
