// tests/lib/quorumset-test.cc
//
// Althrough implementation is under common/
// currectly there is no test dir for common and it seems quorum set should be
// moved to lib/ eventually...
// Notice: only ByzantineQuorumSet is tested

#include "common/quorumset.h"

#include <gtest/gtest.h>

using namespace std;
using namespace dsnet;

TEST(QuorumSetTest, BasicByzantine) {
  ByzantineQuorumSet<uint64_t, string> qs(3);
  ASSERT_FALSE(qs.CheckForQuorum(1, "cowsay"));
  qs.Add(1, 0, "cowsay");
  ASSERT_FALSE(qs.CheckForQuorum(1, "cowsay"));
  qs.Add(1, 0, "cowsay");
  ASSERT_FALSE(qs.CheckForQuorum(1, "cowsay"));
  qs.Add(1, 1, "cowsay");
  ASSERT_FALSE(qs.CheckForQuorum(1, "cowsay"));
  qs.Add(1, 1, "cowsay");
  ASSERT_FALSE(qs.CheckForQuorum(1, "cowsay"));
  qs.Add(1, 1, "cowsay");
  ASSERT_FALSE(qs.CheckForQuorum(1, "cowsay"));
  qs.Add(1, 2, "catsay");
  ASSERT_FALSE(qs.CheckForQuorum(1, "cowsay"));
  ASSERT_TRUE(qs.Add(1, 3, "cowsay"));
  ASSERT_TRUE(qs.CheckForQuorum(1, "cowsay"));
  qs.Clear(1);
  ASSERT_FALSE(qs.CheckForQuorum(1, "cowsay"));
}