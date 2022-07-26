#include "lib/signature.h"

#include <gtest/gtest.h>

#include <string>

#include "lib/configuration.h"

using namespace std;
using namespace dsnet;
TEST(Signature, CanSignAndVerify) {
  std::string message = "Hello!";
  Secp256k1Signer signer;
  Secp256k1Verifier verifier(signer);
  HomogeneousSecurity sec(signer, verifier);
  std::string signature;
  ASSERT_TRUE(sec.ReplicaSigner(0).Sign(message, signature));
  ASSERT_GT(signature.size(), 0);
  ASSERT_TRUE(sec.ReplicaVerifier(0).Verify(message, signature));
}

TEST(Signature, MultipleSignAndVerify) {
  std::string hello = "Hello!", bye = "Goodbye!";
  Secp256k1Signer signer;
  Secp256k1Verifier verifier(signer);
  HomogeneousSecurity sec(signer, verifier);
  std::string helloSig, byeSig;
  ASSERT_TRUE(sec.ReplicaSigner(0).Sign(hello, helloSig));
  ASSERT_TRUE(sec.ReplicaSigner(0).Sign(bye, byeSig));
  ASSERT_TRUE(sec.ReplicaVerifier(0).Verify(hello, helloSig));
  ASSERT_TRUE(sec.ReplicaVerifier(0).Verify(hello, helloSig));
  ASSERT_TRUE(sec.ReplicaVerifier(0).Verify(bye, byeSig));
  ASSERT_FALSE(sec.ReplicaVerifier(0).Verify(hello, byeSig));
  ASSERT_FALSE(sec.ReplicaVerifier(0).Verify(bye, helloSig));
}
