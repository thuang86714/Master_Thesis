// signature.h - sign and verify message using RSA
// author: Sun Guangda <sung@comp.nus.edu.sg>

#ifndef DSNET_COMMON_SIGNATURE_H_
#define DSNET_COMMON_SIGNATURE_H_

#include <openssl/evp.h>
#include <secp256k1.h>

#include <string>

#include "lib/configuration.h"
#include "lib/transport.h"

// change back to specpaxos if we are not adopting
namespace dsnet {

class Signer {
 public:
  // return true and overwrite signature on sucess
  virtual bool Sign(const std::string &message, std::string &signature) const {
    signature = "signed";
    return true;
  }
};

class Verifier {
 public:
  // return false on both signature mismatching and verification failure
  virtual bool Verify(const std::string &message,
                      const std::string &signature) const {
    return signature.substr(0, 6) == "signed";
  }
};

class RsaSigner : public Signer {
 private:
  EVP_PKEY *pkey;

 public:
  RsaSigner(const std::string &privateKey);
  ~RsaSigner();
  bool Sign(const std::string &message, std::string &signature) const override;
};

class RsaVerifier : public Verifier {
 private:
  EVP_PKEY *pkey;

 public:
  RsaVerifier(const std::string &publicKey);
  ~RsaVerifier();
  bool Verify(const std::string &message,
              const std::string &signature) const override;
};

class Secp256k1Signer : public Signer {
 private:
  static const unsigned char *kDefaultSecret;
  secp256k1_context *ctx;
  unsigned char secKey[32];
  friend class Secp256k1Verifier;

 public:
  // if nullptr is passed then random key will be generate
  Secp256k1Signer(
      const unsigned char *secKey = Secp256k1Signer::kDefaultSecret);
  ~Secp256k1Signer();
  bool Sign(const std::string &message, std::string &signature) const override;
};

class Secp256k1Verifier : public Verifier {
 private:
  secp256k1_context *ctx;
  secp256k1_pubkey *pubKey;

 public:
  Secp256k1Verifier(const Secp256k1Signer &signer);
  ~Secp256k1Verifier();
  bool Verify(const std::string &message,
              const std::string &signature) const override;
};

// each BFT replica/client should accept a &Security in its constructor so
// proper signature impl can be injected
class Security {
 private:
  const Signer &client_signer;
  const Verifier &client_verifier;

 protected:
  Security(const Signer &client_signer, const Verifier &client_verifier)
      : client_signer(client_signer), client_verifier(client_verifier) {}

 public:
  const Signer &ClientSigner() const { return client_signer; }
  const Verifier &ClientVerifier() const { return client_verifier; }

  virtual const Signer &ReplicaSigner(int replica_id) const = 0;
  virtual const Verifier &ReplicaVerifier(int replica_id) const = 0;
  virtual const Signer &SequencerSigner(int replica_id,
                                        int index = 0) const = 0;
  virtual const Verifier &SequencerVerifier(int replica_id,
                                            int index = 0) const = 0;
};

// for bench
class HomogeneousSecurity : public Security {
 private:
  const Signer &s, &seq_s;
  const Verifier &v, &seq_v;

 public:
  HomogeneousSecurity(const Signer &s, const Verifier &v, const Signer &seq_s,
                      const Verifier &seq_v)
      : Security(s, v), s(s), seq_s(s), v(v), seq_v(v) {}
  HomogeneousSecurity(const Signer &s, const Verifier &v)
      : HomogeneousSecurity(s, v, s, v) {}

  virtual const Signer &ReplicaSigner(int replica_id) const { return s; }
  virtual const Verifier &ReplicaVerifier(int replica_id) const { return v; }
  virtual const Signer &SequencerSigner(int replica_id,
                                        int index) const override {
    return seq_s;
  }
  virtual const Verifier &SequencerVerifier(int replica_id,
                                            int index) const override {
    return seq_v;
  }
};

// for debug
// the only way to fail NopSecurity is to Verify without Sign
class NopSecurity : public HomogeneousSecurity {
 private:
  const static Signer s;
  const static Verifier v;

 public:
  NopSecurity() : HomogeneousSecurity(NopSecurity::s, NopSecurity::v) {}
};

}  // namespace dsnet

#endif