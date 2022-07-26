// digest.c - sign and verify message using RSA
// author: Sun Guangda <sung@comp.nus.edu.sg>
// openssl RSA part adopts from
//  https://gist.github.com/irbull/08339ddcd5686f509e9826964b17bb59
// TODO: figure out how to chain a pipeline instead of creating temporary
// objects on every sign/verify to improve performance
// TODO: test for memory leaking

#include "signature.h"

#include <openssl/aes.h>
#include <openssl/bio.h>
#include <openssl/err.h>
#include <openssl/evp.h>
#include <openssl/pem.h>
#include <openssl/rsa.h>
#include <openssl/sha.h>
#include <openssl/ssl.h>

#include <cstring>

#include "lib/assert.h"

// https://stackoverflow.com/a/34477445
static const unsigned char SEC[] = "01234567890123456789012345678901";
const unsigned char *dsnet::Secp256k1Signer::kDefaultSecret = SEC;

const dsnet::Signer dsnet::NopSecurity::s;
const dsnet::Verifier dsnet::NopSecurity::v;

namespace {

// https://gist.github.com/irbull/08339ddcd5686f509e9826964b17bb59
size_t calcDecodeLength(const char *b64input) {
  size_t len = strlen(b64input), padding = 0;

  if (b64input[len - 1] == '=' &&
      b64input[len - 2] == '=')  // last two chars are =
    padding = 2;
  else if (b64input[len - 1] == '=')  // last char is =
    padding = 1;
  return (len * 3) / 4 - padding;
}

}  // namespace

dsnet::RsaSigner::RsaSigner(const std::string &privateKey) {
  BIO *keybio = BIO_new_mem_buf(privateKey.c_str(), -1);
  Assert(keybio);
  RSA *rsa = nullptr;
  rsa = PEM_read_bio_RSAPrivateKey(keybio, &rsa, nullptr, nullptr);
  Assert(rsa);
  pkey = EVP_PKEY_new();
  Assert(pkey);

  EVP_PKEY_assign_RSA(pkey, rsa);
}

dsnet::RsaSigner::~RsaSigner() {
  // todo
  // signer should only be instantiated once per client/replica
  // so the memory leaking should be fine
}

// assume panic on failure so no "finally" clean up
bool dsnet::RsaSigner::Sign(const std::string &message,
                            std::string &signature) const {
  // create binary signature
  EVP_MD_CTX *context = EVP_MD_CTX_new();
  if (EVP_DigestSignInit(context, nullptr, EVP_sha256(), nullptr, pkey) <= 0)
    return false;
  if (EVP_DigestSignUpdate(context, message.c_str(), message.size()) <= 0)
    return false;
  size_t binSigSize;
  if (EVP_DigestSignFinal(context, nullptr, &binSigSize) <= 0) return false;
  unsigned char *binSig = new unsigned char[binSigSize];
  if (!binSig) return false;
  if (EVP_DigestSignFinal(context, binSig, &binSigSize) <= 0) return false;
  EVP_MD_CTX_free(context);

  // (optional) create base64 signature
  BIO *b64 = BIO_new(BIO_f_base64());
  BIO *bio = BIO_new(BIO_s_mem());
  bio = BIO_push(b64, bio);
  BIO_write(bio, binSig, binSigSize);
  BIO_flush(bio);
  char *sigPtr;
  long sigSize = BIO_get_mem_data(bio, &sigPtr);
  signature.assign(sigPtr, sigSize);
  BIO_set_close(bio, BIO_NOCLOSE);
  BIO_free_all(bio);
  delete[] binSig;

  return true;
}

dsnet::RsaVerifier::RsaVerifier(const std::string &publicKey) {
  BIO *keybio = BIO_new_mem_buf(publicKey.c_str(), -1);
  Assert(keybio);
  RSA *rsa = nullptr;
  rsa = PEM_read_bio_RSA_PUBKEY(keybio, &rsa, nullptr, nullptr);
  Assert(rsa);
  pkey = EVP_PKEY_new();
  Assert(pkey);

  EVP_PKEY_assign_RSA(pkey, rsa);
}

dsnet::RsaVerifier::~RsaVerifier() {
  // todo: same as above
}

bool dsnet::RsaVerifier::Verify(const std::string &message,
                                const std::string &signature) const {
  int bufferSize = calcDecodeLength(signature.c_str());
  unsigned char *binSig = new unsigned char[bufferSize + 1];
  if (!binSig) return false;
  binSig[bufferSize] = 0;

  BIO *bio = BIO_new_mem_buf(signature.c_str(), -1);
  BIO *b64 = BIO_new(BIO_f_base64());
  bio = BIO_push(b64, bio);
  size_t binSigSize = BIO_read(bio, binSig, signature.size());
  BIO_free_all(bio);

  EVP_MD_CTX *context = EVP_MD_CTX_new();
  if (EVP_DigestVerifyInit(context, nullptr, EVP_sha256(), nullptr, pkey) <= 0)
    return false;
  if (EVP_DigestVerifyUpdate(context, message.c_str(), message.size()) <= 0) {
    return false;
  }
  if (EVP_DigestVerifyFinal(context, binSig, binSigSize) != 1) {
    return false;
  }
  EVP_MD_CTX_free(context);
  delete[] binSig;
  return true;
}

dsnet::Secp256k1Signer::Secp256k1Signer(const unsigned char *secKey) {
  if (secKey == nullptr) {
    NOT_IMPLEMENTED();
  }
  std::memcpy(this->secKey, secKey, 32);
  ctx = secp256k1_context_create(SECP256K1_CONTEXT_SIGN);
}

dsnet::Secp256k1Signer::~Secp256k1Signer() { secp256k1_context_destroy(ctx); }

bool dsnet::Secp256k1Signer::Sign(const std::string &message,
                                  std::string &signature) const {
  // hash message to 32 bytes
  // use sha-256 following libhotstuff
  unsigned char hash[SHA256_DIGEST_LENGTH];
  SHA256_CTX sha256;
  if (!SHA256_Init(&sha256)) return false;
  if (!SHA256_Update(&sha256, message.c_str(), message.length())) return false;
  if (!SHA256_Final(hash, &sha256)) return false;

  secp256k1_ecdsa_signature data;
  if (!secp256k1_ecdsa_sign(ctx, &data, hash, secKey, nullptr, nullptr))
    return false;
  unsigned char sig[64];
  if (!secp256k1_ecdsa_signature_serialize_compact(ctx, sig, &data))
    return false;
  signature.assign(reinterpret_cast<const char *>(sig), 64);
  return true;
}

dsnet::Secp256k1Verifier::Secp256k1Verifier(
    const dsnet::Secp256k1Signer &signer) {
  ctx = secp256k1_context_create(SECP256K1_CONTEXT_SIGN |
                                 SECP256K1_CONTEXT_VERIFY);
  pubKey = new secp256k1_pubkey;
  if (!secp256k1_ec_pubkey_create(ctx, pubKey, signer.secKey)) {
    Panic("Cannot create public key");
  }
}

dsnet::Secp256k1Verifier::~Secp256k1Verifier() {
  secp256k1_context_destroy(ctx);
  delete pubKey;
}

bool dsnet::Secp256k1Verifier::Verify(const std::string &message,
                                      const std::string &signature) const {
  unsigned char hash[SHA256_DIGEST_LENGTH];
  SHA256_CTX sha256;
  if (!SHA256_Init(&sha256)) return false;
  if (!SHA256_Update(&sha256, message.c_str(), message.length())) return false;
  if (!SHA256_Final(hash, &sha256)) return false;

  secp256k1_ecdsa_signature data;
  if (!secp256k1_ecdsa_signature_parse_compact(
          ctx, &data,
          reinterpret_cast<const unsigned char *>(signature.c_str())))
    return false;
  return secp256k1_ecdsa_verify(ctx, &data, hash, pubKey);
}