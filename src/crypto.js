/**
 * crypto.js — AES-ECB encode, MD5, SHA-256, hash_password
 * Ported from Python main.py lines 1023-1114
 */
const crypto = require('crypto');

/**
 * AES-ECB encode: encrypts hex plaintext with hex key using AES-ECB,
 * returns first 32 hex characters of the ciphertext.
 * Python: AES.new(bytes.fromhex(key), AES.MODE_ECB).encrypt(bytes.fromhex(plaintext)).hex()[:32]
 * Key length determines AES variant: 16 bytes = AES-128, 24 = AES-192, 32 = AES-256
 */
function encode(key, plaintext) {
  const keyBuf = Buffer.from(key, 'hex');
  const ptBuf  = Buffer.from(plaintext, 'hex');
  const keyLen = keyBuf.length;
  let algo;
  if (keyLen === 16) algo = 'aes-128-ecb';
  else if (keyLen === 24) algo = 'aes-192-ecb';
  else if (keyLen === 32) algo = 'aes-256-ecb';
  else throw new Error(`Invalid AES key length: ${keyLen} bytes`);
  const cipher = crypto.createCipheriv(algo, keyBuf, null);
  const encrypted = Buffer.concat([cipher.update(ptBuf), cipher.final()]);
  return encrypted.toString('hex').slice(0, 32);
}

/**
 * MD5 hash of the password with URL decoding.
 * Python: hashlib.md5(urllib.parse.unquote(password).encode()).hexdigest()
 */
function getPassMd5(password) {
  const decoded = decodeURIComponent(password);
  return crypto.createHash('md5').update(decoded).digest('hex');
}

/**
 * Hash password for Garena SSO login.
 * Python hash_password():
 *   passmd5 = get_passmd5(password)
 *   inner = hashlib.sha256(f"{passmd5}{v1}".encode()).hexdigest()
 *   outer = hashlib.sha256(f"{inner}{v2}".encode()).hexdigest()
 *   return encode(passmd5, outer)
 */
function hashPassword(password, v1, v2) {
  const passmd5 = getPassMd5(password);
  const inner = crypto.createHash('sha256')
    .update(`${passmd5}${v1}`)
    .digest('hex');
  const outer = crypto.createHash('sha256')
    .update(`${inner}${v2}`)
    .digest('hex');
  return encode(passmd5, outer);
}

/**
 * Simple MD5 hash of a string.
 */
function md5(str) {
  return crypto.createHash('md5').update(str).digest('hex');
}

/**
 * SHA-256 hash of a string.
 */
function sha256(str) {
  return crypto.createHash('sha256').update(str).digest('hex');
}

/**
 * Generate a random hex string of given byte length.
 */
function randomHex(bytes = 16) {
  return crypto.randomBytes(bytes).toString('hex');
}

module.exports = {
  encode,
  getPassMd5,
  hashPassword,
  md5,
  sha256,
  randomHex,
};
