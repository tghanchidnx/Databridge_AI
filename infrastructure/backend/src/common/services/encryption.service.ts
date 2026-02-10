import * as crypto from 'crypto';

export class EncryptionService {
  private readonly algorithm = 'aes-256-gcm';
  private readonly key: Buffer;
  private readonly ivLength = 16;
  private readonly saltLength = 64;
  private readonly tagLength = 16;
  private readonly tagPosition: number;
  private readonly encryptedPosition: number;

  constructor(encryptionKey?: string) {
    const key = encryptionKey || process.env.ENCRYPTION_KEY;
    if (!key) {
      throw new Error('Encryption key not provided');
    }

    // Derive a 256-bit key from the provided key
    this.key = crypto.createHash('sha256').update(key).digest();

    this.tagPosition = this.saltLength + this.ivLength;
    this.encryptedPosition = this.tagPosition + this.tagLength;
  }

  /**
   * Encrypts a string value
   * @param text The text to encrypt
   * @returns Encrypted string in base64 format
   */
  encrypt(text: string): string {
    if (!text) return text;

    const iv = crypto.randomBytes(this.ivLength);
    const salt = crypto.randomBytes(this.saltLength);

    const cipher = crypto.createCipheriv(this.algorithm, this.key, iv);

    const encrypted = Buffer.concat([cipher.update(String(text), 'utf8'), cipher.final()]);

    const tag = cipher.getAuthTag();

    return Buffer.concat([salt, iv, tag, encrypted]).toString('base64');
  }

  /**
   * Decrypts an encrypted string
   * @param encryptedText The encrypted text in base64 format
   * @returns Decrypted string
   */
  decrypt(encryptedText: string): string {
    if (!encryptedText) return encryptedText;

    try {
      const data = Buffer.from(encryptedText, 'base64');

      const salt = data.subarray(0, this.saltLength);
      const iv = data.subarray(this.saltLength, this.tagPosition);
      const tag = data.subarray(this.tagPosition, this.encryptedPosition);
      const encrypted = data.subarray(this.encryptedPosition);

      const decipher = crypto.createDecipheriv(this.algorithm, this.key, iv);
      decipher.setAuthTag(tag);

      return decipher.update(encrypted) + decipher.final('utf8');
    } catch (error) {
      throw new Error('Failed to decrypt data: ' + error.message);
    }
  }

  /**
   * Encrypts an object by encrypting all its string values
   * @param obj Object to encrypt
   * @returns Object with encrypted values
   */
  encryptObject(obj: Record<string, any>): Record<string, any> {
    const encrypted: Record<string, any> = {};

    for (const [key, value] of Object.entries(obj)) {
      if (typeof value === 'string') {
        encrypted[key] = this.encrypt(value);
      } else if (typeof value === 'object' && value !== null) {
        encrypted[key] = this.encryptObject(value);
      } else {
        encrypted[key] = value;
      }
    }

    return encrypted;
  }

  /**
   * Decrypts an object by decrypting all its encrypted string values
   * @param obj Object to decrypt
   * @returns Object with decrypted values
   */
  decryptObject(obj: Record<string, any>): Record<string, any> {
    const decrypted: Record<string, any> = {};

    for (const [key, value] of Object.entries(obj)) {
      if (typeof value === 'string') {
        try {
          decrypted[key] = this.decrypt(value);
        } catch {
          decrypted[key] = value;
        }
      } else if (typeof value === 'object' && value !== null) {
        decrypted[key] = this.decryptObject(value);
      } else {
        decrypted[key] = value;
      }
    }

    return decrypted;
  }

  /**
   * Hashes a value using SHA256
   * @param value The value to hash
   * @returns Hashed value in hex format
   */
  hash(value: string): string {
    return crypto.createHash('sha256').update(value).digest('hex');
  }

  /**
   * Generates a random token
   * @param length Length of the token (default: 32)
   * @returns Random token in hex format
   */
  generateToken(length: number = 32): string {
    return crypto.randomBytes(length).toString('hex');
  }
}
