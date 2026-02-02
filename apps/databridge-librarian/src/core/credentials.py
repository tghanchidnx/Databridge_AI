"""
Credential encryption and management for DataBridge AI Librarian.

Uses Fernet symmetric encryption for storing sensitive credentials.
"""

import base64
import hashlib
import secrets
from pathlib import Path
from typing import Optional

from cryptography.fernet import Fernet, InvalidToken
from cryptography.hazmat.primitives import hashes
from cryptography.hazmat.primitives.kdf.pbkdf2 import PBKDF2HMAC

from .config import get_settings


class CredentialManager:
    """
    Secure credential storage and retrieval.

    Encrypts credentials using Fernet (AES-128) with a master key.
    """

    # Salt for key derivation (in production, store per-installation)
    _SALT = b"databridge_librarian_credential_salt"
    _ITERATIONS = 480000

    def __init__(self, master_key: Optional[str] = None):
        """
        Initialize the credential manager.

        Args:
            master_key: Master encryption key. If None, attempts to load from config.
        """
        settings = get_settings()

        if master_key:
            self._master_key = master_key
        elif settings.security.master_key:
            self._master_key = settings.security.master_key.get_secret_value()
        else:
            self._master_key = None

        self._fernet: Optional[Fernet] = None

    def _ensure_fernet(self) -> Fernet:
        """Ensure Fernet instance is initialized."""
        if self._fernet is None:
            if self._master_key is None:
                raise ValueError(
                    "Master key not configured. Set DATABRIDGE_SECURITY_MASTER_KEY "
                    "environment variable or provide master_key parameter."
                )
            self._fernet = self._create_fernet(self._master_key)
        return self._fernet

    def _create_fernet(self, master_key: str) -> Fernet:
        """
        Create a Fernet instance from a master key.

        Derives an encryption key using PBKDF2.

        Args:
            master_key: The master password/key.

        Returns:
            Fernet: Initialized Fernet instance.
        """
        kdf = PBKDF2HMAC(
            algorithm=hashes.SHA256(),
            length=32,
            salt=self._SALT,
            iterations=self._ITERATIONS,
        )
        key = base64.urlsafe_b64encode(kdf.derive(master_key.encode()))
        return Fernet(key)

    def encrypt(self, plaintext: str) -> str:
        """
        Encrypt a credential for storage.

        Args:
            plaintext: The credential to encrypt.

        Returns:
            str: Base64-encoded encrypted credential.
        """
        fernet = self._ensure_fernet()
        return fernet.encrypt(plaintext.encode()).decode()

    def decrypt(self, ciphertext: str) -> str:
        """
        Decrypt a stored credential.

        Args:
            ciphertext: The encrypted credential.

        Returns:
            str: Decrypted plaintext credential.

        Raises:
            InvalidToken: If the ciphertext is invalid or tampered.
        """
        fernet = self._ensure_fernet()
        return fernet.decrypt(ciphertext.encode()).decode()

    def is_configured(self) -> bool:
        """
        Check if credential management is configured.

        Returns:
            bool: True if master key is available.
        """
        return self._master_key is not None

    @staticmethod
    def generate_master_key() -> str:
        """
        Generate a new secure master key.

        Returns:
            str: A new Fernet-compatible key.
        """
        return Fernet.generate_key().decode()

    @staticmethod
    def generate_api_key(prefix: str = "db_") -> tuple[str, str]:
        """
        Generate a new API key pair.

        Returns:
            tuple: (key_id, key_secret) - The key_id is for identification,
                   key_secret is the actual authentication value.
        """
        key_id = f"{prefix}{secrets.token_urlsafe(8)}"
        key_secret = secrets.token_urlsafe(32)
        return key_id, key_secret

    @staticmethod
    def hash_api_key(key_secret: str) -> str:
        """
        Hash an API key for secure storage.

        Uses SHA-256 for one-way hashing.

        Args:
            key_secret: The API key secret to hash.

        Returns:
            str: Hex-encoded hash of the key.
        """
        return hashlib.sha256(key_secret.encode()).hexdigest()

    @staticmethod
    def verify_api_key(key_secret: str, key_hash: str) -> bool:
        """
        Verify an API key against its stored hash.

        Uses constant-time comparison to prevent timing attacks.

        Args:
            key_secret: The API key secret to verify.
            key_hash: The stored hash to compare against.

        Returns:
            bool: True if the key matches the hash.
        """
        computed_hash = CredentialManager.hash_api_key(key_secret)
        return secrets.compare_digest(computed_hash, key_hash)


# Module-level credential manager instance
_credential_manager: Optional[CredentialManager] = None


def get_credential_manager() -> CredentialManager:
    """
    Get the credential manager singleton.

    Returns:
        CredentialManager: The credential manager instance.
    """
    global _credential_manager
    if _credential_manager is None:
        _credential_manager = CredentialManager()
    return _credential_manager


def encrypt_credential(plaintext: str) -> str:
    """
    Convenience function to encrypt a credential.

    Args:
        plaintext: The credential to encrypt.

    Returns:
        str: Encrypted credential.
    """
    return get_credential_manager().encrypt(plaintext)


def decrypt_credential(ciphertext: str) -> str:
    """
    Convenience function to decrypt a credential.

    Args:
        ciphertext: The encrypted credential.

    Returns:
        str: Decrypted credential.
    """
    return get_credential_manager().decrypt(ciphertext)
