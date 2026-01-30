"""
Credential management for DataBridge AI platform.

Provides secure storage and retrieval of credentials using Fernet encryption.
"""

import base64
import os
from typing import Optional
from cryptography.fernet import Fernet
from cryptography.hazmat.primitives import hashes
from cryptography.hazmat.primitives.kdf.pbkdf2 import PBKDF2HMAC


class CredentialManager:
    """
    Manages encrypted credentials.

    Uses Fernet symmetric encryption for secure storage.
    """

    def __init__(self, master_key: Optional[str] = None):
        """
        Initialize the credential manager.

        Args:
            master_key: Master encryption key. If not provided, a new one is generated.
        """
        if master_key:
            # Derive a key from the master key
            self._fernet = self._derive_fernet(master_key)
        else:
            # Generate a new key
            self._key = Fernet.generate_key()
            self._fernet = Fernet(self._key)

    def _derive_fernet(self, master_key: str) -> Fernet:
        """Derive a Fernet key from a master key string."""
        salt = b"databridge_salt_v1"  # Fixed salt for deterministic derivation
        kdf = PBKDF2HMAC(
            algorithm=hashes.SHA256(),
            length=32,
            salt=salt,
            iterations=100000,
        )
        key = base64.urlsafe_b64encode(kdf.derive(master_key.encode()))
        return Fernet(key)

    def encrypt(self, plaintext: str) -> str:
        """
        Encrypt a plaintext string.

        Args:
            plaintext: String to encrypt.

        Returns:
            Base64-encoded encrypted string.
        """
        encrypted = self._fernet.encrypt(plaintext.encode())
        return base64.urlsafe_b64encode(encrypted).decode()

    def decrypt(self, ciphertext: str) -> str:
        """
        Decrypt an encrypted string.

        Args:
            ciphertext: Base64-encoded encrypted string.

        Returns:
            Decrypted plaintext string.

        Raises:
            ValueError: If decryption fails.
        """
        try:
            encrypted = base64.urlsafe_b64decode(ciphertext.encode())
            decrypted = self._fernet.decrypt(encrypted)
            return decrypted.decode()
        except Exception as e:
            raise ValueError(f"Decryption failed: {e}")

    def encrypt_password(self, password: str) -> str:
        """Encrypt a password for storage."""
        return self.encrypt(password)

    def decrypt_password(self, encrypted_password: str) -> str:
        """Decrypt a stored password."""
        return self.decrypt(encrypted_password)


# Global credential manager
_credential_manager: Optional[CredentialManager] = None


def set_credential_manager(manager: CredentialManager) -> None:
    """Set the global credential manager."""
    global _credential_manager
    _credential_manager = manager


def get_credential_manager() -> Optional[CredentialManager]:
    """Get the global credential manager."""
    return _credential_manager


def init_credential_manager(master_key: Optional[str] = None) -> CredentialManager:
    """
    Initialize and set the global credential manager.

    Args:
        master_key: Optional master encryption key.

    Returns:
        CredentialManager: The initialized manager.
    """
    manager = CredentialManager(master_key)
    set_credential_manager(manager)
    return manager
