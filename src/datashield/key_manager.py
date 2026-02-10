"""DataShield Key Manager - Local encrypted keystore with Fernet + PBKDF2.

Keys are stored locally in an encrypted JSON file. The master key is derived
from a user passphrase via PBKDF2 (100k iterations). Each project gets its
own random 256-bit key, encrypted at rest.

Keys never leave the local machine — DataBridge cannot reverse scrambling.
"""

import json
import os
import secrets
import base64
import logging
from pathlib import Path
from typing import Optional, Dict

try:
    from cryptography.fernet import Fernet
    from cryptography.hazmat.primitives import hashes
    from cryptography.hazmat.primitives.kdf.pbkdf2 import PBKDF2HMAC
    CRYPTO_AVAILABLE = True
except ImportError:
    CRYPTO_AVAILABLE = False

logger = logging.getLogger(__name__)


class KeyManager:
    """Manages encrypted keystore for DataShield projects.

    Keystore format (encrypted JSON):
    {
        "keys": {
            "alias1": "<base64-encoded 32-byte key>",
            "alias2": "<base64-encoded 32-byte key>"
        }
    }
    """

    PBKDF2_ITERATIONS = 100_000

    def __init__(self, keystore_path: str):
        """Initialize key manager.

        Args:
            keystore_path: Path to the encrypted keystore file
        """
        self._path = Path(keystore_path)
        self._salt_path = self._path.with_suffix(".salt")
        self._fernet: Optional[object] = None
        self._keys: Dict[str, str] = {}
        self._unlocked = False

    @property
    def is_available(self) -> bool:
        """Check if cryptography library is installed."""
        return CRYPTO_AVAILABLE

    @property
    def is_unlocked(self) -> bool:
        """Check if keystore is unlocked."""
        return self._unlocked

    @property
    def keystore_exists(self) -> bool:
        """Check if a keystore file exists."""
        return self._path.exists()

    def _derive_fernet_key(self, passphrase: str, salt: bytes) -> bytes:
        """Derive a Fernet key from passphrase using PBKDF2."""
        if not CRYPTO_AVAILABLE:
            raise RuntimeError("cryptography library is required for DataShield key management. "
                               "Install with: pip install cryptography")

        kdf = PBKDF2HMAC(
            algorithm=hashes.SHA256(),
            length=32,
            salt=salt,
            iterations=self.PBKDF2_ITERATIONS,
        )
        return base64.urlsafe_b64encode(kdf.derive(passphrase.encode("utf-8")))

    def create_keystore(self, passphrase: str) -> bool:
        """Create a new encrypted keystore.

        Args:
            passphrase: User passphrase to protect the keystore

        Returns:
            True if created successfully
        """
        if not CRYPTO_AVAILABLE:
            raise RuntimeError("cryptography library required")

        # Generate random salt
        salt = os.urandom(16)

        # Derive Fernet key
        fernet_key = self._derive_fernet_key(passphrase, salt)
        self._fernet = Fernet(fernet_key)

        # Initialize empty keystore
        self._keys = {}
        self._unlocked = True

        # Save salt (plaintext)
        self._path.parent.mkdir(parents=True, exist_ok=True)
        self._salt_path.write_bytes(salt)

        # Save encrypted keystore
        self._save()

        logger.info("Created new DataShield keystore at %s", self._path)
        return True

    def unlock(self, passphrase: str) -> bool:
        """Unlock an existing keystore with passphrase.

        Args:
            passphrase: User passphrase

        Returns:
            True if unlocked successfully
        """
        if not CRYPTO_AVAILABLE:
            raise RuntimeError("cryptography library required")

        if not self._path.exists():
            raise FileNotFoundError(f"Keystore not found: {self._path}")

        if not self._salt_path.exists():
            raise FileNotFoundError(f"Keystore salt not found: {self._salt_path}")

        # Read salt
        salt = self._salt_path.read_bytes()

        # Derive Fernet key
        fernet_key = self._derive_fernet_key(passphrase, salt)
        self._fernet = Fernet(fernet_key)

        # Decrypt and load keystore
        try:
            encrypted = self._path.read_bytes()
            decrypted = self._fernet.decrypt(encrypted)
            data = json.loads(decrypted.decode("utf-8"))
            self._keys = data.get("keys", {})
            self._unlocked = True
            logger.info("Unlocked DataShield keystore (%d keys)", len(self._keys))
            return True
        except Exception as e:
            self._fernet = None
            self._unlocked = False
            logger.error("Failed to unlock keystore: %s", e)
            raise ValueError("Invalid passphrase or corrupted keystore") from e

    def _save(self):
        """Save keystore to disk (encrypted)."""
        if not self._fernet:
            raise RuntimeError("Keystore not unlocked")

        data = json.dumps({"keys": self._keys}).encode("utf-8")
        encrypted = self._fernet.encrypt(data)
        self._path.write_bytes(encrypted)

    def generate_project_key(self, alias: str) -> bytes:
        """Generate and store a new random 256-bit key for a project.

        Args:
            alias: Key alias (usually project name or ID)

        Returns:
            The raw 32-byte key
        """
        if not self._unlocked:
            raise RuntimeError("Keystore is locked")

        raw_key = secrets.token_bytes(32)
        self._keys[alias] = base64.b64encode(raw_key).decode("ascii")
        self._save()

        logger.info("Generated project key: %s", alias)
        return raw_key

    def get_project_key(self, alias: str) -> bytes:
        """Retrieve a project key by alias.

        Args:
            alias: Key alias

        Returns:
            The raw 32-byte key

        Raises:
            KeyError: If alias not found
        """
        if not self._unlocked:
            raise RuntimeError("Keystore is locked")

        encoded = self._keys.get(alias)
        if not encoded:
            raise KeyError(f"No key found for alias: {alias}")

        return base64.b64decode(encoded)

    def delete_project_key(self, alias: str) -> bool:
        """Delete a project key.

        Args:
            alias: Key alias to delete

        Returns:
            True if deleted
        """
        if not self._unlocked:
            raise RuntimeError("Keystore is locked")

        if alias not in self._keys:
            return False

        del self._keys[alias]
        self._save()
        logger.info("Deleted project key: %s", alias)
        return True

    def list_aliases(self) -> list:
        """List all key aliases in the keystore."""
        if not self._unlocked:
            raise RuntimeError("Keystore is locked")
        return list(self._keys.keys())

    def get_status(self) -> dict:
        """Get keystore status."""
        return {
            "crypto_available": CRYPTO_AVAILABLE,
            "keystore_exists": self.keystore_exists,
            "unlocked": self._unlocked,
            "key_count": len(self._keys) if self._unlocked else None,
            "keystore_path": str(self._path),
        }
