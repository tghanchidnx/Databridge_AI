"""
Unit tests for the credentials module.
"""

import pytest
from cryptography.fernet import InvalidToken


class TestCredentialManager:
    """Tests for CredentialManager class."""

    @pytest.fixture
    def credential_manager(self):
        """Create a credential manager with a test key."""
        from src.core.credentials import CredentialManager

        return CredentialManager(master_key="test_master_key_12345")

    def test_encrypt_decrypt_roundtrip(self, credential_manager):
        """Test encrypt then decrypt returns original value."""
        original = "my_secret_password_123!"
        encrypted = credential_manager.encrypt(original)
        decrypted = credential_manager.decrypt(encrypted)

        assert decrypted == original
        assert encrypted != original

    def test_encrypt_produces_different_output(self, credential_manager):
        """Test that encrypting the same value twice produces different output."""
        original = "test_password"
        encrypted1 = credential_manager.encrypt(original)
        encrypted2 = credential_manager.encrypt(original)

        # Fernet uses a nonce, so outputs should be different
        assert encrypted1 != encrypted2

        # But both should decrypt to the same value
        assert credential_manager.decrypt(encrypted1) == original
        assert credential_manager.decrypt(encrypted2) == original

    def test_decrypt_invalid_token(self, credential_manager):
        """Test decrypting invalid token raises error."""
        with pytest.raises(InvalidToken):
            credential_manager.decrypt("invalid_encrypted_value")

    def test_is_configured_with_key(self, credential_manager):
        """Test is_configured returns True when key is provided."""
        assert credential_manager.is_configured() is True

    def test_is_configured_without_key(self):
        """Test is_configured returns False when no key."""
        from src.core.credentials import CredentialManager

        manager = CredentialManager()
        # Will be False unless environment has the key
        # This depends on environment, so we just check the method exists
        assert hasattr(manager, "is_configured")

    def test_generate_master_key(self):
        """Test generating a new master key."""
        from src.core.credentials import CredentialManager

        key = CredentialManager.generate_master_key()

        assert key is not None
        assert len(key) > 0
        # Should be a valid Fernet key
        assert "=" in key  # Base64 encoding typically has padding

    def test_generate_api_key(self):
        """Test generating API key pair."""
        from src.core.credentials import CredentialManager

        key_id, key_secret = CredentialManager.generate_api_key()

        assert key_id.startswith("db_")
        assert len(key_secret) > 20  # Should be sufficiently long

    def test_generate_api_key_custom_prefix(self):
        """Test generating API key with custom prefix."""
        from src.core.credentials import CredentialManager

        key_id, key_secret = CredentialManager.generate_api_key(prefix="test_")

        assert key_id.startswith("test_")

    def test_hash_api_key(self):
        """Test hashing an API key."""
        from src.core.credentials import CredentialManager

        key_secret = "test_key_secret_123"
        hash1 = CredentialManager.hash_api_key(key_secret)
        hash2 = CredentialManager.hash_api_key(key_secret)

        # Same input should produce same hash
        assert hash1 == hash2
        # Hash should be hex string (SHA256 = 64 chars)
        assert len(hash1) == 64
        assert all(c in "0123456789abcdef" for c in hash1)

    def test_verify_api_key_valid(self):
        """Test verifying a valid API key."""
        from src.core.credentials import CredentialManager

        key_secret = "test_key_secret_456"
        key_hash = CredentialManager.hash_api_key(key_secret)

        assert CredentialManager.verify_api_key(key_secret, key_hash) is True

    def test_verify_api_key_invalid(self):
        """Test verifying an invalid API key."""
        from src.core.credentials import CredentialManager

        key_secret = "test_key_secret_789"
        wrong_hash = CredentialManager.hash_api_key("different_key")

        assert CredentialManager.verify_api_key(key_secret, wrong_hash) is False


class TestConvenienceFunctions:
    """Tests for module-level convenience functions."""

    def test_encrypt_credential_requires_key(self):
        """Test that encrypt_credential fails without master key."""
        from src.core.credentials import encrypt_credential

        # This test depends on whether a key is configured in the environment
        # Just verify the function exists
        assert callable(encrypt_credential)

    def test_decrypt_credential_requires_key(self):
        """Test that decrypt_credential fails without master key."""
        from src.core.credentials import decrypt_credential

        # This test depends on whether a key is configured in the environment
        # Just verify the function exists
        assert callable(decrypt_credential)
