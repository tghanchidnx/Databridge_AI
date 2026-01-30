"""
DataBridge Core Credentials Module.

Provides secure credential management with Fernet encryption.
"""

from databridge_core.credentials.manager import (
    CredentialManager,
    get_credential_manager,
)

__all__ = [
    "CredentialManager",
    "get_credential_manager",
]
