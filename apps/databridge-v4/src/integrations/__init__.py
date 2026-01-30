"""
External integrations module for DataBridge Analytics V4.

Provides integrations with external services:
- Notion: Documentation sync
- Slack: Notifications (future)
- Email: Alerts (future)
"""

from .notion_sync import (
    NotionSync,
    NotionPage,
    NotionDatabase,
    NotionSyncResult,
)

__all__ = [
    "NotionSync",
    "NotionPage",
    "NotionDatabase",
    "NotionSyncResult",
]
