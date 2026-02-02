"""
External integrations module for DataBridge Analytics Researcher.

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
