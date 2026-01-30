"""
Notion Sync Service for DataBridge Analytics V4.

Syncs documentation, hierarchy structures, and project data to Notion.
"""

from dataclasses import dataclass, field
from datetime import datetime
from typing import Optional, List, Dict, Any
import logging

try:
    import httpx
    HTTPX_AVAILABLE = True
except ImportError:
    HTTPX_AVAILABLE = False


logger = logging.getLogger(__name__)


NOTION_API_VERSION = "2022-06-28"
NOTION_API_BASE = "https://api.notion.com/v1"


@dataclass
class NotionPage:
    """Represents a Notion page."""

    page_id: str
    title: str
    parent_id: Optional[str] = None
    parent_type: str = "page_id"  # page_id, database_id, workspace
    url: str = ""
    created_time: str = ""
    last_edited_time: str = ""
    icon: Optional[str] = None
    cover: Optional[str] = None
    archived: bool = False
    properties: Dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary."""
        return {
            "page_id": self.page_id,
            "title": self.title,
            "parent_id": self.parent_id,
            "parent_type": self.parent_type,
            "url": self.url,
            "created_time": self.created_time,
            "last_edited_time": self.last_edited_time,
            "icon": self.icon,
            "cover": self.cover,
            "archived": self.archived,
            "properties": self.properties,
        }

    @classmethod
    def from_notion_response(cls, data: Dict[str, Any]) -> "NotionPage":
        """Create from Notion API response."""
        # Extract title from properties
        title = ""
        props = data.get("properties", {})
        if "title" in props:
            title_array = props["title"].get("title", [])
            if title_array:
                title = title_array[0].get("plain_text", "")
        elif "Name" in props:
            name_array = props["Name"].get("title", [])
            if name_array:
                title = name_array[0].get("plain_text", "")

        # Extract parent info
        parent = data.get("parent", {})
        parent_type = parent.get("type", "workspace")
        parent_id = parent.get(parent_type)

        return cls(
            page_id=data.get("id", ""),
            title=title,
            parent_id=parent_id,
            parent_type=parent_type,
            url=data.get("url", ""),
            created_time=data.get("created_time", ""),
            last_edited_time=data.get("last_edited_time", ""),
            icon=data.get("icon", {}).get("emoji") if data.get("icon") else None,
            archived=data.get("archived", False),
            properties=props,
        )


@dataclass
class NotionDatabase:
    """Represents a Notion database."""

    database_id: str
    title: str
    description: str = ""
    url: str = ""
    created_time: str = ""
    last_edited_time: str = ""
    properties: Dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary."""
        return {
            "database_id": self.database_id,
            "title": self.title,
            "description": self.description,
            "url": self.url,
            "created_time": self.created_time,
            "last_edited_time": self.last_edited_time,
            "properties": self.properties,
        }

    @classmethod
    def from_notion_response(cls, data: Dict[str, Any]) -> "NotionDatabase":
        """Create from Notion API response."""
        # Extract title
        title_array = data.get("title", [])
        title = title_array[0].get("plain_text", "") if title_array else ""

        # Extract description
        desc_array = data.get("description", [])
        description = desc_array[0].get("plain_text", "") if desc_array else ""

        return cls(
            database_id=data.get("id", ""),
            title=title,
            description=description,
            url=data.get("url", ""),
            created_time=data.get("created_time", ""),
            last_edited_time=data.get("last_edited_time", ""),
            properties=data.get("properties", {}),
        )


@dataclass
class NotionSyncResult:
    """Result from Notion sync operations."""

    success: bool
    message: str = ""
    pages_created: int = 0
    pages_updated: int = 0
    pages_deleted: int = 0
    errors: List[str] = field(default_factory=list)
    data: Any = None

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary."""
        return {
            "success": self.success,
            "message": self.message,
            "pages_created": self.pages_created,
            "pages_updated": self.pages_updated,
            "pages_deleted": self.pages_deleted,
            "errors": self.errors,
            "data": self.data,
        }


class NotionSync:
    """
    Syncs DataBridge content to Notion.

    Features:
    - Create and update documentation pages
    - Sync hierarchy structures as databases
    - Handle incremental updates
    - Support bi-directional sync for comments
    """

    def __init__(
        self,
        api_key: str,
        workspace_id: Optional[str] = None,
        timeout: float = 30.0,
    ):
        """
        Initialize Notion sync service.

        Args:
            api_key: Notion integration API key.
            workspace_id: Optional workspace ID for filtering.
            timeout: Request timeout in seconds.
        """
        if not HTTPX_AVAILABLE:
            raise ImportError("httpx is required for NotionSync. Install with: pip install httpx")

        self.api_key = api_key
        self.workspace_id = workspace_id
        self.timeout = timeout
        self._client: Optional[httpx.Client] = None

    def _get_client(self) -> httpx.Client:
        """Get or create HTTP client."""
        if self._client is None:
            self._client = httpx.Client(
                base_url=NOTION_API_BASE,
                headers={
                    "Authorization": f"Bearer {self.api_key}",
                    "Notion-Version": NOTION_API_VERSION,
                    "Content-Type": "application/json",
                },
                timeout=self.timeout,
            )
        return self._client

    def test_connection(self) -> NotionSyncResult:
        """
        Test the Notion API connection.

        Returns:
            NotionSyncResult indicating connection status.
        """
        try:
            client = self._get_client()
            response = client.get("/users/me")
            response.raise_for_status()

            user_data = response.json()
            bot_name = user_data.get("name", "Unknown")

            return NotionSyncResult(
                success=True,
                message=f"Connected as: {bot_name}",
                data=user_data,
            )
        except httpx.HTTPStatusError as e:
            return NotionSyncResult(
                success=False,
                message=f"Authentication failed: {e.response.status_code}",
                errors=[str(e)],
            )
        except Exception as e:
            return NotionSyncResult(
                success=False,
                message=f"Connection failed: {str(e)}",
                errors=[str(e)],
            )

    # ==================== Page Operations ====================

    def get_page(self, page_id: str) -> NotionSyncResult:
        """
        Get a page by ID.

        Args:
            page_id: Notion page ID.

        Returns:
            NotionSyncResult with page data.
        """
        try:
            client = self._get_client()
            response = client.get(f"/pages/{page_id}")
            response.raise_for_status()

            page = NotionPage.from_notion_response(response.json())

            return NotionSyncResult(
                success=True,
                message=f"Retrieved page: {page.title}",
                data=page,
            )
        except Exception as e:
            logger.error(f"Failed to get page {page_id}: {e}")
            return NotionSyncResult(
                success=False,
                message=f"Failed to get page: {str(e)}",
                errors=[str(e)],
            )

    def create_page(
        self,
        parent_id: str,
        title: str,
        content: Optional[List[Dict[str, Any]]] = None,
        parent_type: str = "page_id",
        icon: Optional[str] = None,
        cover_url: Optional[str] = None,
        properties: Optional[Dict[str, Any]] = None,
    ) -> NotionSyncResult:
        """
        Create a new page.

        Args:
            parent_id: Parent page or database ID.
            title: Page title.
            content: Page content blocks.
            parent_type: Type of parent (page_id or database_id).
            icon: Emoji icon for the page.
            cover_url: URL for cover image.
            properties: Additional properties (for database pages).

        Returns:
            NotionSyncResult with created page.
        """
        try:
            client = self._get_client()

            # Build request body
            body: Dict[str, Any] = {
                "parent": {parent_type: parent_id},
            }

            # Set properties based on parent type
            if parent_type == "database_id":
                body["properties"] = properties or {
                    "Name": {"title": [{"text": {"content": title}}]}
                }
            else:
                body["properties"] = {
                    "title": {"title": [{"text": {"content": title}}]}
                }

            # Optional icon
            if icon:
                body["icon"] = {"type": "emoji", "emoji": icon}

            # Optional cover
            if cover_url:
                body["cover"] = {"type": "external", "external": {"url": cover_url}}

            # Optional content blocks
            if content:
                body["children"] = content

            response = client.post("/pages", json=body)
            response.raise_for_status()

            page = NotionPage.from_notion_response(response.json())

            return NotionSyncResult(
                success=True,
                message=f"Created page: {page.title}",
                pages_created=1,
                data=page,
            )
        except Exception as e:
            logger.error(f"Failed to create page: {e}")
            return NotionSyncResult(
                success=False,
                message=f"Failed to create page: {str(e)}",
                errors=[str(e)],
            )

    def update_page(
        self,
        page_id: str,
        title: Optional[str] = None,
        icon: Optional[str] = None,
        cover_url: Optional[str] = None,
        archived: Optional[bool] = None,
        properties: Optional[Dict[str, Any]] = None,
    ) -> NotionSyncResult:
        """
        Update an existing page.

        Args:
            page_id: Page ID to update.
            title: New title (optional).
            icon: New emoji icon (optional).
            cover_url: New cover image URL (optional).
            archived: Archive status (optional).
            properties: Additional properties to update.

        Returns:
            NotionSyncResult with updated page.
        """
        try:
            client = self._get_client()

            body: Dict[str, Any] = {}

            # Update properties
            if properties:
                body["properties"] = properties
            elif title:
                body["properties"] = {
                    "title": {"title": [{"text": {"content": title}}]}
                }

            if icon:
                body["icon"] = {"type": "emoji", "emoji": icon}

            if cover_url:
                body["cover"] = {"type": "external", "external": {"url": cover_url}}

            if archived is not None:
                body["archived"] = archived

            if not body:
                return NotionSyncResult(
                    success=False,
                    message="No updates specified",
                    errors=["Nothing to update"],
                )

            response = client.patch(f"/pages/{page_id}", json=body)
            response.raise_for_status()

            page = NotionPage.from_notion_response(response.json())

            return NotionSyncResult(
                success=True,
                message=f"Updated page: {page.title}",
                pages_updated=1,
                data=page,
            )
        except Exception as e:
            logger.error(f"Failed to update page {page_id}: {e}")
            return NotionSyncResult(
                success=False,
                message=f"Failed to update page: {str(e)}",
                errors=[str(e)],
            )

    def append_blocks(
        self,
        page_id: str,
        blocks: List[Dict[str, Any]],
    ) -> NotionSyncResult:
        """
        Append blocks to a page.

        Args:
            page_id: Page ID to append to.
            blocks: List of block objects.

        Returns:
            NotionSyncResult with operation status.
        """
        try:
            client = self._get_client()

            response = client.patch(
                f"/blocks/{page_id}/children",
                json={"children": blocks},
            )
            response.raise_for_status()

            return NotionSyncResult(
                success=True,
                message=f"Appended {len(blocks)} blocks",
                data=response.json(),
            )
        except Exception as e:
            logger.error(f"Failed to append blocks: {e}")
            return NotionSyncResult(
                success=False,
                message=f"Failed to append blocks: {str(e)}",
                errors=[str(e)],
            )

    # ==================== Database Operations ====================

    def get_database(self, database_id: str) -> NotionSyncResult:
        """
        Get a database by ID.

        Args:
            database_id: Notion database ID.

        Returns:
            NotionSyncResult with database data.
        """
        try:
            client = self._get_client()
            response = client.get(f"/databases/{database_id}")
            response.raise_for_status()

            database = NotionDatabase.from_notion_response(response.json())

            return NotionSyncResult(
                success=True,
                message=f"Retrieved database: {database.title}",
                data=database,
            )
        except Exception as e:
            logger.error(f"Failed to get database {database_id}: {e}")
            return NotionSyncResult(
                success=False,
                message=f"Failed to get database: {str(e)}",
                errors=[str(e)],
            )

    def query_database(
        self,
        database_id: str,
        filter_obj: Optional[Dict[str, Any]] = None,
        sorts: Optional[List[Dict[str, Any]]] = None,
        page_size: int = 100,
    ) -> NotionSyncResult:
        """
        Query a database.

        Args:
            database_id: Database ID to query.
            filter_obj: Filter conditions.
            sorts: Sort conditions.
            page_size: Maximum results per page.

        Returns:
            NotionSyncResult with query results.
        """
        try:
            client = self._get_client()

            body: Dict[str, Any] = {"page_size": page_size}
            if filter_obj:
                body["filter"] = filter_obj
            if sorts:
                body["sorts"] = sorts

            response = client.post(f"/databases/{database_id}/query", json=body)
            response.raise_for_status()

            data = response.json()
            pages = [
                NotionPage.from_notion_response(p)
                for p in data.get("results", [])
            ]

            return NotionSyncResult(
                success=True,
                message=f"Found {len(pages)} pages",
                data={
                    "pages": pages,
                    "has_more": data.get("has_more", False),
                    "next_cursor": data.get("next_cursor"),
                },
            )
        except Exception as e:
            logger.error(f"Failed to query database {database_id}: {e}")
            return NotionSyncResult(
                success=False,
                message=f"Failed to query database: {str(e)}",
                errors=[str(e)],
            )

    def create_database(
        self,
        parent_id: str,
        title: str,
        properties: Dict[str, Any],
        parent_type: str = "page_id",
        icon: Optional[str] = None,
    ) -> NotionSyncResult:
        """
        Create a new database.

        Args:
            parent_id: Parent page ID.
            title: Database title.
            properties: Database property schema.
            parent_type: Type of parent.
            icon: Emoji icon.

        Returns:
            NotionSyncResult with created database.
        """
        try:
            client = self._get_client()

            body: Dict[str, Any] = {
                "parent": {parent_type: parent_id},
                "title": [{"type": "text", "text": {"content": title}}],
                "properties": properties,
            }

            if icon:
                body["icon"] = {"type": "emoji", "emoji": icon}

            response = client.post("/databases", json=body)
            response.raise_for_status()

            database = NotionDatabase.from_notion_response(response.json())

            return NotionSyncResult(
                success=True,
                message=f"Created database: {database.title}",
                data=database,
            )
        except Exception as e:
            logger.error(f"Failed to create database: {e}")
            return NotionSyncResult(
                success=False,
                message=f"Failed to create database: {str(e)}",
                errors=[str(e)],
            )

    # ==================== Search ====================

    def search(
        self,
        query: str = "",
        filter_type: Optional[str] = None,
        page_size: int = 100,
    ) -> NotionSyncResult:
        """
        Search for pages and databases.

        Args:
            query: Search query.
            filter_type: Filter by "page" or "database".
            page_size: Maximum results.

        Returns:
            NotionSyncResult with search results.
        """
        try:
            client = self._get_client()

            body: Dict[str, Any] = {"page_size": page_size}
            if query:
                body["query"] = query
            if filter_type:
                body["filter"] = {"value": filter_type, "property": "object"}

            response = client.post("/search", json=body)
            response.raise_for_status()

            data = response.json()
            results = data.get("results", [])

            pages = []
            databases = []
            for item in results:
                if item.get("object") == "page":
                    pages.append(NotionPage.from_notion_response(item))
                elif item.get("object") == "database":
                    databases.append(NotionDatabase.from_notion_response(item))

            return NotionSyncResult(
                success=True,
                message=f"Found {len(pages)} pages and {len(databases)} databases",
                data={
                    "pages": pages,
                    "databases": databases,
                    "has_more": data.get("has_more", False),
                },
            )
        except Exception as e:
            logger.error(f"Search failed: {e}")
            return NotionSyncResult(
                success=False,
                message=f"Search failed: {str(e)}",
                errors=[str(e)],
            )

    # ==================== Block Helpers ====================

    @staticmethod
    def heading_block(text: str, level: int = 1) -> Dict[str, Any]:
        """Create a heading block."""
        block_type = f"heading_{min(max(level, 1), 3)}"
        return {
            "object": "block",
            "type": block_type,
            block_type: {
                "rich_text": [{"type": "text", "text": {"content": text}}]
            },
        }

    @staticmethod
    def paragraph_block(text: str) -> Dict[str, Any]:
        """Create a paragraph block."""
        return {
            "object": "block",
            "type": "paragraph",
            "paragraph": {
                "rich_text": [{"type": "text", "text": {"content": text}}]
            },
        }

    @staticmethod
    def bullet_list_block(text: str) -> Dict[str, Any]:
        """Create a bulleted list item block."""
        return {
            "object": "block",
            "type": "bulleted_list_item",
            "bulleted_list_item": {
                "rich_text": [{"type": "text", "text": {"content": text}}]
            },
        }

    @staticmethod
    def code_block(code: str, language: str = "python") -> Dict[str, Any]:
        """Create a code block."""
        return {
            "object": "block",
            "type": "code",
            "code": {
                "rich_text": [{"type": "text", "text": {"content": code}}],
                "language": language,
            },
        }

    @staticmethod
    def divider_block() -> Dict[str, Any]:
        """Create a divider block."""
        return {
            "object": "block",
            "type": "divider",
            "divider": {},
        }

    @staticmethod
    def callout_block(text: str, icon: str = "💡") -> Dict[str, Any]:
        """Create a callout block."""
        return {
            "object": "block",
            "type": "callout",
            "callout": {
                "rich_text": [{"type": "text", "text": {"content": text}}],
                "icon": {"type": "emoji", "emoji": icon},
            },
        }

    # ==================== Sync Operations ====================

    def sync_documentation(
        self,
        parent_page_id: str,
        docs: List[Dict[str, Any]],
    ) -> NotionSyncResult:
        """
        Sync documentation pages to Notion.

        Args:
            parent_page_id: Parent page to create docs under.
            docs: List of doc dicts with 'title', 'content', 'icon'.

        Returns:
            NotionSyncResult with sync status.
        """
        created = 0
        updated = 0
        errors = []

        for doc in docs:
            title = doc.get("title", "Untitled")
            content = doc.get("content", "")
            icon = doc.get("icon", "📄")

            # Convert content to blocks
            blocks = []
            for line in content.split("\n\n"):
                if line.startswith("# "):
                    blocks.append(self.heading_block(line[2:], level=1))
                elif line.startswith("## "):
                    blocks.append(self.heading_block(line[3:], level=2))
                elif line.startswith("### "):
                    blocks.append(self.heading_block(line[4:], level=3))
                elif line.startswith("- "):
                    for item in line.split("\n"):
                        if item.startswith("- "):
                            blocks.append(self.bullet_list_block(item[2:]))
                elif line.strip():
                    blocks.append(self.paragraph_block(line))

            # Create page
            result = self.create_page(
                parent_id=parent_page_id,
                title=title,
                content=blocks,
                icon=icon,
            )

            if result.success:
                created += 1
            else:
                errors.extend(result.errors)

        return NotionSyncResult(
            success=len(errors) == 0,
            message=f"Synced {created} docs, {len(errors)} errors",
            pages_created=created,
            pages_updated=updated,
            errors=errors,
        )

    def close(self) -> None:
        """Close the HTTP client."""
        if self._client:
            self._client.close()
            self._client = None

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()
