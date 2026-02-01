"""
Unit tests for NotionSync service.

Tests Notion API integration with mocked HTTP responses.
"""

import pytest
from unittest.mock import MagicMock, patch, PropertyMock
from datetime import datetime

from src.integrations.notion_sync import (
    NotionSync,
    NotionPage,
    NotionDatabase,
    NotionSyncResult,
)


class MockHTTPStatusError(Exception):
    """Mock HTTPStatusError for testing."""
    def __init__(self, message, request=None, response=None):
        super().__init__(message)
        self.request = request
        self.response = response


@pytest.fixture
def mock_httpx():
    """Mock httpx client while preserving exception classes."""
    with patch("src.integrations.notion_sync.httpx") as mock:
        mock.Client = MagicMock()
        mock.HTTPStatusError = MockHTTPStatusError
        yield mock


@pytest.fixture
def notion_sync(mock_httpx):
    """Create NotionSync with mocked client."""
    client_instance = MagicMock()
    mock_httpx.Client.return_value = client_instance

    sync = NotionSync(api_key="test-api-key", workspace_id="ws-123")
    sync._client = client_instance
    return sync


class TestNotionPage:
    """Tests for NotionPage dataclass."""

    def test_to_dict(self):
        page = NotionPage(
            page_id="page-123",
            title="Test Page",
            parent_id="parent-456",
            url="https://notion.so/page-123",
            created_time="2024-01-01T00:00:00Z",
        )
        data = page.to_dict()
        assert data["page_id"] == "page-123"
        assert data["title"] == "Test Page"
        assert data["parent_id"] == "parent-456"

    def test_from_notion_response_with_title(self):
        response = {
            "id": "page-123",
            "url": "https://notion.so/page-123",
            "created_time": "2024-01-01T00:00:00Z",
            "last_edited_time": "2024-01-02T00:00:00Z",
            "archived": False,
            "parent": {"type": "page_id", "page_id": "parent-456"},
            "properties": {
                "title": {
                    "title": [{"plain_text": "My Page Title"}]
                }
            },
            "icon": {"emoji": "📄"},
        }
        page = NotionPage.from_notion_response(response)
        assert page.page_id == "page-123"
        assert page.title == "My Page Title"
        assert page.parent_id == "parent-456"
        assert page.parent_type == "page_id"
        assert page.icon == "📄"

    def test_from_notion_response_with_name(self):
        """Test parsing when title is in 'Name' property (database pages)."""
        response = {
            "id": "page-123",
            "parent": {"type": "database_id", "database_id": "db-456"},
            "properties": {
                "Name": {
                    "title": [{"plain_text": "Database Page Title"}]
                }
            },
        }
        page = NotionPage.from_notion_response(response)
        assert page.title == "Database Page Title"
        assert page.parent_type == "database_id"
        assert page.parent_id == "db-456"

    def test_from_notion_response_no_title(self):
        response = {
            "id": "page-123",
            "parent": {"type": "workspace"},
            "properties": {},
        }
        page = NotionPage.from_notion_response(response)
        assert page.title == ""

    def test_from_notion_response_empty_title_array(self):
        response = {
            "id": "page-123",
            "parent": {"type": "page_id", "page_id": "p-123"},
            "properties": {
                "title": {"title": []}
            },
        }
        page = NotionPage.from_notion_response(response)
        assert page.title == ""


class TestNotionDatabase:
    """Tests for NotionDatabase dataclass."""

    def test_to_dict(self):
        db = NotionDatabase(
            database_id="db-123",
            title="Test Database",
            description="A test database",
            url="https://notion.so/db-123",
        )
        data = db.to_dict()
        assert data["database_id"] == "db-123"
        assert data["title"] == "Test Database"
        assert data["description"] == "A test database"

    def test_from_notion_response(self):
        response = {
            "id": "db-123",
            "url": "https://notion.so/db-123",
            "created_time": "2024-01-01T00:00:00Z",
            "last_edited_time": "2024-01-02T00:00:00Z",
            "title": [{"plain_text": "My Database"}],
            "description": [{"plain_text": "Database description"}],
            "properties": {
                "Name": {"type": "title"},
                "Status": {"type": "select"},
            },
        }
        db = NotionDatabase.from_notion_response(response)
        assert db.database_id == "db-123"
        assert db.title == "My Database"
        assert db.description == "Database description"
        assert "Name" in db.properties

    def test_from_notion_response_empty_arrays(self):
        response = {
            "id": "db-123",
            "title": [],
            "description": [],
            "properties": {},
        }
        db = NotionDatabase.from_notion_response(response)
        assert db.title == ""
        assert db.description == ""


class TestNotionSyncResult:
    """Tests for NotionSyncResult dataclass."""

    def test_success_result(self):
        result = NotionSyncResult(
            success=True,
            message="Operation completed",
            pages_created=2,
            pages_updated=1,
        )
        assert result.success is True
        assert result.pages_created == 2
        assert result.errors == []

    def test_failure_result(self):
        result = NotionSyncResult(
            success=False,
            message="Operation failed",
            errors=["Error 1", "Error 2"],
        )
        assert result.success is False
        assert len(result.errors) == 2

    def test_to_dict(self):
        result = NotionSyncResult(
            success=True,
            message="Done",
            pages_created=1,
            data={"key": "value"},
        )
        data = result.to_dict()
        assert data["success"] is True
        assert data["pages_created"] == 1
        assert data["data"]["key"] == "value"


class TestNotionSyncInit:
    """Tests for NotionSync initialization."""

    def test_init_with_api_key(self, mock_httpx):
        sync = NotionSync(api_key="test-key")
        assert sync.api_key == "test-key"
        assert sync.workspace_id is None
        assert sync.timeout == 30.0

    def test_init_with_workspace_id(self, mock_httpx):
        sync = NotionSync(api_key="test-key", workspace_id="ws-123")
        assert sync.workspace_id == "ws-123"

    def test_init_with_custom_timeout(self, mock_httpx):
        sync = NotionSync(api_key="test-key", timeout=60.0)
        assert sync.timeout == 60.0

    def test_init_without_httpx(self):
        with patch("src.integrations.notion_sync.HTTPX_AVAILABLE", False):
            with pytest.raises(ImportError, match="httpx is required"):
                NotionSync(api_key="test-key")


class TestNotionSyncConnection:
    """Tests for connection operations."""

    def test_test_connection_success(self, notion_sync):
        notion_sync._client.get.return_value = MagicMock(
            json=lambda: {"name": "DataBridge Bot"},
            raise_for_status=lambda: None,
        )

        result = notion_sync.test_connection()
        assert result.success is True
        assert "DataBridge Bot" in result.message
        assert result.data["name"] == "DataBridge Bot"

    def test_test_connection_auth_failure(self, notion_sync, mock_httpx):
        # Create a mock that raises HTTPStatusError
        response_mock = MagicMock()
        response_mock.status_code = 401

        def raise_http_error():
            raise mock_httpx.HTTPStatusError("Auth failed", request=MagicMock(), response=response_mock)

        notion_sync._client.get.return_value.raise_for_status = raise_http_error

        result = notion_sync.test_connection()
        assert result.success is False
        assert "Authentication failed" in result.message or "Auth failed" in str(result.errors)

    def test_test_connection_network_error(self, notion_sync):
        # Use a generic exception that will be caught by the outer except block
        notion_sync._client.get.side_effect = RuntimeError("Network error")

        result = notion_sync.test_connection()
        assert result.success is False
        assert "Connection failed" in result.message or "Network error" in str(result.errors)


class TestNotionSyncPageOperations:
    """Tests for page operations."""

    def test_get_page_success(self, notion_sync):
        notion_sync._client.get.return_value = MagicMock(
            json=lambda: {
                "id": "page-123",
                "properties": {"title": {"title": [{"plain_text": "Test Page"}]}},
                "parent": {"type": "page_id", "page_id": "parent-456"},
            },
            raise_for_status=lambda: None,
        )

        result = notion_sync.get_page("page-123")
        assert result.success is True
        assert result.data.page_id == "page-123"
        assert result.data.title == "Test Page"

    def test_get_page_failure(self, notion_sync):
        notion_sync._client.get.side_effect = Exception("Not found")

        result = notion_sync.get_page("invalid-id")
        assert result.success is False
        assert "Failed to get page" in result.message

    def test_create_page_success(self, notion_sync):
        notion_sync._client.post.return_value = MagicMock(
            json=lambda: {
                "id": "new-page-123",
                "properties": {"title": {"title": [{"plain_text": "New Page"}]}},
                "parent": {"type": "page_id", "page_id": "parent-456"},
            },
            raise_for_status=lambda: None,
        )

        result = notion_sync.create_page(
            parent_id="parent-456",
            title="New Page",
            icon="📄",
        )

        assert result.success is True
        assert result.pages_created == 1
        assert result.data.title == "New Page"

        # Verify request body
        call_args = notion_sync._client.post.call_args
        assert call_args[0][0] == "/pages"
        body = call_args[1]["json"]
        assert body["parent"]["page_id"] == "parent-456"
        assert body["icon"]["emoji"] == "📄"

    def test_create_page_with_content(self, notion_sync):
        notion_sync._client.post.return_value = MagicMock(
            json=lambda: {
                "id": "new-page-123",
                "properties": {"title": {"title": [{"plain_text": "Page with Content"}]}},
                "parent": {"type": "page_id", "page_id": "parent-456"},
            },
            raise_for_status=lambda: None,
        )

        content = [
            NotionSync.heading_block("Introduction", level=1),
            NotionSync.paragraph_block("This is the content."),
        ]

        result = notion_sync.create_page(
            parent_id="parent-456",
            title="Page with Content",
            content=content,
        )

        assert result.success is True
        call_args = notion_sync._client.post.call_args
        body = call_args[1]["json"]
        assert "children" in body
        assert len(body["children"]) == 2

    def test_create_page_in_database(self, notion_sync):
        notion_sync._client.post.return_value = MagicMock(
            json=lambda: {
                "id": "new-page-123",
                "properties": {"Name": {"title": [{"plain_text": "DB Page"}]}},
                "parent": {"type": "database_id", "database_id": "db-456"},
            },
            raise_for_status=lambda: None,
        )

        result = notion_sync.create_page(
            parent_id="db-456",
            title="DB Page",
            parent_type="database_id",
        )

        assert result.success is True
        call_args = notion_sync._client.post.call_args
        body = call_args[1]["json"]
        assert body["parent"]["database_id"] == "db-456"
        assert "Name" in body["properties"]

    def test_create_page_failure(self, notion_sync):
        notion_sync._client.post.side_effect = Exception("API error")

        result = notion_sync.create_page(
            parent_id="parent-456",
            title="Test",
        )

        assert result.success is False
        assert "Failed to create page" in result.message

    def test_update_page_title(self, notion_sync):
        notion_sync._client.patch.return_value = MagicMock(
            json=lambda: {
                "id": "page-123",
                "properties": {"title": {"title": [{"plain_text": "Updated Title"}]}},
                "parent": {"type": "page_id", "page_id": "parent-456"},
            },
            raise_for_status=lambda: None,
        )

        result = notion_sync.update_page(
            page_id="page-123",
            title="Updated Title",
        )

        assert result.success is True
        assert result.pages_updated == 1

    def test_update_page_archive(self, notion_sync):
        notion_sync._client.patch.return_value = MagicMock(
            json=lambda: {
                "id": "page-123",
                "archived": True,
                "properties": {"title": {"title": [{"plain_text": "Archived"}]}},
                "parent": {"type": "page_id", "page_id": "p-456"},
            },
            raise_for_status=lambda: None,
        )

        result = notion_sync.update_page(
            page_id="page-123",
            archived=True,
        )

        assert result.success is True
        call_args = notion_sync._client.patch.call_args
        body = call_args[1]["json"]
        assert body["archived"] is True

    def test_update_page_no_changes(self, notion_sync):
        result = notion_sync.update_page(page_id="page-123")
        assert result.success is False
        assert "No updates specified" in result.message

    def test_append_blocks_success(self, notion_sync):
        notion_sync._client.patch.return_value = MagicMock(
            json=lambda: {"results": []},
            raise_for_status=lambda: None,
        )

        blocks = [
            NotionSync.paragraph_block("New paragraph"),
            NotionSync.bullet_list_block("List item"),
        ]

        result = notion_sync.append_blocks("page-123", blocks)
        assert result.success is True
        assert "2 blocks" in result.message


class TestNotionSyncDatabaseOperations:
    """Tests for database operations."""

    def test_get_database_success(self, notion_sync):
        notion_sync._client.get.return_value = MagicMock(
            json=lambda: {
                "id": "db-123",
                "title": [{"plain_text": "Test DB"}],
                "description": [],
                "properties": {"Name": {"type": "title"}},
            },
            raise_for_status=lambda: None,
        )

        result = notion_sync.get_database("db-123")
        assert result.success is True
        assert result.data.database_id == "db-123"
        assert result.data.title == "Test DB"

    def test_query_database_success(self, notion_sync):
        notion_sync._client.post.return_value = MagicMock(
            json=lambda: {
                "results": [
                    {
                        "id": "page-1",
                        "properties": {"Name": {"title": [{"plain_text": "Item 1"}]}},
                        "parent": {"type": "database_id", "database_id": "db-123"},
                    },
                    {
                        "id": "page-2",
                        "properties": {"Name": {"title": [{"plain_text": "Item 2"}]}},
                        "parent": {"type": "database_id", "database_id": "db-123"},
                    },
                ],
                "has_more": False,
                "next_cursor": None,
            },
            raise_for_status=lambda: None,
        )

        result = notion_sync.query_database("db-123")
        assert result.success is True
        assert len(result.data["pages"]) == 2
        assert result.data["has_more"] is False

    def test_query_database_with_filter(self, notion_sync):
        notion_sync._client.post.return_value = MagicMock(
            json=lambda: {"results": [], "has_more": False},
            raise_for_status=lambda: None,
        )

        filter_obj = {
            "property": "Status",
            "select": {"equals": "Active"},
        }

        notion_sync.query_database(
            "db-123",
            filter_obj=filter_obj,
            sorts=[{"property": "Name", "direction": "ascending"}],
        )

        call_args = notion_sync._client.post.call_args
        body = call_args[1]["json"]
        assert "filter" in body
        assert "sorts" in body

    def test_create_database_success(self, notion_sync):
        notion_sync._client.post.return_value = MagicMock(
            json=lambda: {
                "id": "new-db-123",
                "title": [{"plain_text": "New Database"}],
                "description": [],
                "properties": {},
            },
            raise_for_status=lambda: None,
        )

        properties = {
            "Name": {"title": {}},
            "Status": {
                "select": {
                    "options": [
                        {"name": "Active", "color": "green"},
                        {"name": "Inactive", "color": "red"},
                    ]
                }
            },
        }

        result = notion_sync.create_database(
            parent_id="page-456",
            title="New Database",
            properties=properties,
            icon="📊",
        )

        assert result.success is True
        assert result.data.database_id == "new-db-123"


class TestNotionSyncSearch:
    """Tests for search operations."""

    def test_search_all(self, notion_sync):
        notion_sync._client.post.return_value = MagicMock(
            json=lambda: {
                "results": [
                    {
                        "object": "page",
                        "id": "page-1",
                        "properties": {"title": {"title": [{"plain_text": "Page 1"}]}},
                        "parent": {"type": "workspace"},
                    },
                    {
                        "object": "database",
                        "id": "db-1",
                        "title": [{"plain_text": "Database 1"}],
                        "description": [],
                        "properties": {},
                    },
                ],
                "has_more": False,
            },
            raise_for_status=lambda: None,
        )

        result = notion_sync.search("test")
        assert result.success is True
        assert len(result.data["pages"]) == 1
        assert len(result.data["databases"]) == 1

    def test_search_pages_only(self, notion_sync):
        notion_sync._client.post.return_value = MagicMock(
            json=lambda: {
                "results": [
                    {
                        "object": "page",
                        "id": "page-1",
                        "properties": {"title": {"title": [{"plain_text": "Page"}]}},
                        "parent": {"type": "workspace"},
                    },
                ],
                "has_more": False,
            },
            raise_for_status=lambda: None,
        )

        notion_sync.search("test", filter_type="page")

        call_args = notion_sync._client.post.call_args
        body = call_args[1]["json"]
        assert body["filter"]["value"] == "page"


class TestNotionSyncBlockHelpers:
    """Tests for block helper methods."""

    def test_heading_block_level_1(self):
        block = NotionSync.heading_block("Title", level=1)
        assert block["type"] == "heading_1"
        assert block["heading_1"]["rich_text"][0]["text"]["content"] == "Title"

    def test_heading_block_level_2(self):
        block = NotionSync.heading_block("Subtitle", level=2)
        assert block["type"] == "heading_2"

    def test_heading_block_level_3(self):
        block = NotionSync.heading_block("Section", level=3)
        assert block["type"] == "heading_3"

    def test_heading_block_clamped(self):
        block = NotionSync.heading_block("Test", level=5)
        assert block["type"] == "heading_3"  # Clamped to max 3

    def test_paragraph_block(self):
        block = NotionSync.paragraph_block("This is text.")
        assert block["type"] == "paragraph"
        assert block["paragraph"]["rich_text"][0]["text"]["content"] == "This is text."

    def test_bullet_list_block(self):
        block = NotionSync.bullet_list_block("List item")
        assert block["type"] == "bulleted_list_item"
        content = block["bulleted_list_item"]["rich_text"][0]["text"]["content"]
        assert content == "List item"

    def test_code_block(self):
        block = NotionSync.code_block("print('hello')", language="python")
        assert block["type"] == "code"
        assert block["code"]["language"] == "python"
        assert block["code"]["rich_text"][0]["text"]["content"] == "print('hello')"

    def test_code_block_default_language(self):
        block = NotionSync.code_block("code")
        assert block["code"]["language"] == "python"

    def test_divider_block(self):
        block = NotionSync.divider_block()
        assert block["type"] == "divider"

    def test_callout_block(self):
        block = NotionSync.callout_block("Important note", icon="⚠️")
        assert block["type"] == "callout"
        assert block["callout"]["icon"]["emoji"] == "⚠️"
        assert block["callout"]["rich_text"][0]["text"]["content"] == "Important note"

    def test_callout_block_default_icon(self):
        block = NotionSync.callout_block("Note")
        assert block["callout"]["icon"]["emoji"] == "💡"


class TestNotionSyncDocumentation:
    """Tests for documentation sync operations."""

    def test_sync_documentation_success(self, notion_sync):
        notion_sync._client.post.return_value = MagicMock(
            json=lambda: {
                "id": "new-page",
                "properties": {"title": {"title": [{"plain_text": "Doc"}]}},
                "parent": {"type": "page_id", "page_id": "parent"},
            },
            raise_for_status=lambda: None,
        )

        docs = [
            {
                "title": "Getting Started",
                "content": "# Introduction\n\nWelcome to the docs.\n\n- Feature 1\n- Feature 2",
                "icon": "🚀",
            },
            {
                "title": "API Reference",
                "content": "## Endpoints\n\nAPI documentation here.",
                "icon": "📖",
            },
        ]

        result = notion_sync.sync_documentation("parent-page", docs)
        assert result.success is True
        assert result.pages_created == 2
        assert len(result.errors) == 0

    def test_sync_documentation_partial_failure(self, notion_sync):
        call_count = [0]

        def mock_post(*args, **kwargs):
            call_count[0] += 1
            if call_count[0] == 1:
                return MagicMock(
                    json=lambda: {
                        "id": "page-1",
                        "properties": {"title": {"title": [{"plain_text": "Doc 1"}]}},
                        "parent": {"type": "page_id", "page_id": "parent"},
                    },
                    raise_for_status=lambda: None,
                )
            else:
                raise Exception("API Error")

        notion_sync._client.post.side_effect = mock_post

        docs = [
            {"title": "Doc 1", "content": "Content 1"},
            {"title": "Doc 2", "content": "Content 2"},
        ]

        result = notion_sync.sync_documentation("parent-page", docs)
        assert result.success is False
        assert result.pages_created == 1
        assert len(result.errors) == 1


class TestNotionSyncContextManager:
    """Tests for context manager protocol."""

    def test_context_manager(self, mock_httpx):
        client_mock = MagicMock()
        mock_httpx.Client.return_value = client_mock

        with NotionSync(api_key="test-key") as sync:
            sync._client = client_mock
            assert sync is not None

        client_mock.close.assert_called_once()

    def test_close(self, notion_sync):
        # Store reference to the client before close sets it to None
        client_ref = notion_sync._client
        notion_sync.close()
        client_ref.close.assert_called_once()
        assert notion_sync._client is None

    def test_close_no_client(self, mock_httpx):
        sync = NotionSync(api_key="test-key")
        sync._client = None
        sync.close()  # Should not raise
