"""
MCP Tools for the Console Dashboard WebSocket Server.

Provides 5 tools for controlling the console server:
- start_console_server
- stop_console_server
- get_console_connections
- broadcast_console_message
- get_console_server_status
"""

import asyncio
import logging
from typing import Any, Dict, List, Optional

from .server import get_server, reset_server, ConsoleServer
from .broadcaster import ConsoleBroadcaster
from .types import (
    WebSocketMessage,
    WebSocketMessageType,
    ConsoleLogLevel,
)

logger = logging.getLogger(__name__)


def register_console_tools(mcp, settings=None) -> Dict[str, Any]:
    """
    Register Console Dashboard MCP tools.

    Args:
        mcp: The FastMCP instance
        settings: Optional settings

    Returns:
        Dict with registration info
    """

    # Track server instance
    _server: Optional[ConsoleServer] = None

    @mcp.tool()
    async def start_console_server(
        port: int = 8080,
        host: str = "0.0.0.0",
        redis_url: Optional[str] = None,
    ) -> Dict[str, Any]:
        """
        Start the WebSocket console server.

        Opens a web-based dashboard for real-time monitoring of:
        - Console log entries from all MCP tools
        - Reasoning loop steps (OBSERVE → PLAN → EXECUTE → REFLECT)
        - Agent activity and inter-agent messages
        - Cortex AI queries and responses

        Args:
            port: Port number (default: 8080)
            host: Host address (default: 0.0.0.0 for all interfaces)
            redis_url: Optional Redis URL for multi-instance broadcasting

        Returns:
            Server status with URL

        Example:
            start_console_server(port=8080)
            # Open http://localhost:8080 in browser
        """
        nonlocal _server

        try:
            # Reset any existing server
            if _server and _server.is_running:
                await _server.stop()
                reset_server()

            # Create and start new server
            _server = get_server(host, port, redis_url)
            await _server.start()

            return {
                "success": True,
                "message": f"Console server started on http://{host}:{port}",
                "url": f"http://localhost:{port}" if host == "0.0.0.0" else f"http://{host}:{port}",
                "websocket_url": f"ws://localhost:{port}/ws/console",
                "status": _server.get_status(),
            }

        except Exception as e:
            logger.error(f"Failed to start console server: {e}")
            return {
                "success": False,
                "error": str(e),
            }

    @mcp.tool()
    async def stop_console_server() -> Dict[str, Any]:
        """
        Stop the WebSocket console server.

        Gracefully shuts down the server and disconnects all clients.

        Returns:
            Success status

        Example:
            stop_console_server()
        """
        nonlocal _server

        try:
            if _server and _server.is_running:
                await _server.stop()
                reset_server()
                _server = None
                return {
                    "success": True,
                    "message": "Console server stopped",
                }
            else:
                return {
                    "success": True,
                    "message": "Console server was not running",
                }

        except Exception as e:
            logger.error(f"Failed to stop console server: {e}")
            return {
                "success": False,
                "error": str(e),
            }

    @mcp.tool()
    def get_console_connections() -> Dict[str, Any]:
        """
        List active WebSocket connections to the console server.

        Returns information about each connected client including:
        - Connection ID
        - Client IP address
        - Connection time
        - Subscribed channels
        - Message count
        - Last activity time

        Returns:
            List of connection info

        Example:
            get_console_connections()
        """
        nonlocal _server

        if not _server or not _server.is_running:
            return {
                "success": False,
                "error": "Console server is not running",
                "connections": [],
            }

        connections = _server.connection_manager.get_connections()

        return {
            "success": True,
            "total_connections": len(connections),
            "connections": [
                {
                    "connection_id": conn.connection_id,
                    "client_ip": conn.client_ip,
                    "connected_at": conn.connected_at.isoformat(),
                    "subscriptions": conn.subscriptions,
                    "message_count": conn.message_count,
                    "last_activity": conn.last_activity.isoformat(),
                }
                for conn in connections
            ],
        }

    @mcp.tool()
    def broadcast_console_message(
        message: str,
        level: str = "info",
        source: str = "system",
        channel: str = "console",
        conversation_id: Optional[str] = None,
        metadata: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, Any]:
        """
        Broadcast a message to all connected console clients.

        Sends a message to clients subscribed to the specified channel.
        Useful for sending system notifications, status updates, or
        custom messages to the dashboard.

        Args:
            message: The message text to broadcast
            level: Log level (debug, info, warning, error, success)
            source: Source identifier (e.g., tool name, agent name)
            channel: Channel to broadcast on (console, reasoning, agents, cortex)
            conversation_id: Optional conversation filter
            metadata: Optional additional metadata

        Returns:
            Broadcast result with recipient count

        Example:
            broadcast_console_message(
                message="Data reconciliation complete",
                level="success",
                source="reconciler"
            )
        """
        try:
            broadcaster = ConsoleBroadcaster.get_instance()

            # Map string level to enum
            level_map = {
                "debug": ConsoleLogLevel.DEBUG,
                "info": ConsoleLogLevel.INFO,
                "warning": ConsoleLogLevel.WARNING,
                "error": ConsoleLogLevel.ERROR,
                "success": ConsoleLogLevel.SUCCESS,
            }
            log_level = level_map.get(level.lower(), ConsoleLogLevel.INFO)

            # Use convenience method
            broadcaster.log(
                message=message,
                level=log_level,
                source=source,
                conversation_id=conversation_id,
                **(metadata or {}),
            )

            return {
                "success": True,
                "message": f"Broadcast sent to channel '{channel}'",
                "channel": channel,
                "level": level,
            }

        except Exception as e:
            logger.error(f"Failed to broadcast message: {e}")
            return {
                "success": False,
                "error": str(e),
            }

    @mcp.tool()
    def get_console_server_status() -> Dict[str, Any]:
        """
        Get the current status of the console server.

        Returns detailed information about:
        - Server running state
        - Host and port configuration
        - Active connection count
        - Broadcaster backend (memory/redis)
        - Channel subscription counts
        - Message history size

        Returns:
            Server status information

        Example:
            get_console_server_status()
        """
        nonlocal _server

        if not _server:
            return {
                "success": True,
                "running": False,
                "message": "Console server has not been started",
            }

        status = _server.get_status()

        return {
            "success": True,
            **status,
        }

    # Return registration info
    return {
        "tools_registered": 5,
        "tools": [
            "start_console_server",
            "stop_console_server",
            "get_console_connections",
            "broadcast_console_message",
            "get_console_server_status",
        ],
    }
