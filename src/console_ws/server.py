"""
FastAPI WebSocket Server for the Console Dashboard.

Provides real-time streaming of:
- Console log entries
- Reasoning loop steps
- Agent activity updates
- Cortex AI interactions
"""

import asyncio
import logging
from contextlib import asynccontextmanager
from typing import Optional
import uvicorn

from fastapi import FastAPI, WebSocket, WebSocketDisconnect, Query
from fastapi.staticfiles import StaticFiles
from fastapi.responses import HTMLResponse, JSONResponse
from pathlib import Path

from .handlers import ConnectionManager
from .broadcaster import ConsoleBroadcaster
from .types import WebSocketMessage, WebSocketMessageType

logger = logging.getLogger(__name__)


class ConsoleServer:
    """
    WebSocket server for the Console Dashboard.

    Manages the FastAPI application and WebSocket connections.
    """

    def __init__(
        self,
        host: str = "0.0.0.0",
        port: int = 8080,
        redis_url: Optional[str] = None,
    ):
        self.host = host
        self.port = port
        self.redis_url = redis_url
        self._app: Optional[FastAPI] = None
        self._server: Optional[uvicorn.Server] = None
        self._server_task: Optional[asyncio.Task] = None
        self._running = False

        # Initialize broadcaster and connection manager
        self.broadcaster = ConsoleBroadcaster.get_instance(redis_url)
        self.connection_manager = ConnectionManager(self.broadcaster)

    def create_app(self) -> FastAPI:
        """Create the FastAPI application."""

        @asynccontextmanager
        async def lifespan(app: FastAPI):
            # Startup
            logger.info(f"Console server starting on {self.host}:{self.port}")
            self._running = True
            yield
            # Shutdown
            logger.info("Console server shutting down")
            self._running = False

        app = FastAPI(
            title="DataBridge Console",
            description="Real-time agent activity streaming",
            version="1.0.0",
            lifespan=lifespan,
        )

        # Mount static files for dashboard
        dashboard_path = Path(__file__).parent.parent.parent / "console_dashboard"
        if dashboard_path.exists():
            app.mount("/static", StaticFiles(directory=str(dashboard_path)), name="static")

        # Register routes
        self._register_routes(app)

        self._app = app
        return app

    def _register_routes(self, app: FastAPI) -> None:
        """Register HTTP and WebSocket routes."""

        @app.get("/", response_class=HTMLResponse)
        async def root():
            """Serve the dashboard or redirect info."""
            dashboard_path = Path(__file__).parent.parent.parent / "console_dashboard" / "index.html"
            if dashboard_path.exists():
                return HTMLResponse(content=dashboard_path.read_text())
            return HTMLResponse(content="""
                <html>
                    <head><title>DataBridge Console</title></head>
                    <body>
                        <h1>DataBridge Console Server</h1>
                        <p>WebSocket endpoint: <code>ws://localhost:{port}/ws/console</code></p>
                        <p>Dashboard not found. Place index.html in console_dashboard/</p>
                    </body>
                </html>
            """.format(port=self.port))

        @app.get("/health")
        async def health():
            """Health check endpoint."""
            return JSONResponse({
                "status": "healthy",
                "connections": self.connection_manager.get_connection_count(),
                "broadcaster": self.broadcaster.get_stats(),
            })

        @app.get("/stats")
        async def stats():
            """Get server statistics."""
            return JSONResponse({
                "connections": self.connection_manager.get_stats(),
                "broadcaster": self.broadcaster.get_stats(),
            })

        @app.get("/connections")
        async def connections():
            """List active connections."""
            return JSONResponse({
                "connections": [
                    conn.model_dump()
                    for conn in self.connection_manager.get_connections()
                ]
            })

        @app.websocket("/ws/console")
        async def websocket_console(
            websocket: WebSocket,
            channels: str = Query(default="console,reasoning,agents,cortex"),
        ):
            """Main console WebSocket endpoint."""
            await self._handle_websocket(websocket, channels.split(","))

        @app.websocket("/ws/console/{session_id}")
        async def websocket_console_session(
            websocket: WebSocket,
            session_id: str,
        ):
            """Session-specific console WebSocket endpoint."""
            await self._handle_websocket(websocket, ["console", "reasoning", "agents", "cortex"], session_id)

        @app.websocket("/ws/agent/{agent_id}")
        async def websocket_agent(
            websocket: WebSocket,
            agent_id: str,
        ):
            """Agent-specific WebSocket endpoint."""
            await self._handle_websocket(websocket, ["agents"], filter_agent=agent_id)

        @app.websocket("/ws/reasoning/{conversation_id}")
        async def websocket_reasoning(
            websocket: WebSocket,
            conversation_id: str,
        ):
            """Conversation-specific reasoning WebSocket endpoint."""
            await self._handle_websocket(websocket, ["reasoning", "cortex"], filter_conversation=conversation_id)

    async def _handle_websocket(
        self,
        websocket: WebSocket,
        channels: list,
        filter_conversation: Optional[str] = None,
        filter_agent: Optional[str] = None,
    ) -> None:
        """Handle a WebSocket connection."""
        await websocket.accept()

        # Get client IP
        client_ip = "unknown"
        if websocket.client:
            client_ip = websocket.client.host

        # Register connection
        connection = await self.connection_manager.connect(websocket, client_ip)

        try:
            # Subscribe to requested channels
            subscribe_msg = WebSocketMessage(
                type=WebSocketMessageType.SUBSCRIBE,
                payload={
                    "channels": channels,
                    "conversation_id": filter_conversation,
                    "agent_id": filter_agent,
                },
            )
            response = await self.connection_manager.handle_message(
                connection, subscribe_msg.model_dump_json()
            )
            if response:
                await connection.send(response)

            # Handle incoming messages
            while True:
                data = await websocket.receive_text()
                response = await self.connection_manager.handle_message(connection, data)
                if response:
                    await connection.send(response)

        except WebSocketDisconnect:
            logger.info(f"Client {connection.connection_id} disconnected")
        except Exception as e:
            logger.error(f"WebSocket error: {e}")
        finally:
            await self.connection_manager.disconnect(connection.connection_id)

    async def start(self) -> None:
        """Start the server."""
        if self._running:
            logger.warning("Server already running")
            return

        app = self.create_app()
        config = uvicorn.Config(
            app,
            host=self.host,
            port=self.port,
            log_level="info",
        )
        self._server = uvicorn.Server(config)
        self._server_task = asyncio.create_task(self._server.serve())
        self._running = True

        logger.info(f"Console server started on http://{self.host}:{self.port}")

    async def stop(self) -> None:
        """Stop the server."""
        if not self._running:
            return

        if self._server:
            self._server.should_exit = True
            if self._server_task:
                try:
                    await asyncio.wait_for(self._server_task, timeout=5.0)
                except asyncio.TimeoutError:
                    self._server_task.cancel()

        self._running = False
        self._server = None
        self._server_task = None

        logger.info("Console server stopped")

    @property
    def is_running(self) -> bool:
        """Check if server is running."""
        return self._running

    def get_status(self) -> dict:
        """Get server status."""
        return {
            "running": self._running,
            "host": self.host,
            "port": self.port,
            "url": f"http://{self.host}:{self.port}" if self._running else None,
            "connections": self.connection_manager.get_connection_count() if self._running else 0,
            "broadcaster": self.broadcaster.get_stats() if self._running else None,
        }


# Singleton instance
_server_instance: Optional[ConsoleServer] = None


def get_server(
    host: str = "0.0.0.0",
    port: int = 8080,
    redis_url: Optional[str] = None,
) -> ConsoleServer:
    """Get or create the singleton server instance."""
    global _server_instance
    if _server_instance is None:
        _server_instance = ConsoleServer(host, port, redis_url)
    return _server_instance


def reset_server() -> None:
    """Reset the singleton server instance."""
    global _server_instance
    _server_instance = None
