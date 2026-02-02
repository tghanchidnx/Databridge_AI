"""
Test client for the DataBridge AI MCP server.
"""
import asyncio
import json
import websockets
import uuid

async def run_test():
    """Connects to the MCP server and runs a series of tool calls."""
    uri = "ws://localhost:8000/mcp"  # Default port for the librarian
    session_id = f"session_{uuid.uuid4()}"

    try:
        async with websockets.connect(uri) as websocket:
            print("--- Connected to MCP Server ---")

            # 1. Call `load_csv`
            print("\n[Step 1] Calling `load_csv` tool...")
            await websocket.send(json.dumps({
                "jsonrpc": "2.0",
                "method": "run_tool",
                "params": {
                    "tool_name": "load_csv",
                    "parameters": {
                        "file_path": "samples/chart_of_accounts.csv",
                        "session_id": session_id,
                    }
                },
                "id": 1
            }))
            response = json.loads(await websocket.recv())
            
            if "error" in response:
                print("\n--- TEST FAILED (Step 1) ---")
                print(response["error"]["message"])
                return
            
            load_result = response["result"]
            step_id = load_result.get("step_id")
            print("\n`load_csv` successful!")
            print(f"  - Session ID: {session_id}")
            print(f"  - Step ID: {step_id}")

            # 2. Call `get_data_snapshot_url`
            print("\n[Step 2] Calling `get_data_snapshot_url` tool...")
            await websocket.send(json.dumps({
                "jsonrpc": "2.0",
                "method": "run_tool",
                "params": {
                    "tool_name": "get_data_snapshot_url",
                    "parameters": { "session_id": session_id, "step_id": step_id }
                },
                "id": 2
            }))
            response = json.loads(await websocket.recv())

            if "error" in response:
                print("\n--- TEST FAILED (Step 2) ---")
                print(response["error"]["message"])
                return

            url_result = response["result"]
            print("\n`get_data_snapshot_url` successful!")
            print(f"  - Access URL: {url_result.get('access_url')}")

            # 3. Call `export_workflow_diagram`
            output_html_path = "workflow_diagram.html"
            print("\n[Step 3] Calling `export_workflow_diagram` tool...")
            await websocket.send(json.dumps({
                "jsonrpc": "2.0",
                "method": "run_tool",
                "params": {
                    "tool_name": "export_workflow_diagram",
                    "parameters": { "session_id": session_id, "output_path": output_html_path }
                },
                "id": 3
            }))
            response = json.loads(await websocket.recv())

            if "error" in response:
                print("\n--- TEST FAILED (Step 3) ---")
                print(response["error"]["message"])
                return

            diagram_result = response["result"]
            print("\n`export_workflow_diagram` successful!")
            print(f"  - Diagram saved to: {diagram_result.get('output_path')}")

            print("\n--- TEST COMPLETED SUCCESSFULLY ---")

    except ConnectionRefusedError:
        print("\n--- TEST FAILED ---")
        print("Connection refused. Is the MCP server running? (run `python run_server.py`)")
    except Exception as e:
        print(f"\n--- An unexpected error occurred ---")
        print(e)


if __name__ == "__main__":
    asyncio.run(run_test())
