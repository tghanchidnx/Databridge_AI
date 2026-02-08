"""
A wrapper script to interact with the Gemini CLI for context management.
Uses Gemini's large context window (1M+ tokens) to maintain project state.
"""
import sys
import subprocess
import os
from pathlib import Path
from datetime import datetime

# Configuration
GEMINI_CMD = r"C:\Users\telha\AppData\Roaming\npm\gemini.cmd"
PROJECT_ROOT = Path(__file__).parent
CONTEXT_FILE = PROJECT_ROOT / "CLAUDE.md"
SESSION_FILE = PROJECT_ROOT / "data" / "gemini_session.md"
TEMP_PROMPT_FILE = PROJECT_ROOT / "data" / "gemini_prompt.txt"

def ensure_data_dir():
    """Ensure data directory exists."""
    (PROJECT_ROOT / "data").mkdir(parents=True, exist_ok=True)

def ensure_session_file():
    """Ensure the session file exists."""
    ensure_data_dir()
    if not SESSION_FILE.exists():
        SESSION_FILE.write_text("# Gemini Session Context\n\nNo session data yet.\n", encoding='utf-8')

def update_context(summary: str):
    """Update the session file with new context."""
    ensure_session_file()
    current = SESSION_FILE.read_text(encoding='utf-8')
    updated = f"# Gemini Session Context\n\nLast Updated: {datetime.now().isoformat()}\n\n{summary}\n\n---\n\n{current}"
    SESSION_FILE.write_text(updated, encoding='utf-8')

def run_gemini_interactive(prompt: str):
    """Run gemini CLI with prompt via stdin for large prompts."""
    ensure_data_dir()

    # Write prompt to temp file
    TEMP_PROMPT_FILE.write_text(prompt, encoding='utf-8')

    # Use gemini with the prompt file content piped via -p
    # For very large prompts, we'll use a summarized version
    short_prompt = prompt[:8000] if len(prompt) > 8000 else prompt

    command = [GEMINI_CMD, "-p", short_prompt]

    try:
        result = subprocess.run(command, check=True, capture_output=False)
    finally:
        # Cleanup temp file
        if TEMP_PROMPT_FILE.exists():
            TEMP_PROMPT_FILE.unlink()

def get_project_summary() -> str:
    """Get a brief summary of the project for context."""
    return """DataBridge AI v0.34.0 - MCP-native data reconciliation engine with 292 tools.

Key modules: Hierarchy Builder (44 tools), Data Reconciliation (38 tools), Cortex AI (25 tools),
Wright Module/Mart Factory (18 tools), Data Catalog (15 tools), Versioning (12 tools),
Git/CI-CD (12 tools), Lineage (11 tools), dbt Integration (8 tools), Data Quality (7 tools).

Recent: Published v0.34.0 to PyPI, GitHub wiki set up, dark-theme UI in databridge-ce."""

def main():
    """
    Main entry point. Supports multiple modes:

    Usage:
        python ask_claude_gemini.py "Your question or task"
        python ask_claude_gemini.py --update "Context update to store"
        python ask_claude_gemini.py --status
        python ask_claude_gemini.py --sync   (sync current state to Gemini)
    """
    if len(sys.argv) < 2:
        print(f"Usage: python {sys.argv[0]} \"Your request to the AI...\"")
        print(f"       python {sys.argv[0]} --update \"Context to store\"")
        print(f"       python {sys.argv[0]} --status")
        print(f"       python {sys.argv[0]} --sync")
        sys.exit(1)

    # Check for special commands
    if sys.argv[1] == "--status":
        ensure_session_file()
        print(f"Session file: {SESSION_FILE}")
        print(f"Context file: {CONTEXT_FILE}")
        print(f"\nSession contents:\n{SESSION_FILE.read_text(encoding='utf-8')[:2000]}")
        sys.exit(0)

    if sys.argv[1] == "--update":
        if len(sys.argv) < 3:
            print("Error: --update requires a context string")
            sys.exit(1)
        context = " ".join(sys.argv[2:])
        update_context(context)
        print(f"Context updated in {SESSION_FILE}")
        sys.exit(0)

    if sys.argv[1] == "--sync":
        summary = get_project_summary()
        ensure_session_file()
        session = SESSION_FILE.read_text(encoding='utf-8')[:5000]

        sync_prompt = f"""Please remember this project context for our session:

{summary}

Session history:
{session}

Confirm you have this context loaded."""

        print("Syncing project context to Gemini...")
        run_gemini_interactive(sync_prompt)
        sys.exit(0)

    # Regular query mode
    user_request = " ".join(sys.argv[1:])

    # Build compact prompt with project context
    summary = get_project_summary()

    full_prompt = f"""Project: DataBridge AI v0.34.0 (292 MCP tools)
{summary}

Request: {user_request}

Please provide a specific, actionable response."""

    print("---")
    print("Querying Gemini...")
    print("---\n")

    try:
        run_gemini_interactive(full_prompt)
    except FileNotFoundError:
        print(f"\n[ERROR] Gemini CLI not found at: {GEMINI_CMD}")
        sys.exit(1)
    except subprocess.CalledProcessError as e:
        print(f"\n[ERROR] Gemini CLI error: {e.returncode}")
        sys.exit(1)

    print("\n---")
    print("Query complete.")
    print("---")

if __name__ == "__main__":
    main()
