"""
A wrapper script to interact with the Gemini CLI for context management.
Uses Gemini's large context window (1M+ tokens) to maintain project state.
Stores context in GEMINI.md for persistent reference.
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
GEMINI_CONTEXT_FILE = PROJECT_ROOT / "GEMINI.md"
SESSION_FILE = PROJECT_ROOT / "data" / "gemini_session.md"
TEMP_PROMPT_FILE = PROJECT_ROOT / "data" / "gemini_prompt.txt"

# Current version info
VERSION = "0.39.0"
TOOL_COUNT = 341

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
    return f"""DataBridge AI v{VERSION} - MCP-native data reconciliation engine with {TOOL_COUNT} tools.

Key modules: Hierarchy Builder (44 tools), Data Reconciliation (38 tools), Cortex AI (26 tools),
Wright Module (29 tools), Data Catalog (19 tools), Data Observability (15 tools), Versioning (12 tools),
Git/CI-CD (12 tools), Lineage (11 tools), GraphRAG (10 tools), dbt Integration (8 tools), Data Quality (7 tools).

Docs: CLAUDE.md (compact rules), GEMINI.md (detailed reference with examples/architectures)."""


def store_context_to_gemini(context: str, section: str = "Session Context"):
    """
    Store context to GEMINI.md file.
    Appends to or updates a specific section.
    """
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    if GEMINI_CONTEXT_FILE.exists():
        content = GEMINI_CONTEXT_FILE.read_text(encoding='utf-8')
    else:
        content = f"# GEMINI.md - DataBridge AI Context Store\n\n"

    # Create the new context block
    new_block = f"""
## {section}
_Updated: {timestamp}_

{context}

---
"""

    # Check if section already exists
    section_marker = f"## {section}"
    if section_marker in content:
        # Find and replace the section
        import re
        pattern = rf"## {re.escape(section)}.*?(?=\n## |\Z)"
        content = re.sub(pattern, new_block.strip() + "\n\n", content, flags=re.DOTALL)
    else:
        # Append to end
        content = content.rstrip() + "\n\n" + new_block

    GEMINI_CONTEXT_FILE.write_text(content, encoding='utf-8')
    return GEMINI_CONTEXT_FILE

def get_gemini_context() -> str:
    """Load the detailed GEMINI.md context file."""
    if GEMINI_CONTEXT_FILE.exists():
        content = GEMINI_CONTEXT_FILE.read_text(encoding='utf-8')
        # Truncate if too large (keep under 50k chars for reasonable prompt size)
        if len(content) > 50000:
            content = content[:50000] + "\n\n[... truncated for context limit ...]"
        return content
    return ""

def main():
    """
    Main entry point. Supports multiple modes:

    Usage:
        python ask_claude_gemini.py "Your question or task"
        python ask_claude_gemini.py --update "Context update to store"
        python ask_claude_gemini.py --store "Context" --section "Section Name"
        python ask_claude_gemini.py --status
        python ask_claude_gemini.py --sync   (sync GEMINI.md to Gemini's context)
        python ask_claude_gemini.py --load   (load full GEMINI.md without query)
    """
    if len(sys.argv) < 2:
        print(f"Usage: python {sys.argv[0]} \"Your request to the AI...\"")
        print(f"       python {sys.argv[0]} --update \"Context to store\"")
        print(f"       python {sys.argv[0]} --store \"Context\" [--section \"Name\"]")
        print(f"       python {sys.argv[0]} --status")
        print(f"       python {sys.argv[0]} --sync   (sync full context)")
        print(f"       python {sys.argv[0]} --load   (load GEMINI.md)")
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

    if sys.argv[1] == "--store":
        if len(sys.argv) < 3:
            print("Error: --store requires a context string")
            sys.exit(1)
        # Parse arguments
        section = "Session Context"
        context_parts = []
        i = 2
        while i < len(sys.argv):
            if sys.argv[i] == "--section" and i + 1 < len(sys.argv):
                section = sys.argv[i + 1]
                i += 2
            else:
                context_parts.append(sys.argv[i])
                i += 1
        context = " ".join(context_parts)
        filepath = store_context_to_gemini(context, section)
        print(f"Context stored in {filepath} under section '{section}'")
        sys.exit(0)

    if sys.argv[1] == "--sync":
        summary = get_project_summary()
        gemini_context = get_gemini_context()
        ensure_session_file()
        session = SESSION_FILE.read_text(encoding='utf-8')[:2000]

        # Build sync prompt with detailed context
        sync_prompt = f"""Please store this DataBridge AI project context in your memory for our session:

## Project Summary
{summary}

## Detailed Reference (from GEMINI.md)
{gemini_context[:30000]}

## Session History
{session}

Confirm you have loaded this context. You can now help Claude with detailed examples and architectures."""

        print("Syncing full project context to Gemini (GEMINI.md)...")
        run_gemini_interactive(sync_prompt)
        update_context("Full GEMINI.md context synced to Gemini for detailed reference.")
        sys.exit(0)

    if sys.argv[1] == "--load":
        gemini_context = get_gemini_context()
        if not gemini_context:
            print("Error: GEMINI.md not found")
            sys.exit(1)

        load_prompt = f"""Load this DataBridge AI detailed reference into your context memory:

{gemini_context}

Confirm loaded. Ready to answer detailed questions about DataBridge AI tools, examples, and architectures."""

        print(f"Loading GEMINI.md ({len(gemini_context)} chars) into Gemini...")
        run_gemini_interactive(load_prompt)
        update_context("GEMINI.md loaded into Gemini context memory.")
        sys.exit(0)

    # Regular query mode
    user_request = " ".join(sys.argv[1:])

    # Build compact prompt with project context
    summary = get_project_summary()

    full_prompt = f"""Project: DataBridge AI v{VERSION} ({TOOL_COUNT} MCP tools)
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
