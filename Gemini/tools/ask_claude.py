#!/usr/bin/env python
import subprocess
import sys
import argparse

def ask_claude(prompt: str) -> str:
    """
    Executes the Claude CLI with a given prompt and returns its output.

    This script assumes that the 'claude' command is available in the system's PATH.
    It passes the provided prompt to `claude -p` and captures the standard output.
    """
    try:
        # We use a list of arguments for better security and handling of spaces.
        command = ["claude", "-p", prompt]

        # Execute the command. We set text=True to get stdout/stderr as strings.
        # We also set a timeout to prevent the script from hanging indefinitely.
        result = subprocess.run(
            command,
            capture_output=True,
            text=True,
            check=True,  # This will raise a CalledProcessError if claude returns a non-zero exit code.
            timeout=300  # 5-minute timeout
        )
        return result.stdout.strip()
    except FileNotFoundError:
        return "Error: The 'claude' command was not found. Please ensure Claude CLI is installed and in your system's PATH."
    except subprocess.CalledProcessError as e:
        # This error is raised if the claude command returns an error.
        error_message = f"Error executing Claude CLI. Return code: {e.returncode}\n"
        error_message += f"Stderr: {e.stderr.strip()}"
        return error_message
    except subprocess.TimeoutExpired:
        return "Error: The request to Claude CLI timed out after 5 minutes."
    except Exception as e:
        return f"An unexpected error occurred: {str(e)}"

def main():
    """
    Main function to run the script from the command line for testing.
    This allows you to test the script directly, e.g., `python ask_claude.py "Hello, Claude!"`
    """
    parser = argparse.ArgumentParser(description="A tool to interact with the Claude CLI.")
    parser.add_argument("prompt", help="The prompt to send to Claude.")
    args = parser.parse_args()

    response = ask_claude(args.prompt)
    print(response)

if __name__ == "__main__":
    main()
