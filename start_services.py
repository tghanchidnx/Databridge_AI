#!/usr/bin/env python3
"""
DataBridge AI - Service Manager
================================
Start, stop, and monitor all DataBridge AI services.

Usage:
    python start_services.py              # Start all services
    python start_services.py --status     # Check service status
    python start_services.py --stop       # Stop all services
    python start_services.py --logs       # View service logs
    python start_services.py --mcp        # Start MCP server only
"""

import subprocess
import sys
import time
import argparse
import os
import signal
from pathlib import Path
from typing import Optional

# Configuration
PROJECT_ROOT = Path(__file__).parent.resolve()
V2_DIR = PROJECT_ROOT / "v2"
DOCKER_DESKTOP_PATH = Path("C:/Program Files/Docker/Docker/Docker Desktop.exe")

# Service ports
SERVICES = {
    "MySQL": {"port": 3308, "container": "databridge-mysql-v2"},
    "Backend API": {"port": 3002, "container": "databridge-backend-v2"},
    "Frontend": {"port": 8080, "container": "databridge-frontend-v2"},
    "Redis": {"port": 6381, "container": "databridge-redis-v2"},
}


class Colors:
    """ANSI color codes for terminal output."""
    GREEN = "\033[92m"
    RED = "\033[91m"
    YELLOW = "\033[93m"
    BLUE = "\033[94m"
    CYAN = "\033[96m"
    BOLD = "\033[1m"
    END = "\033[0m"


def print_banner():
    """Print the application banner."""
    banner = f"""
{Colors.CYAN}{Colors.BOLD}
+---------------------------------------------------------------+
|                    DataBridge AI V2                           |
|                   Service Manager                             |
+---------------------------------------------------------------+
{Colors.END}"""
    print(banner)


def run_command(cmd: list, capture: bool = True, timeout: int = 60) -> tuple[int, str, str]:
    """Run a shell command and return exit code, stdout, stderr."""
    try:
        result = subprocess.run(
            cmd,
            capture_output=capture,
            text=True,
            timeout=timeout,
            shell=True if isinstance(cmd, str) else False
        )
        return result.returncode, result.stdout or "", result.stderr or ""
    except subprocess.TimeoutExpired:
        return -1, "", "Command timed out"
    except Exception as e:
        return -1, "", str(e)


def check_docker_running() -> bool:
    """Check if Docker daemon is running."""
    code, _, _ = run_command(["docker", "info"], timeout=10)
    return code == 0


def start_docker_desktop():
    """Start Docker Desktop on Windows."""
    print(f"{Colors.YELLOW}Starting Docker Desktop...{Colors.END}")

    if DOCKER_DESKTOP_PATH.exists():
        subprocess.Popen([str(DOCKER_DESKTOP_PATH)], shell=True)

        # Wait for Docker to be ready
        max_wait = 60
        waited = 0
        while waited < max_wait:
            if check_docker_running():
                print(f"{Colors.GREEN}Docker Desktop is ready!{Colors.END}")
                return True
            print(f"  Waiting for Docker... ({waited}s)", end="\r")
            time.sleep(5)
            waited += 5

        print(f"\n{Colors.RED}Docker failed to start within {max_wait}s{Colors.END}")
        return False
    else:
        print(f"{Colors.RED}Docker Desktop not found at {DOCKER_DESKTOP_PATH}{Colors.END}")
        print("Please install Docker Desktop or start it manually.")
        return False


def get_container_status(container_name: str) -> tuple[bool, str]:
    """Get the status of a Docker container."""
    # First check if container is running
    code, stdout, _ = run_command(
        ["docker", "inspect", "-f", "{{.State.Status}}", container_name],
        timeout=10
    )
    if code != 0:
        return False, "not running"

    status = stdout.strip()
    if status != "running":
        return False, status

    # Check for health status (may not exist)
    code, health_out, _ = run_command(
        ["docker", "inspect", "-f", "{{if .State.Health}}{{.State.Health.Status}}{{else}}none{{end}}", container_name],
        timeout=10
    )
    health = health_out.strip() if code == 0 else "none"

    if health and health != "none":
        return True, f"running ({health})"
    return True, "running"


def start_docker_services():
    """Start all Docker services using docker-compose."""
    print(f"\n{Colors.BLUE}Starting Docker services...{Colors.END}")

    os.chdir(V2_DIR)

    # Run docker-compose up
    process = subprocess.Popen(
        ["docker-compose", "up", "-d"],
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True
    )

    # Stream output
    for line in process.stdout:
        line = line.strip()
        if line:
            # Color code the output
            if "Started" in line or "Running" in line or "Healthy" in line:
                print(f"  {Colors.GREEN}{line}{Colors.END}")
            elif "Creating" in line or "Building" in line:
                print(f"  {Colors.YELLOW}{line}{Colors.END}")
            elif "Error" in line or "error" in line:
                print(f"  {Colors.RED}{line}{Colors.END}")
            else:
                print(f"  {line}")

    process.wait()
    os.chdir(PROJECT_ROOT)

    return process.returncode == 0


def stop_docker_services():
    """Stop all Docker services."""
    print(f"\n{Colors.YELLOW}Stopping Docker services...{Colors.END}")

    os.chdir(V2_DIR)
    code, stdout, stderr = run_command(["docker-compose", "down"], timeout=120)
    os.chdir(PROJECT_ROOT)

    if code == 0:
        print(f"{Colors.GREEN}All services stopped.{Colors.END}")
    else:
        print(f"{Colors.RED}Error stopping services: {stderr}{Colors.END}")

    return code == 0


def check_service_health():
    """Check health of all services."""
    print(f"\n{Colors.BLUE}Service Status:{Colors.END}")
    print("-" * 50)

    all_healthy = True

    for service_name, info in SERVICES.items():
        running, status = get_container_status(info["container"])

        if running and "healthy" in status.lower():
            icon = f"{Colors.GREEN}[OK]{Colors.END}"
            status_text = f"{Colors.GREEN}{status}{Colors.END}"
        elif running and "running" in status.lower():
            icon = f"{Colors.YELLOW}[..]{Colors.END}"
            status_text = f"{Colors.YELLOW}{status}{Colors.END}"
            all_healthy = False
        else:
            icon = f"{Colors.RED}[X]{Colors.END}"
            status_text = f"{Colors.RED}{status}{Colors.END}"
            all_healthy = False

        print(f"  {icon} {service_name:15} Port {info['port']:5}  {status_text}")

    print("-" * 50)
    return all_healthy


def wait_for_services(timeout: int = 120):
    """Wait for all services to be healthy."""
    print(f"\n{Colors.YELLOW}Waiting for services to be ready...{Colors.END}")

    start_time = time.time()
    while time.time() - start_time < timeout:
        all_ready = True
        for service_name, info in SERVICES.items():
            running, status = get_container_status(info["container"])
            if not running or ("healthy" not in status.lower() and "running" not in status.lower()):
                all_ready = False
                break

        if all_ready:
            print(f"\n{Colors.GREEN}All services are ready!{Colors.END}")
            return True

        elapsed = int(time.time() - start_time)
        print(f"  Waiting... ({elapsed}s / {timeout}s)", end="\r")
        time.sleep(5)

    print(f"\n{Colors.YELLOW}Timeout waiting for services.{Colors.END}")
    return False


def test_endpoints():
    """Test that service endpoints are responding."""
    print(f"\n{Colors.BLUE}Testing Endpoints:{Colors.END}")
    print("-" * 50)

    import urllib.request
    import urllib.error

    endpoints = [
        ("Backend Health", "http://localhost:3002/api/health"),
        ("Frontend", "http://localhost:8080/health"),
    ]

    for name, url in endpoints:
        try:
            req = urllib.request.urlopen(url, timeout=5)
            status = req.getcode()
            if status == 200:
                print(f"  {Colors.GREEN}[OK]{Colors.END} {name:20} {Colors.GREEN}OK{Colors.END} ({url})")
            else:
                print(f"  {Colors.YELLOW}[..]{Colors.END} {name:20} {Colors.YELLOW}Status {status}{Colors.END}")
        except urllib.error.URLError as e:
            print(f"  {Colors.RED}[X]{Colors.END} {name:20} {Colors.RED}Failed{Colors.END} ({e.reason})")
        except Exception as e:
            print(f"  {Colors.RED}[X]{Colors.END} {name:20} {Colors.RED}Error{Colors.END} ({e})")

    print("-" * 50)


def view_logs(service: Optional[str] = None, follow: bool = False, tail: int = 50):
    """View Docker service logs."""
    os.chdir(V2_DIR)

    cmd = ["docker-compose", "logs"]
    if tail:
        cmd.extend(["--tail", str(tail)])
    if follow:
        cmd.append("-f")
    if service:
        cmd.append(service)

    try:
        subprocess.run(cmd)
    except KeyboardInterrupt:
        pass

    os.chdir(PROJECT_ROOT)


def start_mcp_server():
    """Start the Python MCP server."""
    print(f"\n{Colors.BLUE}Starting MCP Server...{Colors.END}")
    print(f"Press Ctrl+C to stop\n")

    os.chdir(PROJECT_ROOT)

    try:
        # Check if fastmcp is available
        code, _, _ = run_command(["python", "-c", "import fastmcp"], timeout=10)

        if code == 0:
            subprocess.run(["python", "-m", "fastmcp", "dev", "src/server.py"])
        else:
            # Try running the server directly
            if (PROJECT_ROOT / "run_server.py").exists():
                subprocess.run(["python", "run_server.py"])
            elif (PROJECT_ROOT / "server.py").exists():
                subprocess.run(["python", "server.py"])
            else:
                print(f"{Colors.RED}MCP server entry point not found.{Colors.END}")
                print("Install dependencies with: pip install -r requirements.txt")
    except KeyboardInterrupt:
        print(f"\n{Colors.YELLOW}MCP Server stopped.{Colors.END}")


def print_access_info():
    """Print access URLs and information."""
    info = f"""
{Colors.CYAN}{Colors.BOLD}Access Information:{Colors.END}
{Colors.CYAN}{'-' * 50}{Colors.END}
  Frontend:      http://localhost:8080
  Frontend SSL:  https://localhost:8443
  Backend API:   http://localhost:3002/api
  API Health:    http://localhost:3002/api/health

  MySQL:         localhost:3308 (user: databridge)
  Redis:         localhost:6381
{Colors.CYAN}{'-' * 50}{Colors.END}
"""
    print(info)


def main():
    parser = argparse.ArgumentParser(
        description="DataBridge AI Service Manager",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python start_services.py              Start all services
  python start_services.py --status     Check service status
  python start_services.py --stop       Stop all services
  python start_services.py --logs       View all logs
  python start_services.py --logs -f    Follow logs in real-time
  python start_services.py --mcp        Start MCP server only
        """
    )

    parser.add_argument("--status", "-s", action="store_true",
                        help="Check status of all services")
    parser.add_argument("--stop", action="store_true",
                        help="Stop all services")
    parser.add_argument("--logs", "-l", action="store_true",
                        help="View service logs")
    parser.add_argument("--follow", "-f", action="store_true",
                        help="Follow logs in real-time (use with --logs)")
    parser.add_argument("--tail", "-t", type=int, default=50,
                        help="Number of log lines to show (default: 50)")
    parser.add_argument("--service", type=str,
                        help="Specific service for logs (backend-v2, frontend-v2, mysql-v2, redis-v2)")
    parser.add_argument("--mcp", action="store_true",
                        help="Start MCP server only (no Docker services)")
    parser.add_argument("--no-wait", action="store_true",
                        help="Don't wait for services to be healthy")

    args = parser.parse_args()

    print_banner()

    # Handle MCP-only mode
    if args.mcp:
        start_mcp_server()
        return 0

    # Handle logs
    if args.logs:
        if not check_docker_running():
            print(f"{Colors.RED}Docker is not running.{Colors.END}")
            return 1
        view_logs(service=args.service, follow=args.follow, tail=args.tail)
        return 0

    # Handle status check
    if args.status:
        if not check_docker_running():
            print(f"{Colors.RED}Docker is not running.{Colors.END}")
            return 1
        check_service_health()
        test_endpoints()
        print_access_info()
        return 0

    # Handle stop
    if args.stop:
        if not check_docker_running():
            print(f"{Colors.YELLOW}Docker is not running.{Colors.END}")
            return 0
        stop_docker_services()
        return 0

    # Default: Start all services
    print(f"{Colors.BOLD}Starting DataBridge AI services...{Colors.END}")

    # Check/start Docker
    if not check_docker_running():
        print(f"{Colors.YELLOW}Docker is not running.{Colors.END}")
        if not start_docker_desktop():
            return 1
    else:
        print(f"{Colors.GREEN}Docker is running.{Colors.END}")

    # Start Docker services
    if not start_docker_services():
        print(f"{Colors.RED}Failed to start Docker services.{Colors.END}")
        return 1

    # Wait for services
    if not args.no_wait:
        wait_for_services(timeout=120)

    # Show status
    check_service_health()
    test_endpoints()
    print_access_info()

    print(f"{Colors.GREEN}{Colors.BOLD}All services started successfully!{Colors.END}")
    print(f"\nRun '{Colors.CYAN}python start_services.py --mcp{Colors.END}' to also start the MCP server.")
    print(f"Run '{Colors.CYAN}python start_services.py --stop{Colors.END}' to stop all services.")

    return 0


if __name__ == "__main__":
    try:
        sys.exit(main())
    except KeyboardInterrupt:
        print(f"\n{Colors.YELLOW}Interrupted.{Colors.END}")
        sys.exit(130)
