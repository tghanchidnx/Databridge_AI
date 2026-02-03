#!/usr/bin/env python3
"""
DataBridge AI - Service Manager
================================
Start, stop, and monitor all DataBridge AI services.

Services:
  - Docker: MySQL, Redis, NestJS Backend (v2 containers)
  - MCP Server: Python FastMCP server (137 tools)
  - Dashboard: DataBridge Dashboard UI (Multi AI, MCP CLI)
  - Excel Plugin: Development server for Office Add-in

Usage:
    python start_services.py              # Start Docker services
    python start_services.py --all        # Start everything (Docker + MCP + Dashboard)
    python start_services.py --status     # Check service status
    python start_services.py --stop       # Stop all services
    python start_services.py --logs       # View Docker logs
    python start_services.py --mcp        # Start MCP server only
    python start_services.py --dashboard  # Start Dashboard server only
    python start_services.py --excel      # Start Excel plugin dev server
"""

import subprocess
import sys
import time
import argparse
import os
import signal
import threading
import socket
from pathlib import Path
from typing import Optional, List

# Configuration
PROJECT_ROOT = Path(__file__).parent.resolve()
V2_DIR = PROJECT_ROOT / "v2"
APPS_DIR = PROJECT_ROOT / "apps"
DASHBOARD_DIR = APPS_DIR / "databridge-dashboard"
EXCEL_PLUGIN_DIR = APPS_DIR / "excel-plugin"
DOCKER_DESKTOP_PATH = Path("C:/Program Files/Docker/Docker/Docker Desktop.exe")

# Service definitions
DOCKER_SERVICES = {
    "MySQL": {"port": 3308, "container": "databridge-mysql-v2", "required": True},
    "Redis": {"port": 6381, "container": "databridge-redis-v2", "required": True},
    "Backend API": {"port": 3002, "container": "databridge-backend-v2", "required": True},
}

OTHER_SERVICES = {
    "MCP Server": {"port": None, "type": "mcp", "script": "run_server.py"},
    "Dashboard": {"port": 5180, "type": "dashboard", "dir": DASHBOARD_DIR},
    "Excel Plugin": {"port": 3000, "type": "excel", "dir": EXCEL_PLUGIN_DIR},
}

# Process tracking
running_processes: List[subprocess.Popen] = []


class Colors:
    """ANSI color codes for terminal output."""
    GREEN = "\033[92m"
    RED = "\033[91m"
    YELLOW = "\033[93m"
    BLUE = "\033[94m"
    CYAN = "\033[96m"
    MAGENTA = "\033[95m"
    BOLD = "\033[1m"
    END = "\033[0m"


def print_banner():
    """Print the application banner."""
    banner = f"""
{Colors.CYAN}{Colors.BOLD}
+---------------------------------------------------------------+
|                     DataBridge AI                             |
|                   Service Manager                             |
|                                                               |
|  MCP Server: 137 Tools | Backend: NestJS | DB: MySQL/Redis   |
+---------------------------------------------------------------+
{Colors.END}"""
    print(banner)


def run_command(cmd: list, capture: bool = True, timeout: int = 60) -> tuple:
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


def is_port_in_use(port: int) -> bool:
    """Check if a port is in use."""
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        return s.connect_ex(('localhost', port)) == 0


def check_docker_running() -> bool:
    """Check if Docker daemon is running."""
    code, _, _ = run_command(["docker", "info"], timeout=10)
    return code == 0


def start_docker_desktop():
    """Start Docker Desktop on Windows."""
    print(f"{Colors.YELLOW}Starting Docker Desktop...{Colors.END}")

    if DOCKER_DESKTOP_PATH.exists():
        subprocess.Popen([str(DOCKER_DESKTOP_PATH)], shell=True)

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
        return False


def get_container_status(container_name: str) -> tuple:
    """Get the status of a Docker container."""
    code, stdout, _ = run_command(
        ["docker", "inspect", "-f", "{{.State.Status}}", container_name],
        timeout=10
    )
    if code != 0:
        return False, "not found"

    status = stdout.strip()
    if status != "running":
        return False, status

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

    if not V2_DIR.exists():
        print(f"{Colors.RED}v2 directory not found at {V2_DIR}{Colors.END}")
        return False

    original_dir = os.getcwd()
    os.chdir(V2_DIR)

    process = subprocess.Popen(
        ["docker-compose", "up", "-d"],
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True
    )

    for line in process.stdout:
        line = line.strip()
        if line:
            if "Started" in line or "Running" in line or "Healthy" in line:
                print(f"  {Colors.GREEN}{line}{Colors.END}")
            elif "Creating" in line or "Building" in line:
                print(f"  {Colors.YELLOW}{line}{Colors.END}")
            elif "Error" in line or "error" in line:
                print(f"  {Colors.RED}{line}{Colors.END}")
            else:
                print(f"  {line}")

    process.wait()
    os.chdir(original_dir)

    return process.returncode == 0


def stop_docker_services():
    """Stop all Docker services."""
    print(f"\n{Colors.YELLOW}Stopping Docker services...{Colors.END}")

    if not V2_DIR.exists():
        print(f"{Colors.YELLOW}v2 directory not found, skipping.{Colors.END}")
        return True

    original_dir = os.getcwd()
    os.chdir(V2_DIR)
    code, stdout, stderr = run_command(["docker-compose", "down"], timeout=120)
    os.chdir(original_dir)

    if code == 0:
        print(f"{Colors.GREEN}Docker services stopped.{Colors.END}")
    else:
        print(f"{Colors.RED}Error stopping services: {stderr}{Colors.END}")

    return code == 0


def check_all_services_status():
    """Check health of all services."""
    print(f"\n{Colors.BLUE}Service Status:{Colors.END}")
    print("=" * 60)

    # Docker services
    print(f"\n{Colors.CYAN}Docker Services:{Colors.END}")
    print("-" * 60)

    docker_running = check_docker_running()
    if not docker_running:
        print(f"  {Colors.RED}[X] Docker is not running{Colors.END}")
    else:
        for service_name, info in DOCKER_SERVICES.items():
            running, status = get_container_status(info["container"])

            if running and "healthy" in status.lower():
                icon = f"{Colors.GREEN}[OK]{Colors.END}"
                status_text = f"{Colors.GREEN}{status}{Colors.END}"
            elif running:
                icon = f"{Colors.YELLOW}[..]{Colors.END}"
                status_text = f"{Colors.YELLOW}{status}{Colors.END}"
            else:
                icon = f"{Colors.RED}[X]{Colors.END}"
                status_text = f"{Colors.RED}{status}{Colors.END}"

            print(f"  {icon} {service_name:15} Port {info['port']:5}  {status_text}")

    # Other services
    print(f"\n{Colors.CYAN}Application Services:{Colors.END}")
    print("-" * 60)

    # MCP Server (check if fastmcp process is running)
    mcp_running = False
    code, stdout, _ = run_command(["tasklist", "/FI", "IMAGENAME eq python.exe"], timeout=5)
    if "python" in stdout.lower():
        # Could be MCP server, just mark as potentially running
        mcp_running = True
    icon = f"{Colors.YELLOW}[?]{Colors.END}" if mcp_running else f"{Colors.RED}[X]{Colors.END}"
    print(f"  {icon} {'MCP Server':15} {'stdio':5}  {'Check manually (run --mcp)' if not mcp_running else 'Python process found'}")

    # Dashboard
    dashboard_running = is_port_in_use(5180)
    icon = f"{Colors.GREEN}[OK]{Colors.END}" if dashboard_running else f"{Colors.RED}[X]{Colors.END}"
    status = "running" if dashboard_running else "not running"
    print(f"  {icon} {'Dashboard':15} Port {5180:5}  {status}")

    # Excel Plugin
    excel_running = is_port_in_use(3000)
    icon = f"{Colors.GREEN}[OK]{Colors.END}" if excel_running else f"{Colors.RED}[X]{Colors.END}"
    status = "running" if excel_running else "not running"
    print(f"  {icon} {'Excel Plugin':15} Port {3000:5}  {status}")

    print("=" * 60)


def wait_for_docker_services(timeout: int = 120):
    """Wait for Docker services to be healthy."""
    print(f"\n{Colors.YELLOW}Waiting for Docker services...{Colors.END}")

    start_time = time.time()
    while time.time() - start_time < timeout:
        all_ready = True
        for service_name, info in DOCKER_SERVICES.items():
            running, status = get_container_status(info["container"])
            if not running:
                all_ready = False
                break

        if all_ready:
            print(f"\n{Colors.GREEN}All Docker services are ready!{Colors.END}")
            return True

        elapsed = int(time.time() - start_time)
        print(f"  Waiting... ({elapsed}s / {timeout}s)", end="\r")
        time.sleep(5)

    print(f"\n{Colors.YELLOW}Timeout waiting for services.{Colors.END}")
    return False


def test_backend_endpoint():
    """Test backend API endpoint."""
    import urllib.request
    import urllib.error

    print(f"\n{Colors.BLUE}Testing Backend API:{Colors.END}")

    try:
        req = urllib.request.urlopen("http://localhost:3002/api/health", timeout=5)
        if req.getcode() == 200:
            print(f"  {Colors.GREEN}[OK] Backend API responding{Colors.END}")
            return True
    except Exception as e:
        print(f"  {Colors.RED}[X] Backend API not responding: {e}{Colors.END}")

    return False


def start_mcp_server(background: bool = False):
    """Start the Python MCP server."""
    print(f"\n{Colors.BLUE}Starting MCP Server...{Colors.END}")

    mcp_script = PROJECT_ROOT / "run_server.py"
    if not mcp_script.exists():
        print(f"{Colors.RED}MCP server script not found: {mcp_script}{Colors.END}")
        return None

    if background:
        print(f"  Running in background mode...")
        process = subprocess.Popen(
            ["python", str(mcp_script)],
            cwd=str(PROJECT_ROOT),
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
        )
        running_processes.append(process)
        print(f"  {Colors.GREEN}MCP Server started (PID: {process.pid}){Colors.END}")
        return process
    else:
        print(f"  Press Ctrl+C to stop\n")
        try:
            # Try fastmcp dev mode first
            code, _, _ = run_command(["python", "-c", "import fastmcp"], timeout=5)
            if code == 0:
                subprocess.run(
                    ["python", "-m", "fastmcp", "dev", "src/server.py"],
                    cwd=str(PROJECT_ROOT)
                )
            else:
                subprocess.run(["python", str(mcp_script)], cwd=str(PROJECT_ROOT))
        except KeyboardInterrupt:
            print(f"\n{Colors.YELLOW}MCP Server stopped.{Colors.END}")
        return None


def start_dashboard_server(background: bool = False):
    """Start the Dashboard server."""
    print(f"\n{Colors.BLUE}Starting Dashboard Server...{Colors.END}")

    if not DASHBOARD_DIR.exists():
        print(f"{Colors.RED}Dashboard directory not found: {DASHBOARD_DIR}{Colors.END}")
        return None

    port = 5180

    if is_port_in_use(port):
        print(f"  {Colors.YELLOW}Port {port} already in use{Colors.END}")
        return None

    if background:
        process = subprocess.Popen(
            ["python", "-m", "http.server", str(port)],
            cwd=str(DASHBOARD_DIR),
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
        )
        running_processes.append(process)
        print(f"  {Colors.GREEN}Dashboard started at http://localhost:{port} (PID: {process.pid}){Colors.END}")
        return process
    else:
        print(f"  Serving at http://localhost:{port}")
        print(f"  Press Ctrl+C to stop\n")
        try:
            subprocess.run(
                ["python", "-m", "http.server", str(port)],
                cwd=str(DASHBOARD_DIR)
            )
        except KeyboardInterrupt:
            print(f"\n{Colors.YELLOW}Dashboard stopped.{Colors.END}")
        return None


def start_excel_plugin_server(background: bool = False):
    """Start the Excel plugin development server."""
    print(f"\n{Colors.BLUE}Starting Excel Plugin Dev Server...{Colors.END}")

    if not EXCEL_PLUGIN_DIR.exists():
        print(f"{Colors.RED}Excel plugin directory not found: {EXCEL_PLUGIN_DIR}{Colors.END}")
        return None

    # Check for node_modules
    if not (EXCEL_PLUGIN_DIR / "node_modules").exists():
        print(f"  {Colors.YELLOW}Installing dependencies...{Colors.END}")
        subprocess.run(["npm", "install"], cwd=str(EXCEL_PLUGIN_DIR), shell=True)

    port = 3000

    if is_port_in_use(port):
        print(f"  {Colors.YELLOW}Port {port} already in use{Colors.END}")
        return None

    if background:
        process = subprocess.Popen(
            ["npx", "webpack", "serve", "--mode", "development", "--port", str(port)],
            cwd=str(EXCEL_PLUGIN_DIR),
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            shell=True,
        )
        running_processes.append(process)
        print(f"  {Colors.GREEN}Excel Plugin started at https://localhost:{port} (PID: {process.pid}){Colors.END}")
        return process
    else:
        print(f"  Serving at https://localhost:{port}")
        print(f"  Press Ctrl+C to stop\n")
        try:
            subprocess.run(
                ["npx", "webpack", "serve", "--mode", "development", "--port", str(port)],
                cwd=str(EXCEL_PLUGIN_DIR),
                shell=True
            )
        except KeyboardInterrupt:
            print(f"\n{Colors.YELLOW}Excel Plugin server stopped.{Colors.END}")
        return None


def view_logs(service: Optional[str] = None, follow: bool = False, tail: int = 50):
    """View Docker service logs."""
    if not V2_DIR.exists():
        print(f"{Colors.RED}v2 directory not found{Colors.END}")
        return

    original_dir = os.getcwd()
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

    os.chdir(original_dir)


def stop_all_services():
    """Stop all running services."""
    # Stop tracked processes
    for proc in running_processes:
        try:
            proc.terminate()
            proc.wait(timeout=5)
        except:
            proc.kill()

    running_processes.clear()

    # Stop Docker
    if check_docker_running():
        stop_docker_services()


def print_access_info():
    """Print access URLs and information."""
    info = f"""
{Colors.CYAN}{Colors.BOLD}Access Information:{Colors.END}
{Colors.CYAN}{'=' * 60}{Colors.END}

  {Colors.BOLD}Backend API:{Colors.END}
    URL:        http://localhost:3002/api
    Health:     http://localhost:3002/api/health
    API Keys:   v2-dev-key-1, v2-dev-key-2

  {Colors.BOLD}Dashboard:{Colors.END}
    URL:        http://localhost:5180
    Multi AI:   http://localhost:5180 (Multi AI tab)

  {Colors.BOLD}Excel Plugin:{Colors.END}
    Dev Server: https://localhost:3000
    Manifest:   apps/excel-plugin/manifest.xml

  {Colors.BOLD}Database:{Colors.END}
    MySQL:      localhost:3308 (user: databridge)
    Redis:      localhost:6381

  {Colors.BOLD}MCP Server:{Colors.END}
    Tools:      137 tools available
    Config:     .mcp.json (for Claude Desktop/Code)
    Run:        python run_server.py

{Colors.CYAN}{'=' * 60}{Colors.END}
"""
    print(info)


def main():
    parser = argparse.ArgumentParser(
        description="DataBridge AI Service Manager",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python start_services.py              Start Docker services only
  python start_services.py --all        Start Docker + Dashboard
  python start_services.py --status     Check all service status
  python start_services.py --stop       Stop all services
  python start_services.py --mcp        Start MCP server (interactive)
  python start_services.py --dashboard  Start Dashboard server
  python start_services.py --excel      Start Excel plugin dev server
  python start_services.py --logs -f    Follow Docker logs
        """
    )

    parser.add_argument("--all", "-a", action="store_true",
                        help="Start all services (Docker + Dashboard)")
    parser.add_argument("--status", "-s", action="store_true",
                        help="Check status of all services")
    parser.add_argument("--stop", action="store_true",
                        help="Stop all services")
    parser.add_argument("--logs", "-l", action="store_true",
                        help="View Docker logs")
    parser.add_argument("--follow", "-f", action="store_true",
                        help="Follow logs in real-time")
    parser.add_argument("--tail", "-t", type=int, default=50,
                        help="Number of log lines (default: 50)")
    parser.add_argument("--service", type=str,
                        help="Specific Docker service for logs")
    parser.add_argument("--mcp", action="store_true",
                        help="Start MCP server only")
    parser.add_argument("--dashboard", action="store_true",
                        help="Start Dashboard server only")
    parser.add_argument("--excel", action="store_true",
                        help="Start Excel plugin dev server")
    parser.add_argument("--no-wait", action="store_true",
                        help="Don't wait for services to be healthy")

    args = parser.parse_args()

    print_banner()

    # Handle individual service modes
    if args.mcp:
        start_mcp_server(background=False)
        return 0

    if args.dashboard:
        start_dashboard_server(background=False)
        return 0

    if args.excel:
        start_excel_plugin_server(background=False)
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
        check_all_services_status()
        test_backend_endpoint()
        print_access_info()
        return 0

    # Handle stop
    if args.stop:
        stop_all_services()
        return 0

    # Default: Start Docker services (and optionally others with --all)
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
        wait_for_docker_services(timeout=120)

    # Start additional services if --all
    if args.all:
        start_dashboard_server(background=True)
        time.sleep(2)

    # Show status
    check_all_services_status()
    test_backend_endpoint()
    print_access_info()

    print(f"{Colors.GREEN}{Colors.BOLD}Services started successfully!{Colors.END}")
    print(f"\n{Colors.CYAN}Quick Commands:{Colors.END}")
    print(f"  python start_services.py --mcp        Start MCP server")
    print(f"  python start_services.py --dashboard  Start Dashboard")
    print(f"  python start_services.py --status     Check status")
    print(f"  python start_services.py --stop       Stop all services")

    return 0


if __name__ == "__main__":
    try:
        sys.exit(main())
    except KeyboardInterrupt:
        print(f"\n{Colors.YELLOW}Interrupted. Cleaning up...{Colors.END}")
        stop_all_services()
        sys.exit(130)
