import argparse
import os
import subprocess
import sys
from pathlib import Path


def main() -> int:
    parser = argparse.ArgumentParser(description="Run the GridTanks backend.")
    parser.add_argument("--host", default="127.0.0.1", help="Host to bind to.")
    parser.add_argument("--port", default="8000", help="Port to bind to.")
    parser.add_argument(
        "--no-reload",
        action="store_true",
        help="Disable uvicorn's auto-reload.",
    )
    args, uvicorn_args = parser.parse_known_args()

    backend_dir = Path(__file__).resolve().parent

    if not backend_dir.exists():
        print(f"Could not find backend directory at {backend_dir}", file=sys.stderr)
        return 1

    env = os.environ.copy()
    env.setdefault("ENVIRONMENT", "development")

    python = sys.executable
    if not os.environ.get("VIRTUAL_ENV"):
        venv_dir = backend_dir / "venv"
        if os.name == "nt":
            venv_python = venv_dir / "Scripts" / "python.exe"
        else:
            venv_python = venv_dir / "bin" / "python"

        if venv_python.exists():
            python = str(venv_python)

    command = [
        python,
        "-m",
        "uvicorn",
        "app.main:app",
        "--host",
        args.host,
        "--port",
        args.port,
    ]

    if not args.no_reload:
        command.append("--reload")

    command.extend(uvicorn_args)

    print(f"Starting GridTanks backend on http://{args.host}:{args.port}")
    print(f"ENVIRONMENT={env['ENVIRONMENT']}")

    return subprocess.call(command, cwd=backend_dir, env=env)


if __name__ == "__main__":
    raise SystemExit(main())
