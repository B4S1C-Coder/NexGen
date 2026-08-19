"""Spawn and wait for the Master, Query, and RAG FastAPI processes."""

from __future__ import annotations

import os
import signal
import subprocess
import sys
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Mapping

import httpx

REPO_ROOT = Path(__file__).resolve().parent.parent


@dataclass(frozen=True)
class ServiceSpec:
    """One FastAPI process the TUI supervises.

    Attributes:
        name: Short label shown in the TUI header.
        cwd: Working directory passed to uvicorn (the service package root).
        port: TCP port the service binds.
        mock: Whether ``MOCK_SERVICES=true`` is injected.
    """

    name: str
    cwd: Path
    port: int
    mock: bool


DEFAULT_SERVICES = (
    ServiceSpec("query", REPO_ROOT / "query", 8001, mock=True),
    ServiceSpec("rag", REPO_ROOT / "rag", 8002, mock=True),
    ServiceSpec("master", REPO_ROOT / "master", 8000, mock=False),
)


def build_service_env(spec: ServiceSpec, extra: Mapping[str, str] | None = None) -> dict[str, str]:
    """Construct the subprocess environment for a NexGen service.

    Args:
        spec: Service identity, port, and mock flag.
        extra: Optional extra environment variables.

    Returns:
        A full environment mapping suitable for ``subprocess.Popen``.
    """
    env = os.environ.copy()
    pythonpath = os.pathsep.join(
        [
            str(spec.cwd),
            str(REPO_ROOT / "nexgen_shared"),
            str(REPO_ROOT),
            env.get("PYTHONPATH", ""),
        ]
    )
    env["PYTHONPATH"] = pythonpath
    env["MOCK_SERVICES"] = "true" if spec.mock else "false"
    if spec.name == "master":
        env.setdefault("QUERY_SERVICE_URL", "http://127.0.0.1:8001")
        env.setdefault("RAG_SERVICE_URL", "http://127.0.0.1:8002")
    if extra:
        env.update(extra)
    return env


class ServiceSupervisor:
    """Start uvicorn for Query, RAG, and Master, then tear them down on exit."""

    def __init__(self, specs: tuple[ServiceSpec, ...] = DEFAULT_SERVICES) -> None:
        self.specs = specs
        self.processes: list[subprocess.Popen[bytes]] = []

    def start(self, timeout_seconds: float = 20.0) -> None:
        """Spawn each service and block until ``/health`` returns 200.

        Args:
            timeout_seconds: Per-service bound for the health wait.

        Raises:
            RuntimeError: If a process exits early or never becomes healthy.
        """
        for spec in self.specs:
            proc = subprocess.Popen(
                [
                    sys.executable,
                    "-m",
                    "uvicorn",
                    "src.main:app",
                    "--host",
                    "127.0.0.1",
                    "--port",
                    str(spec.port),
                    "--log-level",
                    "warning",
                ],
                cwd=str(spec.cwd),
                env=build_service_env(spec),
                stdout=subprocess.DEVNULL,
                stderr=subprocess.PIPE,
            )
            self.processes.append(proc)
            self._wait_healthy(spec, proc, timeout_seconds)

    def _wait_healthy(
        self,
        spec: ServiceSpec,
        proc: subprocess.Popen[bytes],
        timeout_seconds: float,
    ) -> None:
        deadline = time.monotonic() + timeout_seconds
        url = f"http://127.0.0.1:{spec.port}/health"
        last_error = "timeout"
        while time.monotonic() < deadline:
            if proc.poll() is not None:
                err = (proc.stderr.read() if proc.stderr else b"").decode("utf-8", "replace")
                raise RuntimeError(
                    f"{spec.name} exited during startup (code {proc.returncode}): {err[-2000:]}"
                )
            try:
                response = httpx.get(url, timeout=0.5)
                if response.status_code == 200:
                    return
                last_error = f"HTTP {response.status_code}"
            except httpx.HTTPError as exc:
                last_error = str(exc)
            time.sleep(0.2)
        raise RuntimeError(f"{spec.name} did not become healthy: {last_error}")

    def stop(self) -> None:
        """Terminate all child uvicorn processes."""
        for proc in self.processes:
            if proc.poll() is not None:
                continue
            proc.send_signal(signal.SIGTERM)
            try:
                proc.wait(timeout=5)
            except subprocess.TimeoutExpired:
                proc.kill()
        self.processes.clear()
