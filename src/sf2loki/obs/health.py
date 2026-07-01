"""Asyncio HTTP health server for sf2loki.

Two endpoints:
  GET /healthz  — liveness:  always 200 "ok"
  GET /readyz   — readiness: 200 "ready" | 503 "not ready"

Design is split into a pure routing helper (decide) and a server class (Health)
so unit tests can exercise routing without touching sockets.
"""

from __future__ import annotations

import asyncio


def decide(path: str, *, ready: bool) -> tuple[int, str]:
    """Return (status_code, body) for the given request path and readiness state."""
    if path == "/healthz":
        return 200, "ok"
    if path == "/readyz":
        return (200, "ready") if ready else (503, "not ready")
    return 404, "not found"


_REASON_PHRASES: dict[int, str] = {
    200: "OK",
    404: "Not Found",
    503: "Service Unavailable",
}


class Health:
    """Minimal asyncio HTTP health server.

    ``read_timeout`` bounds how long a connected client may take to send its
    request; a client that connects and never sends would otherwise hold a
    coroutine + fd forever.
    """

    def __init__(self, *, read_timeout: float = 5.0) -> None:
        self._ready: bool = False
        self._server: asyncio.Server | None = None
        self._read_timeout: float = read_timeout
        self.port: int = 0  # set after start(); reflects actual bound port

    @property
    def ready(self) -> bool:
        return self._ready

    def set_ready(self) -> None:
        self._ready = True

    def set_not_ready(self) -> None:
        self._ready = False

    async def start(self, addr: str) -> None:
        """Start listening on *addr* (e.g. ':8080' or '0.0.0.0:8080').

        Use ':0' to let the OS assign an ephemeral port; read it back from self.port.
        """
        host, _, port_str = addr.rpartition(":")
        host = host or "0.0.0.0"  # bind all interfaces so container health probes can reach it
        port = int(port_str)

        self._server = await asyncio.start_server(
            self._handle,
            host=host,
            port=port,
        )
        # Discover the actual bound port (important when port=0)
        sock = self._server.sockets[0]
        self.port = sock.getsockname()[1]
        await self._server.start_serving()

    async def stop(self) -> None:
        """Stop the server and wait for all connections to close."""
        if self._server is not None:
            self._server.close()
            await self._server.wait_closed()
            self._server = None

    async def _handle(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        try:
            try:
                async with asyncio.timeout(self._read_timeout):
                    request_line = await reader.readline()
                    # Drain remaining headers until blank line
                    while True:
                        line = await reader.readline()
                        if line in (b"\r\n", b"\n", b""):
                            break
            except TimeoutError:
                # Slow/silent client: drop the connection, free the coroutine/fd.
                return

            # Parse: b"GET /path HTTP/1.1\r\n"
            parts = request_line.split()
            path = parts[1].decode("utf-8", errors="replace") if len(parts) >= 2 else "/"

            status, body = decide(path, ready=self._ready)
            reason = _REASON_PHRASES.get(status, "")
            body_bytes = body.encode()
            response = (
                f"HTTP/1.1 {status} {reason}\r\n"
                f"Content-Length: {len(body_bytes)}\r\n"
                f"Connection: close\r\n"
                f"\r\n"
            ).encode() + body_bytes
            writer.write(response)
            await writer.drain()
        finally:
            writer.close()
            await writer.wait_closed()
