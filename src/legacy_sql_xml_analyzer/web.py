from __future__ import annotations

from functools import partial
from http.server import SimpleHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path


def serve_report(root: Path, host: str = "127.0.0.1", port: int = 8000) -> None:
    root = root.resolve()
    dashboard_url = "/dashboard.html"
    if (root / "analysis" / "dashboard.html").exists():
        dashboard_url = "/analysis/dashboard.html"
    elif not (root / "dashboard.html").exists():
        raise FileNotFoundError(
            f"Could not find dashboard.html under {root}. Run analyze first to generate the web dashboard."
        )

    handler = partial(SimpleHTTPRequestHandler, directory=str(root))
    server = ThreadingHTTPServer((host, port), handler)
    print(f"Serving report at http://{host}:{port}{dashboard_url}")
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        pass
    finally:
        server.server_close()
