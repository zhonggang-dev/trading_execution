from __future__ import annotations

import logging
import os

import uvicorn

from .app import create_app
from .config import Settings


def main() -> None:
    logging.basicConfig(
        level=os.environ.get("PM_DECISION_LOG_LEVEL", "INFO").upper(),
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )
    port = int(os.environ.get("PM_DECISION_PORT", "8787"))
    if not 1 <= port <= 65535:
        raise ValueError("PM_DECISION_PORT must be in [1,65535]")
    uvicorn.run(
        create_app(Settings.from_env()),
        host="127.0.0.1",
        port=port,
        workers=1,
        access_log=False,
        proxy_headers=False,
        server_header=False,
    )


if __name__ == "__main__":
    main()
