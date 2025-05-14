#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import logging
import sys
import time
import threading
import io

# Configure logging globally 
logging.basicConfig(
    level=logging.DEBUG,
    format="%(asctime)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)

BOOTSTRAP_SERVERS: list[str] = [
    "kafka2:9092",
    "kafka3:9093",
    "kafka4:9094",
]

from firstBatch import FirstBatch

def main() -> None:
    app = FirstBatch(BOOTSTRAP_SERVERS)
    app.run()


if __name__ == "__main__":
    main()
