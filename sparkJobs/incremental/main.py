#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import logging
import sys

from nbaIncrementalClass import NBAIncrementalLoad

logging.basicConfig(
    level=logging.DEBUG,
    format="%(asctime)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)

def main() -> None:
    app = NBAIncrementalLoad()
    app.run()

if __name__ == "__main__":
    main()
