#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
   - Reads the latest known “last_ingestion_date” from a metadata table in PostgreSQL.
   - Computes a new date range from that day onward (up to today) to fetch fresh data
     instead of re-pulling everything historically.
   - After successful ingestion, updates PostgreSQL with the new final ingestion date.
"""

import argparse
import logging
import time

from nbaIncrementalClass import NBAIncrementalClass

logging.basicConfig(level=logging.DEBUG, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def main() -> None:
    ingestion = NBAIncrementalClass(
        topic="NBA_player_boxscores_incr"
    )
    ingestion.run()

if __name__ == "__main__":
    main()
