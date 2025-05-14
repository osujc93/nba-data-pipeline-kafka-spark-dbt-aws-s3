#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
- create Kafka topic for NBA player box-score data.
- Fetch NBA data (spanning decades of seasons) in date ranges.
- Validate the responses with Pydantic.
- Publish each row to Kafka in JSON format and log failures for retry.
"""

import argparse
import logging
import time

from nbaBoxScoreIngestion import NBABoxScoreIngestion

logging.basicConfig(level=logging.DEBUG, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def main() -> None:
    ingestion = NBABoxScoreIngestion(topic="NBA_player_boxscores")
    ingestion.run()

if __name__ == "__main__":
    main()
