"""ONLY FOR DEVELOPMENT.

This script is used to test the pipeline process.
"""

import logging

from ingest.cen.collector import main

logging.basicConfig(level=logging.INFO)

if __name__ == "__main__":
    main()
