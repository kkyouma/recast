"""ONLY FOR DEVELOPMENT.

This script is used to test the pipeline process.
"""

import logging
import os
import sys

sys.path.insert(
    0, os.path.abspath(os.path.join(os.path.dirname(__file__), "src", "ingest"))
)

from cen_collector import main  # noqa: E402, I001 # type: ignore


logging.basicConfig(level=logging.INFO)

if __name__ == "__main__":
    main()
