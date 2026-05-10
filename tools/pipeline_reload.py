#!/usr/bin/env python3
"""Compatibility entrypoint: delegates to schedule_pipeline (HTTP agents).

The all-in-one import/push/pull/reload implementation lives in pipeline_agent + this thin client.
For shell-based local debugging without agents, see tools/pipeline_reload.sh (legacy).

  pip install -r tools/pipeline_agent/requirements.txt
  python3 tools/pipeline_agent/pipeline_agent.py   # on build and/or online hosts

  python3 tools/pipeline_reload.py --data-dir /path/to/raw   # same flags as schedule_pipeline.py
"""

from __future__ import annotations

import sys
from pathlib import Path

_tools_dir = Path(__file__).resolve().parent
if str(_tools_dir) not in sys.path:
    sys.path.insert(0, str(_tools_dir))

from schedule_pipeline import main

if __name__ == "__main__":
    main(sys.argv[1:])
