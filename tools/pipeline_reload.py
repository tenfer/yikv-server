#!/usr/bin/env python3
"""Compatibility entrypoint: delegates to schedule_pipeline (HTTP agents).

Use tools/pipeline_agent/pipeline_agent.py on build/online hosts and tools/schedule_pipeline.py from the scheduler.

  pip install -r tools/pipeline_agent/requirements.txt
  python3 tools/pipeline_agent/pipeline_agent.py

  python3 tools/pipeline_reload.py --data-dir /path/to/raw -v   # stderr: full args + JSON bodies
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
