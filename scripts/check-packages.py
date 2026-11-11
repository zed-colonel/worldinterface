#!/usr/bin/env python3
"""Internal release gate: prepare archives, or build their documentation."""
import argparse
import sys
sys.dont_write_bytecode = True
from pathlib import Path
from wi_release_packages import build_packages, rustdoc

if __name__ == '__main__':
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument('--output', type=Path, required=True)
    p.add_argument('--work', type=Path, required=True)
    p.add_argument('--version', required=True)
    p.add_argument('--rustdoc', action='store_true')
    a = p.parse_args()
    if a.rustdoc: rustdoc(a.output, a.work)
    else: build_packages(a.output, a.work, a.version)
