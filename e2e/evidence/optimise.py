#!/usr/bin/env python3
"""Shrinks the evidence PNGs in place: an adaptive palette (screenshots of a flat UI
use few colours; 256 keep the section chips and the status tints true, 64 do not),
no dithering.

usage: optimise.py <dir> [max-colours]   (needs Pillow; without it the files are left as they are)
"""
import os
import sys

try:
    from PIL import Image
except ImportError:
    print("Pillow is not installed: PNGs left unoptimised")
    sys.exit(0)

root = sys.argv[1]
colours = int(sys.argv[2]) if len(sys.argv) > 2 else 256
before = after = 0
for dirpath, _, files in os.walk(root):
    for f in files:
        if not f.endswith(".png"):
            continue
        p = os.path.join(dirpath, f)
        before += os.path.getsize(p)
        im = Image.open(p).convert("RGB")
        q = im.quantize(colors=colours, method=Image.Quantize.MEDIANCUT, dither=Image.Dither.NONE)
        q.save(p, optimize=True)
        after += os.path.getsize(p)
print(f"optimised PNGs: {before/1e6:.1f} MB -> {after/1e6:.1f} MB")
