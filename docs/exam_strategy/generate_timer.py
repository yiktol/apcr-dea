#!/usr/bin/env python3
"""
Generate a 2-minute countdown timer as an animated GIF.
Each frame = 1 second, 121 frames total (2:00 down to 0:00).
Styled to match the AWS dark theme used in the PPTX slides.
"""

from PIL import Image, ImageDraw, ImageFont
import os, math

OUTPUT = os.path.join(os.path.dirname(os.path.abspath(__file__)), "countdown_2min.gif")

# Dimensions — sized to fit nicely in a corner of a widescreen slide
W, H = 320, 140
TOTAL_SECONDS = 120

# Colors
BG = (35, 47, 62)        # AWS dark
ORANGE = (255, 153, 0)   # AWS orange
WHITE = (255, 255, 255)
RED = (198, 40, 40)
GRAY = (117, 117, 117)
GREEN = (46, 125, 50)

# Try to get a nice monospace font, fall back to default
try:
    font_big = ImageFont.truetype("/System/Library/Fonts/Menlo.ttc", 52)
    font_small = ImageFont.truetype("/System/Library/Fonts/Menlo.ttc", 16)
except Exception:
    try:
        font_big = ImageFont.truetype("/System/Library/Fonts/Courier.dfont", 52)
        font_small = ImageFont.truetype("/System/Library/Fonts/Courier.dfont", 16)
    except Exception:
        font_big = ImageFont.load_default()
        font_small = ImageFont.load_default()

frames = []

for sec in range(TOTAL_SECONDS, -1, -1):
    img = Image.new("RGB", (W, H), BG)
    draw = ImageDraw.Draw(img)

    # Progress bar background
    bar_x, bar_y, bar_w, bar_h = 20, 108, W - 40, 12
    draw.rounded_rectangle([bar_x, bar_y, bar_x + bar_w, bar_y + bar_h],
                           radius=6, fill=(60, 70, 85))

    # Progress bar fill
    progress = sec / TOTAL_SECONDS
    fill_w = int(bar_w * progress)
    if fill_w > 0:
        bar_color = GREEN if sec > 30 else (ORANGE if sec > 10 else RED)
        draw.rounded_rectangle([bar_x, bar_y, bar_x + fill_w, bar_y + bar_h],
                               radius=6, fill=bar_color)

    # Timer text
    mins = sec // 60
    secs = sec % 60
    time_str = f"{mins}:{secs:02d}"

    if sec <= 10:
        time_color = RED
    elif sec <= 30:
        time_color = ORANGE
    else:
        time_color = WHITE

    # Center the time text
    bbox = draw.textbbox((0, 0), time_str, font=font_big)
    tw = bbox[2] - bbox[0]
    th = bbox[3] - bbox[1]
    tx = (W - tw) // 2
    ty = 18
    draw.text((tx, ty), time_str, fill=time_color, font=font_big)

    # Label
    label = "TIME REMAINING"
    lbbox = draw.textbbox((0, 0), label, font=font_small)
    lw = lbbox[2] - lbbox[0]
    draw.text(((W - lw) // 2, 85), label, fill=GRAY, font=font_small)

    # Border
    draw.rounded_rectangle([2, 2, W - 3, H - 3], radius=10, outline=ORANGE, width=2)

    frames.append(img)

# Save as animated GIF — 1 second per frame
frames[0].save(
    OUTPUT,
    save_all=True,
    append_images=frames[1:],
    duration=1000,  # 1000ms per frame
    loop=0,         # loop forever (restarts after finishing)
)

print(f"Saved: {OUTPUT}")
print(f"Frames: {len(frames)}, Duration: {TOTAL_SECONDS}s")
fsize = os.path.getsize(OUTPUT)
print(f"File size: {fsize / 1024:.0f} KB")
