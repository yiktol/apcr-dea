"""
Generate a 2-minute countdown timer GIF for use in exam strategy slides.
"""
import os
from PIL import Image, ImageDraw, ImageFont

os.chdir(os.path.dirname(os.path.abspath(__file__)))

WIDTH, HEIGHT = 280, 280
BG_COLOR = "#1a237e"
TEXT_COLOR = "#ffffff"
RING_COLOR = "#00e676"
RING_BG = "#37474f"
TOTAL_SECONDS = 120

frames = []
for sec in range(TOTAL_SECONDS, -1, -1):
    img = Image.new("RGBA", (WIDTH, HEIGHT), BG_COLOR)
    draw = ImageDraw.Draw(img)

    # Draw background ring
    margin = 20
    draw.ellipse([margin, margin, WIDTH - margin, HEIGHT - margin],
                 outline=RING_BG, width=10)

    # Draw progress arc
    progress = sec / TOTAL_SECONDS
    end_angle = -90 + (1 - progress) * 360
    draw.arc([margin, margin, WIDTH - margin, HEIGHT - margin],
             start=-90, end=end_angle, fill=RING_COLOR, width=10)

    # Draw time text (large)
    minutes = sec // 60
    secs = sec % 60
    time_str = f"{minutes}:{secs:02d}"

    try:
        font = ImageFont.truetype("/usr/share/fonts/truetype/dejavu/DejaVuSans-Bold.ttf", 56)
    except (OSError, IOError):
        font = ImageFont.load_default()

    bbox = draw.textbbox((0, 0), time_str, font=font)
    tw = bbox[2] - bbox[0]
    th = bbox[3] - bbox[1]
    draw.text(((WIDTH - tw) / 2, (HEIGHT - th) / 2 - 5), time_str,
              fill=TEXT_COLOR, font=font)

    frames.append(img.convert("P", palette=Image.ADAPTIVE, colors=64))

# Save as animated GIF (1 frame per second, plays once)
frames[0].save(
    "countdown_2min.gif",
    save_all=True,
    append_images=frames[1:],
    duration=1000,
    loop=1,
    optimize=True
)

print("Timer GIF generated: countdown_2min.gif")
