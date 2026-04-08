#!/usr/bin/env python3
"""
generate_pin_images.py v3
- Uses repo fonts (assets/fonts/)
- Larger product image, auto-crops whitespace
- 1-sentence description below title
- Pillow-drawn arrow (no emoji)
- Pin generated BEFORE sheet append
- Hooked into generate_posts.py via make_pin_for_post()
"""
import os, re, urllib.request, urllib.error
from pathlib import Path
from io import BytesIO
from dotenv import load_dotenv

load_dotenv(Path.home() / '.env')

from PIL import Image, ImageDraw, ImageFont, ImageOps
import gspread
from google.oauth2.service_account import Credentials


def log_pin(msg: str, level: str = "INFO") -> None:
    line = f"{datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')} [PINGEN]    [{level}]  {msg}"
    print(line, flush=True)


REPO      = Path(__file__).parent
PINS_DIR  = REPO / 'assets' / 'images' / 'pins'
POSTS_DIR = REPO / '_posts'
FONT_DIR  = REPO / 'assets' / 'fonts'
SITE_URL  = 'https://happypetproductreviews.com'

PINS_DIR.mkdir(parents=True, exist_ok=True)

PEACH = '#FFEEE4'; CORAL = '#FF6B4A'; TEAL = '#0D5C63'; SUN = '#FFD166'

def hex2rgb(h):
    h = h.lstrip('#')
    if len(h) == 3: h = ''.join(c*2 for c in h)
    return tuple(int(h[i:i+2], 16) for i in (0, 2, 4))

THEMES = [
    {'bg':PEACH, 'accent':CORAL, 'chip_bg':TEAL,  'chip_fg':SUN,    'title':TEAL,     'desc':TEAL,      'cta_bg':CORAL, 'cta_fg':'#FFF', 'brand':TEAL},
    {'bg':TEAL,  'accent':SUN,   'chip_bg':CORAL,  'chip_fg':'#FFF', 'title':'#FFFFFF','desc':'#C8E6E8', 'cta_bg':SUN,   'cta_fg':TEAL,   'brand':'#FFF'},
    {'bg':CORAL, 'accent':SUN,   'chip_bg':'#FFF', 'chip_fg':CORAL,  'title':'#FFFFFF','desc':'#FFD4CB', 'cta_bg':TEAL,  'cta_fg':'#FFF', 'brand':'#FFF'},
    {'bg':SUN,   'accent':TEAL,  'chip_bg':TEAL,   'chip_fg':SUN,    'title':TEAL,     'desc':TEAL,      'cta_bg':CORAL, 'cta_fg':'#FFF', 'brand':TEAL},
]

CAT_LABELS = {
    'cat-feeders':'Cat Feeders',   'cat-carriers':'Cat Carriers',
    'cat-litter':'Cat Litter',     'cat-scratching':'Cat Scratching',
    'dog-beds':'Dog Beds',         'dog-collars':'Dog Collars',
    'dog-toys':'Dog Toys',         'dog-harnesses':'Dog Harnesses',
    'pet-feeding':'Pet Feeding',   'dog-training':'Dog Training',
}
CTA_LABELS = {
    'cat-feeders':'Read the Review',   'cat-carriers':'See Our Pick',
    'cat-litter':'Read the Review',    'cat-scratching':'See Our Picks',
    'dog-beds':'Read the Review',      'dog-collars':'See Our Picks',
    'dog-toys':'See Our Picks',        'dog-harnesses':'Read the Review',
    'pet-feeding':'Read the Review',   'dog-training':'See Our Picks',
}

def get_font(name, size):
    for d in [FONT_DIR, Path('/tmp/happpet_fonts')]:
        p = d / name
        if p.exists():
            try: return ImageFont.truetype(str(p), size)
            except: pass
    return ImageFont.load_default()

def fetch_image(url):
    try:
        req = urllib.request.Request(url, headers={'User-Agent':'Mozilla/5.0'})
        with urllib.request.urlopen(req, timeout=10) as r:
            data = r.read()
        return Image.open(BytesIO(data)).convert('RGBA')
    except Exception as e:
        log_pin(f'image fetch failed: {e}', 'WARN')
        return None

def autocrop_whitespace(img, threshold=240):
    diff = ImageOps.invert(img.convert('RGB'))
    bbox = diff.getbbox()
    if bbox:
        pad = 20
        x1 = max(0, bbox[0]-pad); y1 = max(0, bbox[1]-pad)
        x2 = min(img.width, bbox[2]+pad); y2 = min(img.height, bbox[3]+pad)
        return img.crop((x1, y1, x2, y2))
    return img

def wrap_text(draw, text, font, max_width):
    words = text.split()
    lines, current = [], ''
    for word in words:
        test = (current + ' ' + word).strip()
        if draw.textlength(test, font=font) <= max_width:
            current = test
        else:
            if current: lines.append(current)
            current = word
    if current: lines.append(current)
    return lines

def get_stage_bg(img, threshold=235):
    """Sample image edges to detect background color. Returns RGB tuple."""
    w, h = img.size
    rgb = img.convert('RGB')
    samples = []
    for x in range(0, w, w//10):
        samples.append(rgb.getpixel((x, 0)))
        samples.append(rgb.getpixel((x, h-1)))
    for y in range(0, h, h//10):
        samples.append(rgb.getpixel((0, y)))
        samples.append(rgb.getpixel((w-1, y)))
    avg = tuple(sum(c[i] for c in samples)//len(samples) for i in range(3))
    # If near-white keep white, else use the sampled color slightly lightened
    if all(v >= threshold for v in avg):
        return (255, 255, 255)
    return tuple(min(255, v + 20) for v in avg)

def draw_rounded_rect(draw, xy, radius, fill):
    x1, y1, x2, y2 = xy
    draw.rectangle([x1+radius, y1, x2-radius, y2], fill=fill)
    draw.rectangle([x1, y1+radius, x2, y2-radius], fill=fill)
    for cx, cy in [(x1,y1),(x2-2*radius,y1),(x1,y2-2*radius),(x2-2*radius,y2-2*radius)]:
        draw.ellipse([cx, cy, cx+2*radius, cy+2*radius], fill=fill)

def draw_arrow(draw, x, y, color, size=30):
    """Draw a right-pointing arrow using Pillow primitives -- no emoji."""
    mid = y + size // 2
    draw.rectangle([x, mid-3, x+size-10, mid+3], fill=color)
    draw.polygon([(x+size-12, y+4), (x+size, mid), (x+size-12, y+size-4)], fill=color)

def make_pin(title, description, product_img_url, category, slug, theme_idx):
    W, H = 1000, 1500
    t = THEMES[theme_idx % len(THEMES)]

    f_title = get_font('Fredoka-Bold.ttf', 78)
    f_desc  = get_font('Nunito-Bold.ttf', 36)
    f_chip  = get_font('Nunito-Bold.ttf', 30)
    f_cta   = get_font('Nunito-Bold.ttf', 36)
    f_brand = get_font('Fredoka-Bold.ttf', 30)

    dummy = Image.new('RGB', (W, H))
    ddraw = ImageDraw.Draw(dummy)
    title_lines = wrap_text(ddraw, title, f_title, W - 100)[:3]
    desc_lines  = wrap_text(ddraw, description, f_desc, W - 100)[:3] if description else []
    n_title = len(title_lines)
    n_desc  = len(desc_lines)

    TOP_BAR   = 90
    CHIP_H    = 62
    CHIP_GAP  = 36
    TITLE_H   = n_title * 90
    DESC_H    = n_desc * 50 + (20 if n_desc else 0)
    CTA_H     = 86
    BOTTOM    = 60
    text_zone = CHIP_GAP + CHIP_H + 16 + TITLE_H + DESC_H + 24 + CTA_H + BOTTOM
    IMG_H     = max(H - TOP_BAR - text_zone, 580)

    img  = Image.new('RGB', (W, H), hex2rgb(t['bg']))
    draw = ImageDraw.Draw(img)

    draw.rectangle([0, 0, W, 16], fill=hex2rgb(t['accent']))
    draw.text((50, 30), 'happypetproductreviews.com', font=f_brand, fill=hex2rgb(t['brand']))

    IMG_Y = TOP_BAR
    prod_img = fetch_image(product_img_url) if product_img_url else None
    stage_bg = get_stage_bg(prod_img) if prod_img else (255, 255, 255)
    draw.rectangle([0, IMG_Y, W, IMG_Y + IMG_H], fill=stage_bg)

    if prod_img:
        prod_img = autocrop_whitespace(prod_img)
        PAD = 30
        scale = min((W-PAD*2)/prod_img.width, (IMG_H-PAD*2)/prod_img.height)
        nw, nh = int(prod_img.width*scale), int(prod_img.height*scale)
        prod_img = prod_img.resize((nw, nh), Image.LANCZOS)
        px = (W - nw) // 2
        py = IMG_Y + (IMG_H - nh) // 2
        if prod_img.mode == 'RGBA':
            img.paste(prod_img, (px, py), prod_img)
        else:
            img.paste(prod_img.convert('RGB'), (px, py))

    y = IMG_Y + IMG_H + CHIP_GAP

    cat_label = CAT_LABELS.get(category, category).upper()
    chip_w = int(draw.textlength(cat_label, font=f_chip)) + 56
    draw_rounded_rect(draw, [50, y, 50+chip_w, y+CHIP_H], 31, hex2rgb(t['chip_bg']))
    draw.text((50+28, y+13), cat_label, font=f_chip, fill=hex2rgb(t['chip_fg']))
    y += CHIP_H + 16

    for line in title_lines:
        draw.text((50, y), line, font=f_title, fill=hex2rgb(t['title']))
        y += 90

    if desc_lines:
        y += 6
        for line in desc_lines:
            draw.text((50, y), line, font=f_desc, fill=hex2rgb(t['desc']))
            y += 50

    y += 24
    cta_label  = CTA_LABELS.get(category, 'Read the Review')
    cta_text_w = int(draw.textlength(cta_label, font=f_cta))
    arrow_size = 30
    arrow_gap  = 18
    btn_w = cta_text_w + arrow_gap + arrow_size + 72
    draw_rounded_rect(draw, [50, y, 50+btn_w, y+76], 38, hex2rgb(t['cta_bg']))
    draw.text((50+36, y+18), cta_label, font=f_cta, fill=hex2rgb(t['cta_fg']))
    draw_arrow(draw, 50+36+cta_text_w+arrow_gap, y+(76-arrow_size)//2,
               hex2rgb(t['cta_fg']), size=arrow_size)

    url_text = 'happypetproductreviews.com'
    url_w = int(draw.textlength(url_text, font=f_brand))
    draw.text((W-50-url_w, H-44), url_text, font=f_brand,
              fill=(*hex2rgb(t['brand']), 120))

    out_path = PINS_DIR / f'{slug}.jpg'
    img.save(str(out_path), 'JPEG', quality=93)
    return out_path

def parse_posts():
    posts = []
    for fname in sorted(os.listdir(POSTS_DIR)):
        if not fname.endswith('.md'): continue
        text = open(POSTS_DIR / fname).read()
        fm = {}
        m = re.match(r'^---\n(.*?)\n---', text, re.DOTALL)
        if m:
            for line in m.group(1).splitlines():
                if ':' in line:
                    k, _, v = line.partition(':')
                    fm[k.strip()] = v.strip().strip('"').strip("'")
        parts = fname.replace('.md','').split('-',3)
        slug = parts[3] if len(parts)==4 else fname.replace('.md','')
        cat  = fm.get('categories','').strip('[]')
        posts.append({
            'title':       fm.get('title',''),
            'description': fm.get('description',''),
            'image':       fm.get('image',''),
            'species':     fm.get('species','both'),
            'cat':         cat,
            'slug':        slug,
            'url':         f'{SITE_URL}/{cat}/{slug}/',
        })
    return posts

def update_sheets(posts_with_pins):
    KEY_FILE = REPO / 'happypet-sheets-key.json'
    if not KEY_FILE.exists():
        log_pin('key file missing', 'WARN'); return
    creds = Credentials.from_service_account_file(str(KEY_FILE),
        scopes=['https://www.googleapis.com/auth/spreadsheets'])
    gc = gspread.authorize(creds)
    DOG_ID = os.getenv('HAPPYPET_SHEET_ID_DOGS')
    CAT_ID = os.getenv('HAPPYPET_SHEET_ID_CATS')
    for label, sid, sp_filter in [('Dogs',DOG_ID,('dog','both')),('Cats',CAT_ID,('cat','both'))]:
        sh   = gc.open_by_key(sid)
        ws   = sh.get_worksheet(0)
        rows = ws.get_all_values()
        updates = []
        for i, row in enumerate(rows[1:], start=2):
            for p in posts_with_pins:
                if p['species'] in sp_filter and row[0] == p['title']:
                    updates.append({'range': f'C{i}', 'values': [[p['pin_url']]]})
                    break
        if updates:
            ws.batch_update(updates)
            log_pin(f'{label}: updated {len(updates)} pin URLs')

def make_pin_for_post(title, description, image_url, category, slug, theme_idx):
    """Called by generate_posts.py. Generates pin image and returns hosted URL."""
    make_pin(title, description, image_url, category, slug, theme_idx)
    return f'{SITE_URL}/assets/images/pins/{slug}.jpg'

def main(update_sheets_flag=True):
    posts = parse_posts()
    log_pin(f'Found {len(posts)} posts')
    results = []
    for i, p in enumerate(posts):
        log_pin(f'[{i+1}/{len(posts)}] {p["title"][:55]}')
        make_pin(p['title'], p['description'], p['image'], p['cat'], p['slug'], i)
        pin_url = f'{SITE_URL}/assets/images/pins/{p["slug"]}.jpg'
        log_pin(f'  -> {pin_url}')
        results.append({**p, 'pin_url': pin_url})
    if update_sheets_flag:
        log_pin('\nUpdating Google Sheets...')
        update_sheets(results)
    log_pin('\nCommitting...')
    os.system(f'cd {REPO} && git add assets/images/pins/ && git commit -m "Regenerate branded Pinterest pin images" && git push')
    log_pin('Done.')
    return results

if __name__ == '__main__':
    main()
