#!/usr/bin/env python3
"""Meridian news collector.

Reads feeds.json, downloads the RSS/Atom feeds, groups articles about the same
story, tags them (region, category, prépa theme), ranks them and writes
news.json. Optionally sends a few phone alerts through ntfy.sh.

Standard library only, so it runs on GitHub Actions with no install step.
"""
import hashlib
import html
import json
import math
import os
import re
import sys
import urllib.request
import xml.etree.ElementTree as ET
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta, timezone
from email.utils import parsedate_to_datetime

HERE = os.path.dirname(os.path.abspath(__file__))
NOW = datetime.now(timezone.utc)
UA = "Mozilla/5.0 (compatible; MeridianNewsBot/1.0; +https://github.com)"
MAX_STORIES = 240
DEFAULT_MAX_AGE_H = 48

# --------------------------------------------------------------------------
# Tagging dictionaries
# --------------------------------------------------------------------------

def rx(words, flags=re.I):
    return re.compile(r"\b(?:" + "|".join(words) + r")\b", flags)

REGION_ORDER = ["France", "Europe", "UK", "USA", "Middle East", "Asia", "Africa", "Americas"]
REGION_WEIGHT = {"France": 1.8, "Europe": 1.5, "UK": 1.3, "USA": 1.15}

REGION_RX = {
    "France": rx(["france", "french", "paris", "macron", "élysée", "elysee", "matignon", "le pen", "bardella",
                  "marseille", "lyon", "toulouse", "bordeaux", "sncf", "lecornu", "mélenchon", "melenchon",
                  "retailleau", "national assembly", "riviera", "corsica", "normandy", "brittany", "provence",
                  "strasbourg", "lille", "nantes", "versailles", "louvre"]),
    "Europe": rx(["eu", "european union", "european commission", "european parliament", "brussels",
                  "europe", "european", "eurozone", "ecb", "nato", "ukraine", "ukrainian", "kyiv", "kiev",
                  "zelensky", "zelenskyy", "russia", "russian", "moscow", "kremlin", "putin", "germany", "german",
                  "berlin", "merz", "italy", "italian", "rome", "meloni", "spain", "spanish", "madrid", "poland",
                  "polish", "warsaw", "hungary", "orbán", "orban", "budapest", "netherlands", "dutch", "belgium",
                  "belgian", "sweden", "swedish", "norway", "norwegian", "denmark", "danish", "finland", "finnish",
                  "greece", "greek", "athens", "romania", "portugal", "portuguese", "lisbon", "austria", "vienna",
                  "switzerland", "swiss", "ireland", "irish", "dublin", "czech", "prague", "serbia", "balkans",
                  "baltic", "estonia", "latvia", "lithuania", "slovakia", "croatia", "bulgaria", "moldova",
                  "schengen", "brexit", "von der leyen", "euro"]),
    "UK": rx(["uk", "britain", "british", "england", "scotland", "scottish", "wales", "welsh",
              "london", "westminster", "starmer", "downing street", "nhs", "king charles", "tories", "tory",
              "reform uk", "farage", "reeves", "badenoch", "northern ireland", "manchester", "birmingham",
              "liverpool", "premier league"]),
    "USA": rx(["usa", "united states", "american", "americans", "america", "washington", "trump", "white house",
               "congress", "senate", "senator", "pentagon", "supreme court", "democrats", "republicans", "gop",
               "midterm", "midterms", "new york", "california", "texas", "florida", "fbi", "biden", "vance",
               "harris", "newsom", "ice agents", "federal reserve", "wall street", "nfl", "nba"]),
    "Middle East": rx(["israel", "israeli", "gaza", "hamas", "hezbollah", "lebanon", "syria", "syrian", "iran",
                       "iranian", "tehran", "iraq", "yemen", "houthi", "houthis", "saudi", "qatar", "uae", "dubai",
                       "jordan", "west bank", "palestinian", "palestinians", "middle east", "netanyahu", "turkey",
                       "turkish", "erdogan", "erdoğan"]),
    "Asia": rx(["china", "chinese", "beijing", "xi jinping", "taiwan", "japan", "japanese", "tokyo", "korea",
                "korean", "seoul", "pyongyang", "india", "indian", "delhi", "modi", "pakistan", "afghanistan",
                "taliban", "bangladesh", "indonesia", "philippines", "vietnam", "thailand", "myanmar",
                "singapore", "asia", "asian", "hong kong", "nepal", "sri lanka", "malaysia"]),
    "Africa": rx(["africa", "african", "nigeria", "kenya", "ethiopia", "sudan", "congo", "south africa", "ghana",
                  "senegal", "mali", "niger", "sahel", "somalia", "uganda", "morocco", "algeria", "egypt",
                  "tunisia", "libya", "rwanda", "cameroon", "zimbabwe", "mozambique", "tanzania"]),
    "Americas": rx(["brazil", "brazilian", "argentina", "milei", "mexico", "mexican", "canada", "canadian",
                    "venezuela", "colombia", "chile", "peru", "cuba", "latin america", "caribbean", "haiti",
                    "ecuador", "bolivia", "panama", "lula", "carney", "trudeau"]),
}
US_STRICT = re.compile(r"\bU\.S\b|\bUS\b|\bU\.S\.A\b")  # case-sensitive "US"

CATEGORY_RULES = [  # used only for general feeds (category "World")
    ("Conflict", rx(["war", "wars", "attack", "attacks", "strike", "strikes", "missile", "missiles", "drone",
                     "drones", "killed", "kills", "troops", "army", "military", "ceasefire", "truce", "bomb",
                     "bombing", "shelling", "invasion", "hostage", "hostages", "terror", "terrorist", "airstrike",
                     "airstrikes", "frontline", "offensive"])),
    ("Politics", rx(["election", "elections", "vote", "votes", "voters", "poll", "polls", "government", "minister",
                     "ministers", "parliament", "president", "prime minister", "coalition", "lawmakers", "senate",
                     "congress", "referendum", "campaign", "mps", "cabinet", "diplomat", "diplomacy", "summit",
                     "treaty", "sanctions", "policy"])),
    ("Economy", rx(["economy", "economic", "inflation", "tariff", "tariffs", "trade", "market", "markets",
                    "bank", "banks", "gdp", "budget", "debt", "deficit", "jobs", "unemployment", "wages", "prices",
                    "recession", "growth", "investors", "currency", "oil", "tax", "taxes", "strike action"])),
    ("Environment", rx(["climate", "emissions", "carbon", "wildfire", "wildfires", "flood", "floods", "drought",
                        "heatwave", "biodiversity", "pollution", "renewable", "cop30", "cop31", "extinction",
                        "glacier", "hurricane", "storm", "earthquake", "wildlife", "deforestation"])),
    ("Health", rx(["health", "hospital", "hospitals", "vaccine", "vaccines", "virus", "outbreak", "cancer",
                   "disease", "doctors", "patients", "medical", "drug", "drugs", "epidemic", "pandemic"])),
    ("Tech", rx(["ai", "artificial intelligence", "chatbot", "tech", "cyber", "cyberattack", "hackers", "software",
                 "app", "robot", "robots", "chip", "chips", "semiconductor", "algorithm", "openai", "google",
                 "apple", "microsoft", "meta", "tiktok", "x.com", "starlink"])),
    ("Science", rx(["scientists", "research", "study", "physics", "quantum", "astronomy", "telescope", "nasa",
                    "space", "esa", "mars", "moon", "fossil", "discovery", "nobel"])),
    ("Sport", rx(["football", "tennis", "rugby", "olympic", "olympics", "tour de france", "formula 1", "f1",
                  "world cup", "champions league", "athletes", "marathon", "cricket", "golf", "ligue 1"])),
    ("Culture", rx(["film", "films", "cinema", "festival", "museum", "novel", "author", "music", "album", "singer",
                    "actor", "actress", "theatre", "art", "exhibition", "fashion", "oscar", "cannes", "nobel prize in literature"])),
    ("Society", rx(["migrants", "migration", "immigration", "refugees", "police", "court", "trial", "protest",
                    "protests", "school", "schools", "students", "housing", "homeless", "crime", "prison"])),
]

THEMES = {  # id -> (label, regex) ; matches the vocabulary chapters of the curriculum
    "world": ("World affairs", rx(["diplomacy", "diplomatic", "summit", "united nations", "un security council", "treaty",
                                   "sanctions", "nato", "g7", "g20", "ceasefire", "peace talks", "foreign minister",
                                   "foreign policy", "alliance", "embassy", "geopolitical", "ambassador"])),
    "earth": ("Endangered Earth", rx(["climate", "emissions", "biodiversity", "extinction", "wildfire", "wildfires",
                                      "deforestation", "pollution", "drought", "heatwave", "flood", "floods",
                                      "glacier", "endangered", "wildlife", "ocean", "plastic", "cop30", "cop31",
                                      "global warming", "sea level"])),
    "media": ("The media & the press", rx(["press freedom", "journalist", "journalists", "newspaper", "broadcaster",
                                           "media", "social media", "misinformation", "disinformation", "censorship",
                                           "fake news", "propaganda", "tiktok",
                                           "reporters", "deepfake", "deepfakes", "newsroom", "broadcasting"])),
    "morality": ("Morality & ethics", rx(["ethics", "ethical", "moral", "morality", "corruption", "scandal",
                                          "human rights", "death penalty", "euthanasia", "abortion", "whistleblower",
                                          "bribery", "fraud", "misconduct", "abuse"])),
    "politics": ("Politics & elections", rx(["election", "elections", "vote", "voters", "ballot", "referendum",
                                             "coalition", "populist", "authoritarian", "democracy", "parliament",
                                             "campaign", "midterm", "midterms", "polls", "senate", "congress",
                                             "cabinet", "prime minister", "president", "regime", "dictatorship"])),
    "work": ("Work & the job market", rx(["unemployment", "jobs", "job market", "layoffs", "strike action", "general strike",
                                          "trade union", "trade unions", "wages", "workers", "labour market", "labor market", "pension",
                                          "pensions", "hiring", "remote work", "employment", "salary", "minimum wage",
                                          "redundancies", "workforce", "walkout", "industrial action"])),
    "demography": ("Demographic issues", rx(["population", "birth rate", "birthrate", "ageing", "aging", "migration",
                                             "immigrants", "immigration", "refugees", "asylum", "census",
                                             "life expectancy", "fertility", "baby boom", "demographic"])),
    "inequality": ("Inequalities", rx(["poverty", "inequality", "inequalities", "wealth gap", "billionaire",
                                       "billionaires", "gender gap", "pay gap", "homeless", "homelessness",
                                       "cost of living", "wealth tax", "food banks", "women's rights",
                                       "gender", "feminism", "discrimination", "social housing"])),
    "sustain": ("Sustainability", rx(["renewable", "renewables", "solar", "wind power", "electric vehicles",
                                      "net zero", "energy transition", "green transition", "green energy", "recycling", "circular economy",
                                      "sustainable", "sustainability", "nuclear power", "hydrogen", "battery"])),
    "education": ("Education", rx(["school", "schools", "university", "universities", "students", "student",
                                   "exam", "exams", "teachers", "teacher", "curriculum", "baccalauréat",
                                   "baccalaureate", "tuition", "education", "a-levels", "gcse", "pupils", "campus"])),
    "sport": ("Sports & the Olympics", rx(["olympic", "olympics", "olympian", "world cup", "tournament",
                                           "athletes", "paralympic", "champion", "medal", "los angeles 2028",
                                           "la28", "tour de france", "roland garros", "wimbledon"])),
    "city": ("City life", rx(["urban", "city centre", "city centres", "city council", "smart city", "megacity", "housing", "public transport", "metro", "tourism", "tourists",
                              "rent", "rents", "mayor", "congestion", "commuters",
                              "gentrification", "overtourism", "traffic", "car-free"])),
    "science": ("Maths & physics", rx(["physics", "mathematics", "maths", "quantum", "particle", "astronomy",
                                       "telescope", "nobel", "supercomputer", "space agency", "spacecraft", "cern", "fusion",
                                       "equation", "theorem", "physicists", "mathematicians"])),
    "health": ("Remedies & health", rx(["vaccine", "vaccines", "treatment", "therapy", "hospital", "hospitals",
                                        "cancer", "drug", "drugs", "cure", "epidemic", "pandemic", "mental health",
                                        "nhs", "doctors", "clinical trial", "antibiotic"])),
    "us_inst": ("US institutions", rx(["congress", "senate", "house of representatives", "supreme court",
                                       "white house", "electoral college", "midterm", "midterms", "federal court",
                                       "constitution", "impeachment", "filibuster", "governor"])),
    "uk_inst": ("UK institutions", rx(["parliament", "house of commons", "house of lords", "monarchy", "king charles",
                                       "prime minister", "downing street", "westminster", "speaker", "peers",
                                       "devolution", "holyrood", "senedd", "cabinet"])),
}
THEME_REGION_GATE = {"us_inst": {"USA"}, "uk_inst": {"UK"}}

STOP = set("""a an the and or but of to in on at for from by with as is are was were be been being it its this that
these those his her their our your my he she they we you i not no yes after before over under into out up down
about than then there here what which who whom whose why how when where will would could should may might can
says say said new more most less much many just also still now today yesterday tomorrow amid against between
during while latest live update updates analysis opinion video watch""".split())

BREAKING_RX = rx(["breaking", "dies", "dead", "death toll", "killed", "assassination", "assassinated", "resigns",
                  "resignation", "earthquake", "tsunami", "explosion", "attack", "coup", "evacuated", "shooting",
                  "collapses", "wins election", "snap election", "declares", "invades", "ceasefire", "arrested",
                  "plane crash", "hostages", "state of emergency"])
SKIP_TITLE_RX = rx(["quiz", "crossword", "podcast", "newsletter", "sudoku", "horoscope", "live blog",
                    "as it happened", "photo essay", "in pictures", "the week in"], re.I)

# --------------------------------------------------------------------------
# Feed download / parsing
# --------------------------------------------------------------------------

def local(tag):
    return tag.rsplit("}", 1)[-1] if isinstance(tag, str) else ""


def clean_text(s, limit=None):
    if not s:
        return ""
    s = re.sub(r"<[^>]+>", " ", s)
    s = html.unescape(s)
    s = re.sub(r"\s+", " ", s).strip()
    if limit and len(s) > limit:
        cut = s[:limit].rsplit(" ", 1)[0].rstrip(",;:- ")
        s = cut + "…"
    return s


def parse_date(text):
    if not text:
        return None
    text = text.strip()
    try:
        dt = parsedate_to_datetime(text)
    except Exception:
        dt = None
    if dt is None:
        try:
            dt = datetime.fromisoformat(text.replace("Z", "+00:00"))
        except Exception:
            return None
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)


def parse_feed(data):
    try:
        root = ET.fromstring(data)
    except ET.ParseError:
        text = data.decode("utf-8", "replace")
        text = re.sub(r"&(?!(?:amp|lt|gt|quot|apos|#\d+|#x[0-9a-fA-F]+);)", "&amp;", text)
        text = re.sub(r"^<\?xml[^>]*\?>", "", text.strip())
        root = ET.fromstring(text)
    items = []
    for el in root.iter():
        name = local(el.tag)
        if name not in ("item", "entry"):
            continue
        rec = {"title": "", "link": "", "summary": "", "date": None, "image": ""}
        for ch in el:
            n = local(ch.tag)
            txt = (ch.text or "").strip()
            if n == "title":
                rec["title"] = re.sub(r"\s+[-|–—]\s+(Euronews|France 24|RFI|DW|BBC News|The Guardian|NPR|Al Jazeera|POLITICO Europe|Politico Europe|The Independent|The New York Times|The Washington Post|The Economist|The Local France)\s*$", "", clean_text(txt))
            elif n == "link":
                href = ch.attrib.get("href")
                if href:
                    if ch.attrib.get("rel", "alternate") == "alternate" or not rec["link"]:
                        rec["link"] = href.strip()
                elif txt:
                    rec["link"] = txt
            elif n in ("description", "summary", "encoded", "content") and not rec["summary"]:
                rec["summary"] = clean_text(txt, 320)
            elif n in ("pubDate", "published", "updated", "date") and not rec["date"]:
                rec["date"] = parse_date(txt)
            elif n in ("thumbnail", "content", "enclosure") and not rec["image"]:
                url = ch.attrib.get("url", "")
                typ = ch.attrib.get("type", "") + ch.attrib.get("medium", "")
                if url and ("image" in typ or re.search(r"\.(jpe?g|png|webp)(\?|$)", url, re.I) or n == "thumbnail"):
                    rec["image"] = url
        if rec["title"] and rec["link"]:
            items.append(rec)
    return items


def fetch_feed(feed):
    fixture_dir = os.environ.get("FIXTURE_DIR")
    try:
        if fixture_dir:
            path = os.path.join(fixture_dir, feed["id"] + ".xml")
            if not os.path.exists(path):
                raise FileNotFoundError("no fixture")
            data = open(path, "rb").read()
        else:
            req = urllib.request.Request(feed["url"], headers={
                "User-Agent": UA,
                "Accept": "application/rss+xml, application/atom+xml, application/xml, text/xml, */*"})
            with urllib.request.urlopen(req, timeout=20) as r:
                data = r.read(4_000_000)
                if r.headers.get("Content-Encoding") == "gzip":
                    import gzip
                    data = gzip.decompress(data)
        items = parse_feed(data)
        max_age = timedelta(hours=feed.get("max_age_h", DEFAULT_MAX_AGE_H))
        fresh = []
        for it in items[:40]:
            dt = it["date"] or NOW
            if dt > NOW + timedelta(hours=2):
                dt = NOW
            if NOW - dt > max_age:
                continue
            if SKIP_TITLE_RX.search(it["title"]):
                continue
            it["date"] = dt
            it["feed"] = feed
            fresh.append(it)
        return feed, fresh, None
    except Exception as e:  # keep going whatever happens to a single feed
        return feed, [], f"{type(e).__name__}: {str(e)[:90]}"

# --------------------------------------------------------------------------
# Grouping of articles about the same story
# --------------------------------------------------------------------------

def tokens(title):
    out = set()
    for w in re.findall(r"[a-zà-ÿ0-9]+", title.lower()):
        if w in STOP or len(w) < 3:
            continue
        if len(w) > 4 and w.endswith("s") and not w.endswith("ss"):
            w = w[:-1]
        out.add(w)
    return out


def group_items(items):
    n = len(items)
    parent = list(range(n))

    def find(x):
        while parent[x] != x:
            parent[x] = parent[parent[x]]
            x = parent[x]
        return x

    toks = [tokens(i["title"]) for i in items]
    for a in range(n):
        for b in range(a + 1, n):
            ta, tb = toks[a], toks[b]
            if not ta or not tb:
                continue
            inter = len(ta & tb)
            if inter < 2:
                continue
            small = min(len(ta), len(tb))
            jac = inter / len(ta | tb)
            if (inter >= 3 and inter / small >= 0.6) or (inter >= 2 and small <= 5 and jac >= 0.5):
                parent[find(a)] = find(b)
    groups = {}
    for i in range(n):
        groups.setdefault(find(i), []).append(items[i])
    return list(groups.values())

# --------------------------------------------------------------------------
# Tagging and scoring
# --------------------------------------------------------------------------

def infer_region(group):
    weights = {r: 0 for r in REGION_ORDER}
    for it in group:
        for part, w in ((it["title"], 3), (it["summary"], 1)):
            for r in REGION_ORDER:
                weights[r] += w * len(REGION_RX[r].findall(part))
            if US_STRICT.search(part):
                weights["USA"] += w * 2
    best = max(REGION_ORDER, key=lambda r: (weights[r], -REGION_ORDER.index(r)))
    if weights[best] >= 2:
        return best
    defaults = [it["feed"]["region"] for it in group if it["feed"]["region"] != "World"]
    if defaults:
        return max(set(defaults), key=defaults.count)
    return "World"


def infer_category(group):
    cats = [it["feed"]["category"] for it in group]
    specific = [c for c in cats if c != "World"]
    if specific:
        return max(set(specific), key=specific.count)
    text = " ".join(it["title"] + " " + it["summary"] for it in group)
    title = " ".join(it["title"] for it in group)
    for cat, pattern in CATEGORY_RULES:
        if len(pattern.findall(title)) >= 1 or len(pattern.findall(text)) >= 2:
            return cat
    return "World"


def infer_themes(group, region, category):
    text = " ".join(it["title"] + " " + it["summary"] for it in group)
    found = []
    for tid, (_label, pattern) in THEMES.items():
        gate = THEME_REGION_GATE.get(tid)
        if gate and region not in gate:
            continue
        hits = len(pattern.findall(text))
        if hits >= 1 and (hits >= 2 or pattern.search(group[0]["title"])):
            found.append((hits, tid))
    found.sort(reverse=True)
    themes = [t for _h, t in found[:3]]
    if category == "Sport" and "sport" not in themes and re.search(r"olympic", text, re.I):
        themes.append("sport")
    return themes


def story_id(link):
    return hashlib.sha1(link.encode()).hexdigest()[:10]


def build_story(group):
    group.sort(key=lambda it: (-(it["feed"].get("w", 1.0)), -bool(it["image"]), -len(it["summary"])))
    primary = group[0]
    outlets = {}
    for it in group:
        outlets.setdefault(it["feed"]["outlet"], it)
    coverage = len(outlets)
    latest = max(it["date"] for it in group)
    first = min(it["date"] for it in group)
    region = infer_region(group)
    category = infer_category(group)
    themes = infer_themes(group, region, category)
    summary = max((it["summary"] for it in group), key=lambda s: min(len(s), 300) if s else 0)
    if summary and summary.lower().startswith(primary["title"].lower()[:40]):
        summary = summary[len(primary["title"]):].strip(" -–—:") or summary
    image = next((it["image"] for it in group if it["image"]), "")

    age_h = max(0.0, (NOW - latest).total_seconds() / 3600)
    recency = math.exp(-age_h / 20)
    importance = min(coverage, 8)
    cat_w = {"Sport": 0.8, "Culture": 0.9, "Tech": 1.0}.get(category, 1.0)
    score = (0.7 + 0.45 * importance) * (0.3 + 0.7 * recency) * REGION_WEIGHT.get(region, 1.0) * cat_w
    if image:
        score *= 1.06
    if category in ("Politics", "Conflict", "Economy", "Environment") and coverage >= 3:
        score *= 1.1

    text = primary["title"] + " " + summary
    breaking = (coverage >= 4 and age_h <= 5) or (coverage >= 3 and age_h <= 3 and bool(BREAKING_RX.search(text)))
    return {
        "id": story_id(primary["link"]),
        "title": primary["title"],
        "summary": summary,
        "link": primary["link"],
        "outlet": primary["feed"]["outlet"],
        "published": latest.isoformat(timespec="seconds"),
        "first": first.isoformat(timespec="seconds"),
        "region": region,
        "category": category,
        "themes": themes,
        "coverage": coverage,
        "sources": [{"outlet": o, "title": it["title"], "link": it["link"]} for o, it in list(outlets.items())[:8]],
        "image": image,
        "score": round(score, 3),
        "breaking": bool(breaking),
        "_links": [it["link"] for it in group],
    }

# --------------------------------------------------------------------------
# Alerts (ntfy.sh)
# --------------------------------------------------------------------------

def load_json(path, default):
    try:
        return json.load(open(path, encoding="utf-8"))
    except Exception:
        return default


def send_alerts(stories):
    topic = os.environ.get("NTFY_TOPIC", "").strip()
    dry = os.environ.get("NTFY_DRY_RUN") == "1"
    if not topic and not dry:
        return []
    state_path = os.path.join(HERE, "sent.json")
    state = load_json(state_path, {"sent": {}})
    cutoff = (NOW - timedelta(days=3)).timestamp()
    sent = {k: v for k, v in state.get("sent", {}).items() if v > cutoff}
    last_day = NOW.timestamp() - 86400
    sent_stories_today = len({v for v in sent.values() if v > last_day})
    sent_now = []
    for s in stories:
        if len(sent_now) >= 3 or sent_stories_today + len(sent_now) >= 8:
            break
        if not s["breaking"]:
            continue
        text = s["title"] + " " + s["summary"]
        important = (s["coverage"] >= 5
                     or (s["region"] in ("France", "Europe") and s["coverage"] >= 4)
                     or (s["coverage"] >= 4 and BREAKING_RX.search(text)))
        if not important:
            continue
        if any(l in sent for l in s["_links"]):
            continue
        age = (NOW - datetime.fromisoformat(s["published"])).total_seconds() / 3600
        if age > 4:
            continue
        payload = {
            "topic": topic or "dry-run",
            "title": f"{s['region']} · {s['category']} · {s['coverage']} outlets",
            "message": s["title"] + ("\n" + s["summary"][:160] if s["summary"] else ""),
            "click": s["link"],
            "priority": 4,
            "tags": ["newspaper"],
        }
        if dry:
            print("ALERT (dry run):", json.dumps(payload, ensure_ascii=False))
        else:
            try:
                req = urllib.request.Request("https://ntfy.sh", data=json.dumps(payload).encode("utf-8"),
                                             headers={"Content-Type": "application/json", "User-Agent": UA})
                urllib.request.urlopen(req, timeout=15).read()
            except Exception as e:
                print("ntfy failed:", e, file=sys.stderr)
                continue
        ts = NOW.timestamp()
        for l in s["_links"]:
            sent[l] = ts
        sent_now.append(s["id"])
    json.dump({"sent": sent}, open(state_path, "w", encoding="utf-8"))
    return sent_now

# --------------------------------------------------------------------------

def main():
    feeds = json.load(open(os.path.join(HERE, "feeds.json"), encoding="utf-8"))
    items, health = [], []
    with ThreadPoolExecutor(max_workers=12) as pool:
        for feed, got, err in pool.map(fetch_feed, feeds):
            health.append({"id": feed["id"], "outlet": feed["outlet"], "ok": err is None, "items": len(got),
                           **({"error": err} if err else {})})
            items.extend(got)
    ok = sum(1 for h in health if h["ok"])
    print(f"feeds ok: {ok}/{len(health)}  articles kept: {len(items)}")
    for h in health:
        if not h["ok"]:
            print("  FAILED", h["id"], h.get("error"))

    # Never wipe a good news.json because of a network hiccup.
    out_path = os.path.join(HERE, "news.json")
    if ok == 0 or len(items) < 10:
        print("Not enough data fetched; keeping the previous news.json")
        previous = load_json(out_path, None)
        if previous is not None:
            previous["health"] = health
            json.dump(previous, open(out_path, "w", encoding="utf-8"), ensure_ascii=False)
        return 0

    stories = [build_story(g) for g in group_items(items)]
    stories.sort(key=lambda s: s["score"], reverse=True)
    stories = stories[:MAX_STORIES]
    alerted = send_alerts(stories)
    for s in stories:
        s.pop("_links", None)
    out = {"updated": NOW.isoformat(timespec="seconds"), "count": len(stories), "alerted": alerted,
           "health": health, "stories": stories}
    json.dump(out, open(out_path, "w", encoding="utf-8"), ensure_ascii=False, separators=(",", ":"))
    print(f"wrote news.json with {len(stories)} stories; alerts sent: {len(alerted)}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
