import csv
import io
from datetime import datetime, timedelta, timezone
from pathlib import Path

import requests
import yaml
from icalendar import Calendar, Event

# Your ranking source gist
GIST_ID = "6eb933355b5cb8d31ef1abcb3c3e1206"

# CCFDDL source
CCFDDL_API = "https://api.github.com/repos/ccfddl/ccf-deadlines/contents/conference"

OUT_DIR = Path("site")
OUT_DIR.mkdir(exist_ok=True)

SESSION = requests.Session()
SESSION.headers.update({
    "Accept": "application/vnd.github+json",
    "User-Agent": "cs-deadline-calendar"
})

ALIASES = {
    "atc": "usenix",
    "bigdata": "bigdataconf",
    "neurips": "nips",
    "pact": "ieeepact",
    "ubicomp": "huc",
}

def get_json(url: str):
    r = SESSION.get(url, timeout=30)
    r.raise_for_status()
    return r.json()


def get_text(url: str):
    r = SESSION.get(url, timeout=30)
    r.raise_for_status()
    return r.text


def load_yaml(path: str):
    with open(path, "r", encoding="utf-8") as f:
        return yaml.safe_load(f)


def normalize_conf_name(x: str) -> str:
    return x.strip().lower()


def parse_timezone(tz_str: str):
    # CCFDDL supports UTC±X and AoE
    if not tz_str or tz_str == "AoE":
        return timezone(timedelta(hours=-12))
    if tz_str == "UTC":
        return timezone.utc
    if tz_str.startswith("UTC"):
        s = tz_str[3:]
        sign = 1
        if s.startswith("+"):
            s = s[1:]
        elif s.startswith("-"):
            sign = -1
            s = s[1:]
        hours = int(s)
        return timezone(timedelta(hours=sign * hours))
    return timezone.utc


def load_interested():
    cfg = load_yaml("interested.yml") or {}
    raw = cfg.get("interested_conferences", [])
    normalized = set()

    for x in raw:
        key = normalize_conf_name(x)
        key = ALIASES.get(key, key)
        normalized.add(key)

    return normalized


def get_gist_csv_raw_url() -> str:
    gist = get_json(f"https://api.github.com/gists/{GIST_ID}")
    files = gist.get("files", {})
    for _, meta in files.items():
        filename = meta.get("filename", "")
        if filename.endswith(".csv"):
            return meta["raw_url"]
    raise RuntimeError("Could not find CSV file in gist.")


def load_rank_sets():
    """
    Build:
      A = conferences marked '최우수'
      B = conferences marked '우수'
    using the gist's DBLP Key column.
    """
    raw_url = get_gist_csv_raw_url()
    csv_text = get_text(raw_url)

    A = set()
    B = set()

    reader = csv.DictReader(io.StringIO(csv_text))
    for row in reader:
        rank = (row.get("한국정보과학회 (2024)") or "").strip()
        dblp_key = (row.get("DBLP Key") or "").strip()

        if not dblp_key:
            continue

        # Examples:
        # conf/aaai -> aaai
        # conf/ifaamas -> ifaamas
        # conf/www -> www
        conf_key = dblp_key.split("/")[-1].strip().lower()

        if rank == "최우수":
            A.add(conf_key)
        elif rank == "우수":
            B.add(conf_key)

    return A, B


def iter_ccfddl_yaml_urls():
    top = get_json(CCFDDL_API)
    for category in top:
        if category["type"] != "dir":
            continue
        children = get_json(category["url"])
        for item in children:
            if item["type"] == "file" and item["name"].endswith(".yml"):
                yield item["download_url"]


def load_ccfddl_entries():
    """
    Returns:
      dict[dblp_suffix] -> list[conference_record]
    """
    out = {}
    for url in iter_ccfddl_yaml_urls():
        text = get_text(url)
        records = yaml.safe_load(text)
        if not isinstance(records, list):
            continue

        for rec in records:
            dblp = (rec.get("dblp") or "").strip().lower()
            if not dblp:
                continue
            out.setdefault(dblp, []).append(rec)

    return out


def build_calendar(calendar_name: str, conference_keys, ccfddl_by_dblp):
    cal = Calendar()
    cal.add("prodid", f"-//{calendar_name}//")
    cal.add("version", "2.0")
    cal.add("X-WR-CALNAME", calendar_name)
    cal.add("X-WR-CALDESC", f"{calendar_name} from CCFDDL + 정보과학회 ranking")
    now = datetime.now(timezone.utc)

    seen = set()

    for dblp in sorted(conference_keys):
        for conf in ccfddl_by_dblp.get(dblp, []):
            title = conf.get("title", dblp.upper())
            desc = conf.get("description", "")

            for edition in conf.get("confs", []):
                year = edition.get("year")
                link = edition.get("link", "")
                tzinfo = parse_timezone(edition.get("timezone", "AoE"))

                for tl in edition.get("timeline", []):
                    ddl = tl.get("deadline")
                    comment = tl.get("comment", "Deadline")

                    if not ddl or ddl == "TBD":
                        continue

                    dt = datetime.strptime(ddl, "%Y-%m-%d %H:%M:%S").replace(tzinfo=tzinfo)

                    # only future deadlines
                    if dt.astimezone(timezone.utc) < now:
                        continue

                    uid = f"{dblp}-{year}-{ddl}-{comment}"
                    if uid in seen:
                        continue
                    seen.add(uid)

                    ev = Event()
                    ev.add("summary", f"{title} {year} — {comment}")
                    ev.add("dtstart", dt)
                    ev.add("dtend", dt + timedelta(hours=1))
                    ev.add("dtstamp", now)
                    ev.add("uid", uid + "@cs-deadline-calendar")
                    ev.add(
                        "description",
                        f"{title} {year}\n"
                        f"{comment}\n"
                        f"{desc}\n"
                        f"DBLP key: {dblp}\n"
                        f"{link}"
                    )
                    if link:
                        ev.add("url", link)

                    cal.add_component(ev)

    return cal


def write_index():
    html = """<!doctype html>
<html>
<head>
  <meta charset="utf-8">
  <title>CS Deadline Calendars</title>
</head>
<body>
  <h1>CS Deadline Calendars</h1>
  <ul>
    <li><a href="./A.ics">A.ics</a></li>
    <li><a href="./B.ics">B.ics</a></li>
  </ul>
</body>
</html>
"""
    (OUT_DIR / "index.html").write_text(html, encoding="utf-8")
    (OUT_DIR / ".nojekyll").write_text("", encoding="utf-8")


def main():
    interested = load_interested()
    rank_A, rank_B = load_rank_sets()
    ccfddl_by_dblp = load_ccfddl_entries()

    selected_A = interested & rank_A
    selected_B = interested & rank_B

    # Helpful diagnostics
    ranked_union = rank_A | rank_B
    unranked = sorted(interested - ranked_union)
    missing_in_ccfddl = sorted(
        (selected_A | selected_B) - set(ccfddl_by_dblp.keys())
    )

    print("Selected for A:", sorted(selected_A))
    print("Selected for B:", sorted(selected_B))
    print("Not found in gist A/B ranking:", unranked)
    print("Ranked but not found in CCFDDL:", missing_in_ccfddl)

    cal_A = build_calendar("My Deadlines - A", selected_A, ccfddl_by_dblp)
    cal_B = build_calendar("My Deadlines - B", selected_B, ccfddl_by_dblp)

    with open(OUT_DIR / "A.ics", "wb") as f:
        f.write(cal_A.to_ical())

    with open(OUT_DIR / "B.ics", "wb") as f:
        f.write(cal_B.to_ical())

    write_index()

    print("Wrote site/A.ics")
    print("Wrote site/B.ics")


if __name__ == "__main__":
    main()
