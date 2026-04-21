import sys
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Dict, List, Optional, Set, Tuple

import requests
import yaml
from icalendar import Calendar, Event

# ----------------------------
# Config
# ----------------------------

CCFDDL_API = "https://api.github.com/repos/ccfddl/ccf-deadlines/contents/conference"

OUT_DIR = Path("site")
OUT_DIR.mkdir(exist_ok=True)

MAX_WORKERS = 8

SESSION = requests.Session()
SESSION.headers.update(
    {
        "Accept": "application/vnd.github+json",
        "User-Agent": "cs-deadline-calendar",
    }
)

# ----------------------------
# Aliases
# ----------------------------

# User shorthand -> CCFDDL/ranking key
RANK_ALIASES: Dict[str, str] = {
    "atc": "usenix",
    "bigdata": "bigdataconf",
    "neurips": "nips",
    "pact": "ieeepact",
    "ubicomp": "huc",
}

# Ranking/manual key -> CCFDDL key
CCFDDL_ALIASES: Dict[str, str] = {
    "ieeepact": "pact",
}

# ----------------------------
# HTTP helpers
# ----------------------------

def get_json(url: str):
    r = SESSION.get(url, timeout=30)
    r.raise_for_status()
    return r.json()


def get_text(url: str) -> str:
    r = SESSION.get(url, timeout=30)
    r.raise_for_status()
    return r.text


# ----------------------------
# Local file helpers
# ----------------------------

def load_yaml(path: str):
    with open(path, "r", encoding="utf-8") as f:
        return yaml.safe_load(f)


def normalize_conf_name(name: str) -> str:
    return name.strip().lower()


# ----------------------------
# Interested list
# ----------------------------

def normalize_conf_list(items: List[object]) -> Set[str]:
    out: Set[str] = set()
    for item in items or []:
        key = normalize_conf_name(str(item))
        key = RANK_ALIASES.get(key, key)
        out.add(key)
    return out


def load_interested() -> Tuple[str, str, Set[str], Set[str]]:
    cfg = load_yaml("interested.yml") or {}

    calendar_names = cfg.get("calendar_names", {})
    calendar_name_a = calendar_names.get("A", "conf_A")
    calendar_name_b = calendar_names.get("B", "conf_B")

    selected_a = normalize_conf_list(cfg.get("A_conferences", []))
    selected_b = normalize_conf_list(cfg.get("B_conferences", []))

    return calendar_name_a, calendar_name_b, selected_a, selected_b


# ----------------------------
# Key remapping
# ----------------------------

def remap_for_ccfddl(keys: Set[str]) -> Set[str]:
    return {CCFDDL_ALIASES.get(k, k) for k in keys}


# ----------------------------
# CCFDDL loader
# ----------------------------

def list_ccfddl_files() -> Dict[str, str]:
    """
    Returns:
      dict[filename_without_ext] -> download_url
    """
    out: Dict[str, str] = {}

    top = get_json(CCFDDL_API)
    for category in top:
        if category.get("type") != "dir":
            continue

        children = get_json(category["url"])
        for item in children:
            name = str(item.get("name", ""))
            if item.get("type") == "file" and name.endswith(".yml"):
                stem = name[:-4].lower()
                out[stem] = item["download_url"]

    return out


def fetch_ccfddl_record(url: str) -> List[dict]:
    records = yaml.safe_load(get_text(url))
    if not isinstance(records, list):
        return []
    return records


def add_records_to_index(out: Dict[str, List[dict]], records: List[dict]) -> None:
    for rec in records:
        dblp = str(rec.get("dblp", "")).strip().lower()
        if not dblp:
            continue
        out.setdefault(dblp, []).append(rec)


def fetch_urls_parallel(urls: List[str], max_workers: int = 8) -> List[List[dict]]:
    results: List[List[dict]] = []

    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = {executor.submit(fetch_ccfddl_record, url): url for url in urls}

        for future in as_completed(futures):
            url = futures[future]
            try:
                results.append(future.result())
            except Exception as exc:
                print(f"Failed to fetch {url}: {exc}", file=sys.stderr)

    return results


def load_ccfddl_entries_for_keys(requested_keys: Set[str]) -> Dict[str, List[dict]]:
    """
    Fast path:
      - fetch files whose filename stem matches requested keys

    Fallback:
      - if some requested dblp keys are still unresolved, scan the remaining YAML files
        and match by internal 'dblp' field instead of filename.
    """
    file_map = list_ccfddl_files()
    out: Dict[str, List[dict]] = {}

    # Fast path
    direct_urls: List[str] = []
    direct_stems: Set[str] = set()

    for key in sorted(requested_keys):
        if key in file_map:
            direct_urls.append(file_map[key])
            direct_stems.add(key)

    for records in fetch_urls_parallel(direct_urls, max_workers=MAX_WORKERS):
        add_records_to_index(out, records)

    unresolved = requested_keys - set(out.keys())
    if not unresolved:
        return out

    print("Falling back to content scan for:", sorted(unresolved))

    remaining_urls: List[str] = [
        url for stem, url in file_map.items() if stem not in direct_stems
    ]

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = {executor.submit(fetch_ccfddl_record, url): url for url in remaining_urls}

        for future in as_completed(futures):
            url = futures[future]
            try:
                records = future.result()
            except Exception as exc:
                print(f"Failed to fetch {url}: {exc}", file=sys.stderr)
                continue

            matched_any = False
            for rec in records:
                dblp = str(rec.get("dblp", "")).strip().lower()
                if dblp and dblp in unresolved:
                    out.setdefault(dblp, []).append(rec)
                    matched_any = True

            if matched_any:
                unresolved = requested_keys - set(out.keys())
                if not unresolved:
                    break

    if unresolved:
        print("Still not found in CCFDDL after fallback scan:", sorted(unresolved))

    return out


# ----------------------------
# Timezone parsing
# ----------------------------

def parse_timezone(tz_str: Optional[str]):
    if not tz_str or tz_str == "AoE":
        return timezone(timedelta(hours=-12))

    if tz_str in ("UTC", "UTC+0"):
        return timezone.utc

    if tz_str.startswith("UTC"):
        s = tz_str[3:]
        sign = 1
        if s.startswith("+"):
            s = s[1:]
        elif s.startswith("-"):
            sign = -1
            s = s[1:]

        try:
            hours = int(s)
            return timezone(timedelta(hours=sign * hours))
        except ValueError:
            return timezone.utc

    return timezone.utc


# ----------------------------
# Calendar generation
# ----------------------------

def make_event_uid(dblp: str, year: object, deadline: str, comment: str) -> str:
    safe_comment = str(comment).replace("\n", " ").strip()
    return f"{dblp}-{year}-{deadline}-{safe_comment}@cs-deadline-calendar"


def build_calendar(
    calendar_name: str,
    conference_keys: Set[str],
    ccfddl_by_dblp: Dict[str, List[dict]],
) -> Calendar:
    cal = Calendar()
    cal.add("prodid", f"-//{calendar_name}//")
    cal.add("version", "2.0")
    cal.add("X-WR-CALNAME", calendar_name)
    cal.add("X-WR-CALDESC", calendar_name)

    now = datetime.now(timezone.utc)
    seen_uids: Set[str] = set()

    for dblp in sorted(conference_keys):
        records = ccfddl_by_dblp.get(dblp, [])
        for conf in records:
            title = conf.get("title", dblp.upper())
            description = conf.get("description", "")

            for edition in conf.get("confs", []):
                year = edition.get("year")
                link = edition.get("link", "")
                tzinfo = parse_timezone(edition.get("timezone", "AoE"))

                for tl in edition.get("timeline", []):
                    deadline = tl.get("deadline")
                    comment = tl.get("comment", "Deadline")

                    if not deadline or deadline == "TBD":
                        continue

                    try:
                        dt = datetime.strptime(
                            deadline, "%Y-%m-%d %H:%M:%S"
                        ).replace(tzinfo=tzinfo)
                    except ValueError:
                        continue

                    if dt.astimezone(timezone.utc) < now:
                        continue

                    uid = make_event_uid(dblp, year, deadline, comment)
                    if uid in seen_uids:
                        continue
                    seen_uids.add(uid)

                    ev = Event()
                    ev.add("uid", uid)
                    ev.add("dtstamp", now)
                    ev.add("summary", f"{title} {year} — {comment}")
                    ev.add("dtstart", dt)
                    ev.add("dtend", dt + timedelta(hours=1))

                    desc_lines = [
                        f"{title} {year}",
                        str(comment),
                        str(description),
                        f"DBLP key: {dblp}",
                    ]
                    if link:
                        desc_lines.append(str(link))

                    ev.add("description", "\n".join(desc_lines))

                    if link:
                        ev.add("url", link)

                    cal.add_component(ev)

    return cal


# ----------------------------
# Output helpers
# ----------------------------

def write_index() -> None:
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


# ----------------------------
# Main
# ----------------------------

def main() -> int:
    try:
        calendar_name_a, calendar_name_b, selected_a_rank, selected_b_rank = load_interested()

        selected_a_ccfddl = remap_for_ccfddl(selected_a_rank)
        selected_b_ccfddl = remap_for_ccfddl(selected_b_rank)

        needed_keys = selected_a_ccfddl | selected_b_ccfddl
        ccfddl_by_dblp = load_ccfddl_entries_for_keys(needed_keys)

        missing_in_ccfddl = sorted(
            (selected_a_ccfddl | selected_b_ccfddl) - set(ccfddl_by_dblp.keys())
        )

        print("Selected for A:", sorted(selected_a_rank))
        print("Selected for B:", sorted(selected_b_rank))
        print("Selected for A (CCFDDL keys):", sorted(selected_a_ccfddl))
        print("Selected for B (CCFDDL keys):", sorted(selected_b_ccfddl))
        print("Ranked but not found in CCFDDL:", missing_in_ccfddl)

        cal_a = build_calendar(calendar_name_a, selected_a_ccfddl, ccfddl_by_dblp)
        cal_b = build_calendar(calendar_name_b, selected_b_ccfddl, ccfddl_by_dblp)

        with open(OUT_DIR / "A.ics", "wb") as f:
            f.write(cal_a.to_ical())

        with open(OUT_DIR / "B.ics", "wb") as f:
            f.write(cal_b.to_ical())

        write_index()

        print("Wrote site/A.ics")
        print("Wrote site/B.ics")
        print("Wrote site/index.html")
        return 0

    except Exception as e:
        print(f"ERROR: {e}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
