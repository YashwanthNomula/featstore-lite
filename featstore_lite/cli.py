"""CLI: `fstore` — tiny feature store with point-in-time joins."""
from __future__ import annotations

import argparse
import csv
import json
import sys

from .demo import run_demo
from .join import compare_with_naive, naive_join, point_in_time_join, read_events_csv
from .report import report_html, report_json, report_terminal
from .schema import DTYPES, format_ts
from .store import FeatureStore


def cmd_init(args) -> int:
    store = FeatureStore(args.dir)
    store.init()
    print(f"initialized feature store at {store.path}")
    return 0


def cmd_register(args) -> int:
    store = FeatureStore(args.dir)
    if not __import__("os").path.isdir(args.dir):
        print(f"error: no store at {args.dir} (run `fstore init` first)", file=sys.stderr)
        return 1
    try:
        from .schema import parse_ts as _parse_ts
        valid_from = _parse_ts(args.valid_from) if args.valid_from else None
        fv = store.register(
            args.feature,
            entity=args.entity,
            dtype=args.dtype,
            ttl_seconds=args.ttl,
            description=args.description or "",
            now=valid_from,
        )
    except (ValueError, KeyError) as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 1
    print(f"registered {args.feature} {fv.version} "
          f"(entity={args.entity}, dtype={args.dtype}, valid_from={format_ts(fv.valid_from)})")
    return 0


def cmd_ingest(args) -> int:
    store = FeatureStore(args.dir)
    try:
        with open(args.csv, newline="") as fh:
            rows = list(csv.DictReader(fh))
    except OSError as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 1
    needed = {"entity_id", "event_ts", "value"}
    if rows and not needed.issubset(rows[0].keys()):
        print(f"error: csv must have columns {sorted(needed)}", file=sys.stderr)
        return 1
    ok, failed = 0, 0
    for r in rows:
        try:
            store.ingest(args.feature, r["entity_id"], r["event_ts"], r["value"],
                         version=args.version)
            ok += 1
        except (KeyError, ValueError) as exc:
            print(f"skip row {r}: {exc}", file=sys.stderr)
            failed += 1
    print(f"ingested {ok} rows into {args.feature}" + (f" ({failed} skipped)" if failed else ""))
    return 0 if failed == 0 else 1


def cmd_latest(args) -> int:
    store = FeatureStore(args.dir)
    try:
        mat = store.latest(args.feature, args.version)
    except KeyError as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 1
    if args.json:
        print(json.dumps({e: {"value": v, "ts": format_ts(ts), "version": ver}
                          for e, (v, ts, ver) in mat.items()}, indent=2, default=str))
        return 0
    print(f"{args.feature}: {len(mat)} entities materialized")
    for entity in sorted(mat):
        val, ts, ver = mat[entity]
        print(f"  {entity:<16} {val!s:<12} as of {format_ts(ts)}  ({ver})")
    return 0


def cmd_join(args) -> int:
    store = FeatureStore(args.dir)
    try:
        events = read_events_csv(args.events)
    except OSError as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 1
    specs = [s.strip() for s in args.features.split(",") if s.strip()]
    try:
        asof_rows, stats = point_in_time_join(store, events, specs)
    except KeyError as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 1

    leak = None
    if args.compare_naive:
        naive_rows = naive_join(store, events, specs)
        leak = compare_with_naive(asof_rows, naive_rows, specs)

    if args.json:
        print(report_json(asof_rows, stats, leak))
    else:
        print(report_terminal(asof_rows, stats, specs, leak))

    if args.out:
        cols = [c for c in events[0].keys()] + specs if events else specs
        with open(args.out, "w", newline="") as fh:
            w = csv.DictWriter(fh, fieldnames=cols, extrasaction="ignore")
            w.writeheader()
            w.writerows(asof_rows)
        print(f"\nwrote {args.out}")
    if args.html:
        with open(args.html, "w") as fh:
            fh.write(report_html(asof_rows, stats, specs, leak))
        print(f"wrote {args.html}")
    if leak and leak["leaked_cells"]:
        return 3  # leaks found: CI can gate on this
    return 0


def cmd_demo(args) -> int:
    result = run_demo(args.out)
    print(result["terminal"])
    print(f"\nwrote {result['joined_csv']} and {result['html']}")
    print(f"store: {result['store_dir']}  (re-run the join with "
          f"`fstore join --dir {result['store_dir']} --events {result['events_csv']} "
          f"--features support_tickets_30d,login_count_7d,session_p95_ms --compare-naive`)")
    return 0


def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(prog="fstore",
                                description="featstore-lite: versioned features, point-in-time joins")
    sub = p.add_subparsers(dest="cmd", required=True)

    i = sub.add_parser("init", help="create a new feature store directory")
    i.add_argument("--dir", default="featstore", help="store directory")
    i.set_defaults(func=cmd_init)

    r = sub.add_parser("register", help="register a feature (re-register to add a version)")
    r.add_argument("--dir", default="featstore")
    r.add_argument("--feature", required=True)
    r.add_argument("--entity", default="user")
    r.add_argument("--dtype", default="float", choices=DTYPES)
    r.add_argument("--ttl", type=float, default=None, help="time-to-live in seconds")
    r.add_argument("--description", default="")
    r.add_argument("--valid-from", default=None,
                   help="version start date (ISO or epoch); default: now. "
                        "Backdate it when registering a feature that already existed.")
    r.set_defaults(func=cmd_register)

    g = sub.add_parser("ingest", help="ingest feature rows from CSV (entity_id,event_ts,value)")
    g.add_argument("--dir", default="featstore")
    g.add_argument("--feature", required=True)
    g.add_argument("--csv", required=True)
    g.add_argument("--version", default=None, help="pin rows to a version (default: latest)")
    g.set_defaults(func=cmd_ingest)

    l = sub.add_parser("latest", help="materialized view: newest value per entity")
    l.add_argument("--dir", default="featstore")
    l.add_argument("--feature", required=True)
    l.add_argument("--version", default=None)
    l.add_argument("--json", action="store_true")
    l.set_defaults(func=cmd_latest)

    j = sub.add_parser("join", help="point-in-time join events CSV with features")
    j.add_argument("--dir", default="featstore")
    j.add_argument("--events", required=True, help="CSV with entity_id,event_ts (+ label cols)")
    j.add_argument("--features", required=True, help="comma list, e.g. f1,f2 or f1@v1,f2")
    j.add_argument("--out", default=None, help="write joined CSV here")
    j.add_argument("--html", default=None, help="write HTML report here")
    j.add_argument("--json", action="store_true")
    j.add_argument("--compare-naive", action="store_true",
                   help="diff against a latest-value join; exit 3 if leaks found")
    j.set_defaults(func=cmd_join)

    d = sub.add_parser("demo", help="run the scripted churn/leakage demo")
    d.add_argument("-o", "--out", default="featstore-demo", help="output directory")
    d.set_defaults(func=cmd_demo)

    return p


def main(argv=None) -> int:
    args = build_parser().parse_args(argv)
    return args.func(args)


if __name__ == "__main__":
    sys.exit(main())
