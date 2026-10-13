"""Terminal, JSON, and standalone-HTML reports for joins."""
from __future__ import annotations

import html
import json
from typing import Any, Dict, List

from .schema import format_ts


def report_terminal(rows: List[Dict[str, Any]], stats: Dict[str, Any], specs: List[str],
                    leak: Dict[str, Any] | None = None) -> str:
    lines = []
    lines.append("featstore-lite · point-in-time join")
    lines.append(f"events: {stats['events']}   features: {len(specs)}")
    lines.append("")
    lines.append("coverage per feature:")
    for name, fs in stats["features"].items():
        total = fs["filled"] + fs["null"]
        pct = 100.0 * fs["filled"] / total if total else 0.0
        ver = ", ".join(f"{v}:{c}" for v, c in sorted(fs["versions"].items()))
        extra = f"   stale>ttl: {fs['stale']}" if fs["stale"] else ""
        lines.append(f"  {name:<22} {fs['filled']:>3}/{total:<3} filled ({pct:5.1f}%)"
                     f"   versions[{ver}]{extra}")
    if leak:
        lines.append("")
        lines.append("leak check — point-in-time vs naive latest-value join:")
        lines.append(f"  {leak['leaked_cells']}/{leak['total_cells']} cells "
                     f"({100*leak['leak_rate']:.1f}%) contain future data in the naive join")
        for spec, n in leak["per_feature"].items():
            if n:
                lines.append(f"    {spec}: {n} leaked cells")
    lines.append("")
    lines.append("joined rows (feature columns only):")
    header = ["entity_id", "event_ts"] + specs
    widths = {h: max(len(h), max((len(str(r.get(h, ''))) for r in rows), default=0)) for h in header}
    lines.append("  " + "  ".join(h.ljust(widths[h]) for h in header))
    for r in rows:
        lines.append("  " + "  ".join(str(r.get(h, '')).ljust(widths[h]) for h in header))
    return "\n".join(lines)


def report_json(rows: List[Dict[str, Any]], stats: Dict[str, Any],
                leak: Dict[str, Any] | None = None) -> str:
    payload: Dict[str, Any] = {"stats": stats, "rows": rows}
    if leak:
        payload["leak_comparison"] = leak
    return json.dumps(payload, indent=2, default=str)


def report_html(rows: List[Dict[str, Any]], stats: Dict[str, Any], specs: List[str],
                leak: Dict[str, Any] | None = None) -> str:
    esc = html.escape

    def cell(v: Any) -> str:
        s = "" if v is None else str(v)
        cls = "null" if v is None else ""
        return f'<td class="{cls}">{esc(s)}</td>'

    feat_cards = []
    for name, fs in stats["features"].items():
        total = fs["filled"] + fs["null"]
        pct = 100.0 * fs["filled"] / total if total else 0.0
        ver = ", ".join(f"{v}: {c}" for v, c in sorted(fs["versions"].items()))
        feat_cards.append(
            f'<div class="card"><h3>{esc(name)}</h3>'
            f'<div class="big">{pct:.0f}%</div><div class="sub">coverage '
            f'({fs["filled"]}/{total})</div>'
            f'<div class="sub">versions: {esc(ver) or "—"}</div>'
            + (f'<div class="warn">{fs["stale"]} stale (TTL)</div>' if fs["stale"] else "")
            + "</div>"
        )

    leak_html = ""
    if leak:
        ex_rows = "".join(
            "<tr>" + "".join(cell(e[k]) for k in
                             ("entity_id", "event_ts", "feature", "point_in_time", "naive")) + "</tr>"
            for e in leak["examples"]
        )
        leak_html = (
            f'<h2>Leak check: naive join vs point-in-time</h2>'
            f'<p class="leak">{leak["leaked_cells"]}/{leak["total_cells"]} cells '
            f'({100*leak["leak_rate"]:.1f}%) in the naive join see the future.</p>'
            f'<table><tr><th>entity</th><th>event</th><th>feature</th>'
            f'<th>point-in-time ✓</th><th>naive ✗</th></tr>{ex_rows}</table>'
        )

    header = ["entity_id", "event_ts"] + specs
    body = "".join(
        "<tr>" + "".join(cell(r.get(h)) for h in header) + "</tr>" for r in rows
    )
    head = "".join(f"<th>{esc(h)}</th>" for h in header)

    return f"""<!doctype html>
<html><head><meta charset="utf-8"><title>featstore-lite join report</title>
<style>
body{{font-family:system-ui,sans-serif;max-width:1000px;margin:2rem auto;padding:0 1rem;color:#1a1a1a}}
.cards{{display:flex;gap:1rem;flex-wrap:wrap}}
.card{{border:1px solid #ddd;border-radius:8px;padding:1rem;min-width:180px}}
.big{{font-size:2rem;font-weight:700}}.sub{{color:#555;font-size:.9rem}}
.warn{{color:#a35400;font-size:.9rem}}
.leak{{background:#fde8e8;border:1px solid #f3b3b3;border-radius:8px;padding:.75rem 1rem}}
table{{border-collapse:collapse;width:100%;margin-top:1rem}}
th,td{{border:1px solid #ddd;padding:.4rem .6rem;text-align:left;font-size:.9rem}}
th{{background:#f5f5f5}}.null{{color:#999;font-style:italic}}
</style></head><body>
<h1>featstore-lite · point-in-time join</h1>
<p>{stats['events']} events × {len(specs)} features</p>
<div class="cards">{''.join(feat_cards)}</div>
{leak_html}
<h2>Joined rows</h2>
<table><tr>{head}</tr>{body}</table>
</body></html>"""
