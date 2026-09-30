"""Append a build record to gh-pages builds.json and regenerate index.html.

The page is the pick-a-tag surface for the agentenv deploy: each row carries
the exact `image.tag` value to set in infra/agentenv/chart/values.yaml.
"""

from __future__ import annotations

import argparse
import datetime
import html
import json
import pathlib

COMPONENTS = ("runtime", "gateway", "scheduler")

PAGE = """<!doctype html>
<html><head><meta charset="utf-8"><title>agentenv builds</title>
<style>
 body {{ font: 14px/1.4 -apple-system, sans-serif; margin: 2rem; }}
 table {{ border-collapse: collapse; width: 100%; }}
 th, td {{ border: 1px solid #ccc; padding: 6px 10px; text-align: left; }}
 th {{ background: #f4f4f4; }}
 code {{ background: #f4f4f4; padding: 1px 4px; }}
 .digest {{ font-size: 11px; color: #666; }}
</style></head><body>
<h1>agentenv image builds</h1>
<p>Deploy a build: set <code>image.tag: &lt;tag&gt;</code> in
<code>infra/agentenv/chart/values.yaml</code> (registry
<code>{registry}</code>), helm upgrade, then roll node pods per the OnDelete
runbook. Newest first.</p>
<table>
<tr><th>tag</th><th>date (UTC)</th><th>author</th><th>commit message</th><th>images</th></tr>
{rows}
</table></body></html>
"""

ROW = """<tr><td><code>{tag}</code></td><td>{date}</td><td>{author}</td>
<td>{message}</td><td>{images}</td></tr>"""


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--pages-dir", required=True)
    ap.add_argument("--meta-dir", required=True)
    ap.add_argument("--registry", required=True)
    ap.add_argument("--sha", required=True)
    ap.add_argument("--author", required=True)
    ap.add_argument("--message", required=True)
    args = ap.parse_args()

    pages = pathlib.Path(args.pages_dir)
    meta = pathlib.Path(args.meta_dir)
    builds_path = pages / "builds.json"
    builds = json.loads(builds_path.read_text()) if builds_path.exists() else []

    digests = {}
    for comp in COMPONENTS:
        f = meta / f"{comp}.digest"
        digests[comp] = f.read_text().strip() if f.exists() else ""

    builds = [b for b in builds if b["tag"] != args.sha]
    builds.insert(0, {
        "tag": args.sha,
        "date": datetime.datetime.now(datetime.timezone.utc).strftime("%Y-%m-%d %H:%M"),
        "author": args.author,
        "message": args.message,
        "registry": args.registry,
        "digests": digests,
    })
    builds_path.write_text(json.dumps(builds, indent=1))

    rows = []
    for b in builds:
        images = "<br>".join(
            f"<code>agentenv-{c}:{b['tag']}</code> "
            f"<span class=digest>{html.escape(b['digests'].get(c, ''))}</span>"
            for c in COMPONENTS
        )
        rows.append(ROW.format(
            tag=html.escape(b["tag"]),
            date=html.escape(b["date"]),
            author=html.escape(b["author"]),
            message=html.escape(b["message"]),
            images=images,
        ))
    (pages / "index.html").write_text(
        PAGE.format(registry=html.escape(args.registry), rows="\n".join(rows))
    )
    print(f"recorded build {args.sha}; total builds: {len(builds)}")


if __name__ == "__main__":
    main()
