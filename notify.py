"""
Run report (HTML + plain text) and e-mail.

    settings:
      report_path: /Workspace/Users/me@corp.com/parity_reports   # optional: writes parity_<run_id>.html there
      email:                                                      # optional: no block = no e-mail
        to: "a@corp.com, b@corp.com"
        send_on: always            # always | issues (any WARN/FAIL/ERROR) | failure (any FAIL/ERROR) | never
        smtp_server: mailrelay.ftr.com
        smtp_port: 25
        sender: noreply@ftr.databricks.com
        starttls: true
        subject_prefix: "[ETL parity]"

Report layout:
  1. run summary   - total tables, tables passed; per validation: tables passed (incl. within tolerance) of tables run
  2. summary table - one row per table, one column per validation: PASS / FAIL + aggregated checks
  3. details       - the checks that did not pass, per table
A validation passes when none of its checks FAILs (WARN = within tolerance counts as passed);
a table passes when all its validations pass.
A failed e-mail never fails the run: the error is printed and recorded on the ParityRun (email_error).
"""
from __future__ import annotations

import html
import os
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from typing import Optional

SEVERITY = {"PASS": 0, "INFO": 0, "SKIP": 1, "WARN": 2, "FAIL": 3, "ERROR": 4}
COLORS = {"PASS": "#1a7f37", "WARN": "#9a6700", "FAIL": "#cf222e", "ERROR": "#8250df", "SKIP": "#57606a", "-": "#8c959f"}
BG = {"PASS": "#dafbe1", "WARN": "#fff8c5", "FAIL": "#ffebe9", "ERROR": "#fbefff", "SKIP": "#f6f8fa", "-": "#f6f8fa"}
EMAIL_KEYS = {"to", "send_on", "smtp_server", "smtp_port", "sender", "starttls", "subject_prefix"}
VALIDATIONS = [("schema", "Schema"), ("row_count", "Row count"), ("row_hash", "Row hash"),
               ("column_profiles", "Column profiles"), ("column_distributions", "Distributions"),
               ("incremental", "Incremental")]
LABEL = {"PASS": "passed", "WARN": "within tolerance", "FAIL": "failed", "ERROR": "error", "SKIP": "skipped"}


# ------------------------------------------------------------------ report content
def build_report(rec, duration_s: float, config_path: str, max_issues: int = 6) -> dict:
    """Everything the report needs, from the Recorder of a finished run."""
    tables = rec.nested_rows()
    facts: dict = {}
    for r in rec.rows:                         # (run_id, run_ts, env, table, validation, scope, check, status, src, tgt, detail)
        t, check = r[3], r[6]
        if check == "row_count":
            facts.setdefault(t, {})["rows"] = (r[8], r[9])
        elif check == "rows_compared":
            facts.setdefault(t, {})["compared"] = (r[8], r[9], r[5])
    out = []
    for t in sorted(tables, key=lambda x: (-SEVERITY.get(x["status"], 0), x["table_name"])):
        f = facts.get(t["table_name"], {})
        bad = [v for v in t["details"] if SEVERITY.get(v["status"], 0) >= SEVERITY["WARN"]]
        cells = {}                             # validation -> aggregated checks for this table
        for v in t["details"]:
            base = v["validation"].split(" [")[0]
            scope = v["validation"][len(base):].strip(" []")
            cells[base] = dict(status=_verdict(v), scope=scope, checks=v["checks"],
                               **{k: v[k] for k in ("pass", "warn", "fail", "skip", "error")})
        verdicts = [c["status"] for c in cells.values()]
        status = "ERROR" if "ERROR" in verdicts else "FAIL" if "FAIL" in verdicts else "PASS"
        out.append(dict(
            table=t["table_name"], status=status, cells=cells,
            validations=len(t["details"]), failed=t["validations_failed"],
            rows=f.get("rows"), compared=f.get("compared"), finding=_main_finding(bad),
            issues=[dict(validation=v["validation"], status=v["status"], details=v["details"],
                         checks=list(v["issues"])[:max_issues], more=max(0, len(v["issues"]) - max_issues))
                    for v in bad]))
    # per validation: how many tables passed / were within tolerance / failed / ... / did not run it
    by_val = []
    for key, label in VALIDATIONS:
        cells = [t["cells"][key] for t in out if key in t["cells"]]
        by_val.append(dict(key=key, label=label, ran=len(cells), not_run=len(out) - len(cells),
                           passed=sum(1 for c in cells if c["status"] == "PASS"),
                           within=sum(1 for c in cells if c["status"] == "PASS" and c["warn"]),
                           failed=sum(1 for c in cells if c["status"] in ("FAIL", "ERROR"))))
    counts = {s: sum(1 for t in out if t["status"] == s) for s in ("PASS", "FAIL", "ERROR")}
    overall = "ERROR" if counts["ERROR"] else "FAIL" if counts["FAIL"] else "PASS"
    return dict(run_id=rec.run_id, run_ts=str(rec.run_ts), env=rec.env or "-", config=config_path,
                duration=_dur(duration_s), tables=out, counts=counts, overall=overall, by_validation=by_val,
                results_table=rec.results_table, mismatch_table=rec.mismatch_table)


def _verdict(v: dict) -> str:
    """A validation passes when none of its checks fails: PASS and WARN (within tolerance) checks both count as passed."""
    return "ERROR" if v["error"] else "FAIL" if v["fail"] else "PASS"


def _main_finding(bad: list) -> str:
    """The most telling problem first: run error, schema, row comparison, ... in validation order."""
    order = ["run", "schema", "row_hash", "incremental", "column_distributions", "column_profiles", "row_count"]
    rank = lambda v: next((i for i, o in enumerate(order) if v["validation"].startswith(o)), 99)
    worst = max((SEVERITY.get(v["status"], 0) for v in bad), default=0)
    top = sorted([v for v in bad if SEVERITY.get(v["status"], 0) == worst], key=rank)
    return f'{top[0]["validation"]}: {top[0]["details"]}' if top else ""


def _cut(text: str, n: int) -> str:
    return text if len(text) <= n else text[:n].rsplit(" ", 1)[0] + " ..."


def _dur(s: float) -> str:
    s = int(s)
    return f"{s // 3600}h {s % 3600 // 60}m" if s >= 3600 else f"{s // 60}m {s % 60}s" if s >= 60 else f"{s}s"


def _n(v) -> str:
    try:
        return f"{int(v):,}"
    except (TypeError, ValueError):
        return "-" if v is None else str(v)


def _rows_txt(t) -> tuple:
    rows = f"{_n(t['rows'][0])} vs {_n(t['rows'][1])}" if t["rows"] else "-"
    if t["compared"]:
        s, g, scope = t["compared"]
        kind = "sample" if "sample" in (scope or "") else "fingerprints" if "fingerprint" in (scope or "") else "all"
        comp = f"{_n(s)} vs {_n(g)} ({kind})"
    else:
        comp = "-"
    return rows, comp


def _checks_txt(c: Optional[dict]) -> str:
    """Aggregated checks of one table x validation, e.g. '9 checks: 5 pass, 4 within tolerance'."""
    if not c:
        return "not run"
    parts = [f"{c['pass']} pass"] + [f"{c[k]} {name}" for k, name in
                                     (("warn", "within tolerance"), ("fail", "fail"), ("error", "error"), ("skip", "skipped")) if c[k]]
    return f"{c['checks']} check{'s' if c['checks'] != 1 else ''}: " + ", ".join(parts)


def subject(rep: dict, prefix: str = "[ETL parity]") -> str:
    c, n = rep["counts"], len(rep["tables"])
    what = f"{c['PASS']} of {n} tables passed" + (f", {c['FAIL']} failed" if c["FAIL"] else "") + \
           (f", {c['ERROR']} errors" if c["ERROR"] else "")
    return f"{prefix} {rep['overall']} - {what} (run {rep['run_id']}, {rep['env']})"


# ------------------------------------------------------------------ plain text
def to_text(rep: dict) -> str:
    c, n = rep["counts"], len(rep["tables"])
    lines = [f"ETL parity run {rep['run_id']}  |  env {rep['env']}  |  {rep['run_ts']}  |  {rep['duration']}", "",
             "RUN SUMMARY",
             f"  Total tables: {n}",
             f"  Tables passed (every validation passed): {c['PASS']} of {n}"
             + (f"   failed: {c['FAIL']}" if c["FAIL"] else "") + (f"   errors: {c['ERROR']}" if c["ERROR"] else "")]
    for v in rep["by_validation"]:
        lines.append(f"  {v['label'] + ':':<17} {v['passed']} of {v['ran']} tables passed"
                     + (f" ({v['within']} within tolerance)" if v["within"] else "")
                     + (f", {v['failed']} failed" if v["failed"] else "")
                     + (f"   [not run for {v['not_run']}]" if v["not_run"] else ""))
    lines += ["", "SUMMARY TABLE (checks per table per validation)"]
    w = max([len(t["table"]) for t in rep["tables"]] + [5])
    for t in rep["tables"]:
        rows, comp = _rows_txt(t)
        lines.append(f"{t['table'].ljust(w)}  {t['status']:<5}  rows {rows} | compared {comp}")
        for key, label in VALIDATIONS:
            cell = t["cells"].get(key)
            lines.append(f"{'':<{w}}    {label:<16} {(cell['status'] if cell else '-'):<5}  {_checks_txt(cell)}")
    for t in rep["tables"]:
        if not t["issues"]:
            continue
        lines += ["", f"{t['table']}  {t['status']}"]
        for v in t["issues"]:
            lines.append(f"  {v['status']:<5} {v['validation']}: {v['details']}")
            for i in v["checks"]:
                lines.append(f"        {i['status']:<5} {i['check']}: {i['detail'][:160]}")
            if v["more"]:
                lines.append(f"        ... {v['more']} more")
    lines += ["", "PASS = no check failed (checks within tolerance count as passed).",
              f"Details: {rep['results_table']} / {rep['mismatch_table']}  WHERE run_id = '{rep['run_id']}'"]
    return "\n".join(lines)


# ------------------------------------------------------------------ HTML (inline styles: e-mail clients drop <style>)
def _badge(status: str, text: Optional[str] = None) -> str:
    return (f'<span style="display:inline-block;padding:1px 7px;border-radius:9px;font-size:11px;font-weight:600;'
            f'color:{COLORS.get(status, "#57606a")};background:{BG.get(status, "#f6f8fa")}">{text or status}</span>')


def to_html(rep: dict) -> str:
    e = html.escape
    c, n = rep["counts"], len(rep["tables"])
    font = "font-family:Segoe UI,Arial,Helvetica,sans-serif;"
    cell = "padding:6px 10px;border:1px solid #c8ccd1;vertical-align:top;font-size:13px;"
    head = "padding:6px 10px;border:1px solid #c8ccd1;background:#f3f4f6;text-align:left;font-size:12px;"
    h2 = "font-size:16px;font-weight:700;margin:22px 0 8px"

    # 1. run summary: plain table, counts are tables
    summary = [f'<tr><td style="{cell}">Total tables</td><td style="{cell}text-align:right"><b>{n}</b></td><td style="{cell}"></td></tr>',
               f'<tr><td style="{cell}">Tables passed (every validation passed)</td>'
               f'<td style="{cell}text-align:right"><b>{c["PASS"]}</b> of {n}</td>'
               f'<td style="{cell}">{c["FAIL"]} failed' + (f', {c["ERROR"]} errors' if c["ERROR"] else "") + '</td></tr>']
    for v in rep["by_validation"]:
        note = ", ".join(x for x in (f'{v["within"]} within tolerance' if v["within"] else "",
                                     f'{v["failed"]} failed' if v["failed"] else "",
                                     f'not run for {v["not_run"]}' if v["not_run"] else "") if x)
        summary.append(f'<tr><td style="{cell}">Passed {e(v["label"].lower())}</td>'
                       f'<td style="{cell}text-align:right"><b>{v["passed"]}</b> of {v["ran"]}</td>'
                       f'<td style="{cell}">{e(note)}</td></tr>')

    # 2. summary table: table x validation, aggregated checks; no colours
    cols = "".join(f'<th style="{head}">{e(label)}</th>' for _, label in VALIDATIONS)
    rows = []
    for t in rep["tables"]:
        r, comp = _rows_txt(t)
        tds = []
        for key, _ in VALIDATIONS:
            cl = t["cells"].get(key)
            if not cl:
                tds.append(f'<td style="{cell}">not run</td>')
                continue
            parts = [f'{cl["pass"]} pass'] + [f'{cl[k]} {name}' for k, name in
                                               (("warn", "within tol."), ("fail", "fail"), ("error", "error"),
                                                ("skip", "skipped")) if cl[k]]
            scope = f' <span style="font-size:11px;color:#57606a">({e(cl["scope"])})</span>' if cl["scope"] else ""
            n_ch = f'{cl["checks"]} check' + ("s" if cl["checks"] != 1 else "")
            tds.append(f'<td style="{cell}min-width:120px"><b>{cl["status"]}</b>{scope}<br>'
                       f'<span style="font-size:12px">{n_ch}: {e(", ".join(parts))}</span></td>')
        rows.append(f'<tr><td style="{cell}"><b>{e(t["table"])}</b><br><span style="font-size:11px;color:#57606a">'
                    f'rows {e(r)}<br>compared {e(comp)}</span></td><td style="{cell}"><b>{t["status"]}</b></td>'
                    f'{"".join(tds)}</tr>')

    # 3. details: checks that did not pass
    details = []
    for t in rep["tables"]:
        if not t["issues"]:
            continue
        items = []
        for v in t["issues"]:
            checks = "".join(f'<li style="margin:2px 0">{_badge(i["status"])} <b>{e(i["check"])}</b>: '
                             f'{e(i["detail"][:220])}</li>' for i in v["checks"])
            more = f'<li>... {v["more"]} more</li>' if v["more"] else ""
            items.append(f'<div style="margin:8px 0 4px"><b>{e(v["validation"])}</b> ({_verdict_word(v)}) '
                         f'<span style="color:#444">{e(v["details"][:220])}</span></div>'
                         f'<ul style="margin:0 0 6px 18px;padding:0;font-size:12px">{checks}{more}</ul>')
        details.append(f'<div style="margin:12px 0;padding:10px 14px;border:1px solid #c8ccd1">'
                       f'<div style="font-size:15px;font-weight:700">{e(t["table"])} - {t["status"]}</div>'
                       f'{"".join(items)}</div>')
    sql = (f"SELECT table_name, validation, check_name, status, src_value, tgt_value, detail\n"
           f"FROM {rep['results_table']}\nWHERE run_id = '{rep['run_id']}' AND status NOT IN ('PASS','INFO')\n"
           f"ORDER BY table_name, validation;")
    return f"""<!doctype html><html><head><meta charset="utf-8"><title>ETL parity {e(rep['run_id'])}</title></head>
<body style="margin:0;padding:20px;background:#ffffff;color:#1f2328;{font}">
<div style="max-width:1200px">
  <div style="font-size:20px;font-weight:700">ETL parity report - {rep['overall']}</div>
  <div style="color:#57606a;font-size:13px;margin:4px 0 6px">run <b>{e(rep['run_id'])}</b> &middot; env <b>{e(rep['env'])}</b>
    &middot; {e(rep['run_ts'])} &middot; {e(rep['duration'])} &middot; {e(rep['config'])}</div>

  <div style="{h2}">1. Run summary</div>
  <table cellspacing="0" cellpadding="0" style="border-collapse:collapse;min-width:560px">{''.join(summary)}</table>
  <div style="font-size:11px;color:#57606a;margin-top:4px">A validation passes when none of its checks fails; checks
    within tolerance count as passed. A table passes when all its validations pass.</div>

  <div style="{h2}">2. Summary table (checks per table per validation)</div>
  <table cellspacing="0" cellpadding="0" style="border-collapse:collapse;width:100%">
    <tr><th style="{head}">Table (rows src vs tgt)</th><th style="{head}">Result</th>{cols}</tr>
    {''.join(rows)}
  </table>

  {f'<div style="{h2}">3. Details (checks that did not pass)</div>' if details else ''}
  {''.join(details)}
  <div style="margin-top:20px;font-size:12px;color:#57606a">All checks: <code>{e(rep['results_table'])}</code>,
    example bad rows: <code>{e(rep['mismatch_table'])}</code></div>
  <pre style="background:#f6f8fa;padding:10px;font-size:12px;overflow:auto">{e(sql)}</pre>
</div></body></html>"""


def _verdict_word(v: dict) -> str:
    return "FAIL" if v["status"] in ("FAIL", "ERROR") else "PASS, within tolerance"


# ------------------------------------------------------------------ output
def write_report(html_text: str, folder: str, run_id: str) -> str:
    os.makedirs(folder, exist_ok=True)
    path = os.path.join(folder, f"parity_{run_id}.html")
    with open(path, "w", encoding="utf-8") as fh:
        fh.write(html_text)
    return path


def should_send(cfg: dict, rep: dict) -> bool:
    on = str(cfg.get("send_on", "always")).lower()
    if on == "never":
        return False
    if on == "failure":
        return rep["overall"] in ("FAIL", "ERROR")
    if on == "issues":                         # any failure, or any check within tolerance
        return rep["overall"] != "PASS" or any(c["warn"] for t in rep["tables"] for c in t["cells"].values())
    return True


def send_email(cfg: dict, subject_line: str, text: str, html_text: Optional[str] = None) -> None:
    """Same approach as the team's EmailSender: SMTP relay, STARTTLS, plain + HTML alternative parts."""
    to = [a.strip() for a in str(cfg["to"]).replace(";", ",").split(",") if a.strip()]
    msg = MIMEMultipart("alternative")
    msg["From"] = cfg.get("sender", "noreply@ftr.databricks.com")
    msg["To"] = ", ".join(to)
    msg["Subject"] = subject_line
    msg.attach(MIMEText(text, "plain", "utf-8"))
    if html_text:
        msg.attach(MIMEText(html_text, "html", "utf-8"))
    server = None
    try:
        server = smtplib.SMTP(cfg.get("smtp_server", "mailrelay.ftr.com"), int(cfg.get("smtp_port", 25)), timeout=60)
        if cfg.get("starttls", True):
            server.starttls()
        server.sendmail(msg["From"], to, msg.as_string())
    finally:
        if server is not None:
            try:
                server.quit()
            except Exception:
                pass


def check_email_cfg(cfg: Optional[dict]):
    if not cfg:
        return
    unknown = set(cfg) - EMAIL_KEYS
    if unknown:
        raise ValueError(f"settings.email: unknown key(s) {sorted(unknown)}; allowed: {sorted(EMAIL_KEYS)}")
    if not cfg.get("to"):
        raise ValueError("settings.email needs 'to' (one address, or several separated by commas)")
    if str(cfg.get("send_on", "always")).lower() not in ("always", "issues", "failure", "never"):
        raise ValueError("settings.email.send_on must be always | issues | failure | never")
