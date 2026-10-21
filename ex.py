import sys
ROOT = "/Workspace/Users/skk983@ftr.com/parity_framework_v3"     # folder that contains etl_parity/
sys.path.insert(0, ROOT)

from etl_parity import run_parity, send_report
import etl_parity
print(etl_parity.__version__)        # should print 3.7.0

FULL_YAML  = f"{ROOT}/configs/icd_tables.yaml"      # your full-load YAML
DELTA_YAML = f"{ROOT}/configs/icd_delta.yaml"       # your incremental YAML


full = run_parity(FULL_YAML, env="databricks", send_email=False, parallel_tables=4)
delta = run_parity(DELTA_YAML, env="databricks", send_email=False, parallel_tables=4,
                   params={"SRC_BATCH_ID": "...", "TGT_BATCH_ID": "..."})    # only if your delta YAML uses ${...}



runs = [r for r in (globals().get("full"), globals().get("delta")) if r]     # whichever ran
report = send_report(runs, config=FULL_YAML)       # email settings come from this YAML's settings.email

print(report.subject)
print("e-mail sent" if report.email_sent else f"not sent: {report.email_error or 'no email settings / send_on'}")
displayHTML(report.report_html)                    # the same report, shown in the notebook
