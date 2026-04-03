"""
email_reports.py
----------------
Helpers for managing the judge email list and sending per-judge
HTML reports via SMTP.
"""

import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.base import MIMEBase
from email import encoders

import pandas as pd
from sqlalchemy import text as sqlt

from report_html import build_judge_report_html


# ── Database helpers ──────────────────────────────────────────────────────────

def ensure_email_table(session):
    """Create judge_email_list table if it doesn't exist."""
    session.execute(sqlt("""
        CREATE TABLE IF NOT EXISTS judge_email_list (
            id SERIAL PRIMARY KEY,
            judge_name TEXT NOT NULL UNIQUE,
            email TEXT NOT NULL
        )
    """))
    session.commit()


def get_email_list(session) -> pd.DataFrame:
    """Return the stored judge email list as a DataFrame."""
    ensure_email_table(session)
    rows = session.execute(
        sqlt("SELECT judge_name, email FROM judge_email_list ORDER BY judge_name")
    ).fetchall()
    return pd.DataFrame(rows, columns=["judge_name", "email"])


def upsert_email_list(session, df: pd.DataFrame):
    """
    Insert or update rows from a DataFrame with columns ['judge_name', 'email'].
    Returns (inserted, updated) counts.
    """
    ensure_email_table(session)
    inserted = updated = 0
    for _, row in df.iterrows():
        name = str(row["judge_name"]).strip()
        email = str(row["email"]).strip()
        if not name or not email:
            continue
        existing = session.execute(
            sqlt("SELECT id FROM judge_email_list WHERE lower(judge_name) = lower(:n)"),
            {"n": name}
        ).fetchone()
        if existing:
            session.execute(
                sqlt("UPDATE judge_email_list SET judge_name=:n, email=:e WHERE id=:id"),
                {"n": name, "e": email, "id": existing[0]}
            )
            updated += 1
        else:
            session.execute(
                sqlt("INSERT INTO judge_email_list (judge_name, email) VALUES (:n, :e)"),
                {"n": name, "e": email}
            )
            inserted += 1
    session.commit()
    return inserted, updated


def delete_email_entry(session, judge_name: str):
    ensure_email_table(session)
    session.execute(
        sqlt("DELETE FROM judge_email_list WHERE lower(judge_name) = lower(:n)"),
        {"n": judge_name}
    )
    session.commit()


# ── Name matching ─────────────────────────────────────────────────────────────

def _norm(s: str) -> str:
    return s.lower().strip()


def match_judge_to_email(judge_name: str, email_df: pd.DataFrame):
    """
    Return the email for a judge name, or None if not found.
    Tries exact case-insensitive match first, then whitespace-normalised match.
    """
    if email_df.empty:
        return None
    norm_target = _norm(judge_name)
    for _, row in email_df.iterrows():
        if _norm(row["judge_name"]) == norm_target:
            return row["email"]
    return None


# ── Report building ───────────────────────────────────────────────────────────

def build_report_for_judge(analytics, judge_id: int, competition_id: int):
    """
    Build the HTML report bytes for a single judge filtered to one competition.
    Returns (html_bytes, judge_name) or raises on error.
    """
    comp_ids = [competition_id]
    pcs_df = analytics.get_judge_pcs_stats(judge_id, competition_ids=comp_ids)
    elem_df = analytics.get_judge_element_stats(judge_id, competition_ids=comp_ids)
    seg_df = analytics.get_judge_segment_stats(judge_id, competition_ids=comp_ids)

    stats = analytics.calculate_judge_summary_stats(pcs_df, elem_df)

    from models import Judge
    judge = analytics.session.get(Judge, judge_id)
    judge_name = judge.name if judge else f"Judge #{judge_id}"

    html_bytes = build_judge_report_html(judge_name, stats, pcs_df, elem_df, seg_df)
    return html_bytes, judge_name


# ── SMTP sending ──────────────────────────────────────────────────────────────


DEFAULT_EMAIL_SUBJECT = "Judge Performance Report – {competition_name}"

DEFAULT_EMAIL_BODY = (
    "Hello {judge_name},\n\n"
    "Please find attached your judge performance report for {competition_name}.\n\n"
    "Open the attached HTML file in any web browser to view your interactive report.\n\n"
    "This report contains only your data and is safe to save on your device.\n\n"
    "Thank you,\n{from_name}"
)


def send_report_email(smtp_config: dict, to_email: str, judge_name: str,
                      competition_name: str, html_bytes: bytes,
                      subject_template: str = DEFAULT_EMAIL_SUBJECT,
                      body_template: str = DEFAULT_EMAIL_BODY):
    """
    Send one judge report via SMTP.
    html_bytes is the report content; it is sent as an HTML attachment.
    subject_template and body_template support {judge_name}, {competition_name},
    and {from_name} placeholders.
    Raises smtplib exceptions on failure.
    """
    subs = {
        "judge_name": judge_name,
        "competition_name": competition_name,
        "from_name": smtp_config["from_name"],
    }
    subject = subject_template.format(**subs)
    body_text = body_template.format(**subs)

    msg = MIMEMultipart("mixed")
    from_addr = f"{smtp_config['from_name']} <{smtp_config['user']}>"
    msg["From"] = from_addr
    msg["To"] = to_email
    msg["Subject"] = subject

    body = MIMEText(body_text, "plain")
    msg.attach(body)

    safe_name = judge_name.replace(" ", "_").replace("/", "_")
    safe_comp = competition_name.replace(" ", "_").replace("/", "_")
    filename = f"judge_report_{safe_name}_{safe_comp}.html"

    attachment = MIMEBase("text", "html")
    attachment.set_payload(html_bytes)
    encoders.encode_base64(attachment)
    attachment.add_header("Content-Disposition", "attachment", filename=filename)
    msg.attach(attachment)

    port = smtp_config["port"]
    if port == 465:
        with smtplib.SMTP_SSL(smtp_config["host"], port) as server:
            server.login(smtp_config["user"], smtp_config["password"])
            server.send_message(msg)
    else:
        with smtplib.SMTP(smtp_config["host"], port) as server:
            server.ehlo()
            server.starttls()
            server.login(smtp_config["user"], smtp_config["password"])
            server.send_message(msg)
