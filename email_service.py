"""Send HTML report via SMTP using smtplib + multipart/related (inline signature image)."""

from __future__ import annotations

import os
import smtplib
from datetime import date, timedelta
from email.mime.image import MIMEImage
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText

from html_report import SIGNATURE_BANNER_CID, resolve_signature_banner_path


def deploy_scheduled_tuesday() -> date:
    """
    Tuesday of the ISO week that contains *today + 14 days*.

    That is the Tuesday in the calendar week two weeks ahead of the current date
    (aligned with “two weeks from now” and “the Tuesday in that week”).
    """
    anchor = date.today() + timedelta(days=14)
    monday = anchor - timedelta(days=anchor.weekday())
    return monday + timedelta(days=1)


def default_mobilife_deploy_subject() -> str:
    d = deploy_scheduled_tuesday()
    return f"Database Changes - {d.strftime('%Y%m%d')} MobiLife Deploy"


def send_report_email(html_body: str, subject: str | None = None) -> None:
    host = os.environ["SMTP_HOST"]
    port = int(os.environ.get("SMTP_PORT", "587"))
    user = os.environ.get("SMTP_USER", "")
    password = os.environ.get("SMTP_PASSWORD", "")
    use_tls = os.environ.get("SMTP_USE_TLS", "true").strip().lower() in {"1", "true", "yes", "y"}
    mail_from = os.environ["EMAIL_FROM"]
    mail_to = os.environ["EMAIL_TO"]
    cc_raw = os.environ.get("EMAIL_CC", "").strip()

    msg_root = MIMEMultipart("related")
    msg_root["Subject"] = subject or default_mobilife_deploy_subject()
    msg_root["From"] = mail_from
    msg_root["To"] = mail_to
    if cc_raw:
        msg_root["Cc"] = cc_raw
    msg_root.preamble = "This message is in MIME format."

    msg_alt = MIMEMultipart("alternative")
    msg_alt.attach(
        MIMEText("This message requires an HTML-capable mail client.", "plain", "utf-8"),
    )
    msg_alt.attach(MIMEText(html_body, "html", "utf-8"))
    msg_root.attach(msg_alt)

    path = resolve_signature_banner_path()
    cid_token = f"cid:{SIGNATURE_BANNER_CID}"
    if path is not None and path.is_file() and cid_token in html_body:
        ext = path.suffix.lower()
        subtype = "jpeg" if ext in (".jpg", ".jpeg") else "png"
        with open(path, "rb") as f:
            raw = f.read()
        image = MIMEImage(raw, _subtype=subtype)
        image.add_header("Content-ID", f"<{SIGNATURE_BANNER_CID}>")
        image.add_header("Content-Disposition", "inline", filename=path.name)
        msg_root.attach(image)

    recipients = [addr.strip() for addr in mail_to.split(",") if addr.strip()]
    if cc_raw:
        recipients.extend([addr.strip() for addr in cc_raw.split(",") if addr.strip()])

    with smtplib.SMTP(host, port, timeout=60) as smtp:
        if use_tls:
            smtp.starttls()
        if user:
            smtp.login(user, password)
        smtp.send_message(msg_root, from_addr=mail_from, to_addrs=recipients)
