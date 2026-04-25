"""Flask UI: compare Preprod vs Prod schema snapshots, preview tables, email report."""

from __future__ import annotations

import os
import traceback

from dotenv import load_dotenv
from flask import Flask, flash, render_template, request

from compare import compute_schema_diff
from database import fetch_both
from email_service import send_report_email
from html_report import render_report_email_html, render_report_html

load_dotenv()

app = Flask(__name__)
app.secret_key = os.environ.get("FLASK_SECRET_KEY", "dev-secret-change-me")


def _run_compare():
    pre, prod = fetch_both()
    return compute_schema_diff(pre, prod)


def _empty_dashboard_kwargs():
    return {
        "tables_html": None,
        "email_preview_html": None,
        "result_meta": None,
        "error": None,
    }


@app.get("/")
def home():
    return render_template("dashboard.html", page="home", **_empty_dashboard_kwargs())


@app.route("/changes", methods=["GET", "POST"])
def changes():
    if request.method == "GET":
        return render_template("dashboard.html", page="changes", **_empty_dashboard_kwargs())
    try:
        result = _run_compare()
        if result.error:
            return render_template(
                "dashboard.html",
                page="changes",
                tables_html=None,
                email_preview_html=None,
                result_meta=None,
                error=result.error,
            )
        tables_html = render_report_html(result, include_document_wrapper=False)
        meta = {
            "preprod_rows": result.preprod_row_count,
            "prod_rows": result.prod_row_count,
        }
        return render_template(
            "dashboard.html",
            page="changes",
            tables_html=tables_html,
            email_preview_html=None,
            result_meta=meta,
            error=None,
        )
    except Exception as exc:  # noqa: BLE001
        return render_template(
            "dashboard.html",
            page="changes",
            tables_html=None,
            email_preview_html=None,
            result_meta=None,
            error=f"{type(exc).__name__}: {exc}\n\n{traceback.format_exc()}",
        )


@app.get("/deploy-dws")
def deploy_dws():
    return render_template("dashboard.html", page="deploy_dws", **_empty_dashboard_kwargs())


@app.route("/mail", methods=["GET", "POST"])
def mail_page():
    if request.method == "GET":
        return render_template("dashboard.html", page="email", **_empty_dashboard_kwargs())

    action = (request.form.get("action") or "").strip().lower()
    try:
        result = _run_compare()
        if result.error:
            return render_template(
                "dashboard.html",
                page="email",
                tables_html=None,
                email_preview_html=None,
                result_meta=None,
                error=result.error,
            )
        meta = {
            "preprod_rows": result.preprod_row_count,
            "prod_rows": result.prod_row_count,
        }

        if action == "send":
            html_body = render_report_email_html(result, for_browser_preview=False)
            send_report_email(html_body)
            flash("Report email sent successfully.", "success")
            preview_html = render_report_email_html(result, for_browser_preview=True)
            return render_template(
                "dashboard.html",
                page="email",
                tables_html=None,
                email_preview_html=preview_html,
                result_meta=meta,
                error=None,
            )

        # preview (default)
        preview_html = render_report_email_html(result, for_browser_preview=True)
        return render_template(
            "dashboard.html",
            page="email",
            tables_html=None,
            email_preview_html=preview_html,
            result_meta=meta,
            error=None,
        )
    except Exception as exc:  # noqa: BLE001
        return render_template(
            "dashboard.html",
            page="email",
            tables_html=None,
            email_preview_html=None,
            result_meta=None,
            error=f"{type(exc).__name__}: {exc}\n\n{traceback.format_exc()}",
        )


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=int(os.environ.get("PORT", "5000")), debug=True)
