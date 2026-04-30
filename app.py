"""Flask UI: compare Preprod vs Prod schema snapshots, preview tables, email report."""

from __future__ import annotations

import os
import traceback

from dotenv import load_dotenv
from flask import Flask, flash, redirect, render_template, request, session, url_for
from werkzeug.security import check_password_hash, generate_password_hash

from compare import compute_schema_diff
from database import (
    complete_signup,
    fetch_both,
    lookup_login_user,
    lookup_signup_candidate,
    username_exists,
)
from email_service import send_report_email
from html_report import render_report_email_html, render_report_html

load_dotenv()

app = Flask(__name__)
app.secret_key = os.environ.get("FLASK_SECRET_KEY", "dev-secret-change-me")


def _is_logged_in() -> bool:
    return "user_id" in session and "username" in session and "access" in session


def _is_admin() -> bool:
    return session.get("access") == "Admin"


def _base_view_model():
    vm = _empty_dashboard_kwargs()
    vm["current_user"] = {
        "id": session.get("user_id"),
        "username": session.get("username"),
        "access": session.get("access"),
    } if _is_logged_in() else None
    return vm


@app.context_processor
def _inject_current_user():
    return {
        "current_user": {
            "id": session.get("user_id"),
            "username": session.get("username"),
            "access": session.get("access"),
        } if _is_logged_in() else None,
    }


def _build_username(first_name: str, last_name: str) -> str:
    return f"{first_name}_{last_name[0].upper()}"


@app.before_request
def _require_login_for_app_routes():
    allowed_endpoints = {"login", "signup", "static"}
    endpoint = request.endpoint or ""
    if endpoint in allowed_endpoints or endpoint.startswith("static"):
        return
    if not _is_logged_in():
        return redirect(url_for("login"))


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
    return render_template("dashboard.html", page="home", **_base_view_model())


@app.route("/changes", methods=["GET", "POST"])
def changes():
    if request.method == "GET":
        return render_template("dashboard.html", page="changes", **_base_view_model())
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
    return render_template("dashboard.html", page="deploy_dws", **_base_view_model())


@app.route("/mail", methods=["GET", "POST"])
def mail_page():
    if request.method == "GET":
        return render_template("dashboard.html", page="email", **_base_view_model())

    action = (request.form.get("action") or "").strip().lower()
    if action == "send" and not _is_admin():
        flash("Only Admin users can send email reports.", "error")
        return render_template("dashboard.html", page="email", **_base_view_model()), 403
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


@app.route("/signup", methods=["GET", "POST"])
def signup():
    if _is_logged_in():
        return redirect(url_for("home"))
    if request.method == "GET":
        return render_template("auth.html", page="signup")

    email = (request.form.get("email") or "").strip().lower()
    first_name = (request.form.get("first_name") or "").strip()
    last_name = (request.form.get("last_name") or "").strip()
    password = request.form.get("password") or ""
    confirm_password = request.form.get("confirm_password") or ""

    if not email or not first_name or not last_name or not password or not confirm_password:
        flash("All fields are required.", "error")
        return render_template("auth.html", page="signup"), 400
    if password != confirm_password:
        flash("Password and confirm password do not match.", "error")
        return render_template("auth.html", page="signup"), 400

    candidate = lookup_signup_candidate(email)
    if not candidate:
        flash("Your email is not pre-approved for registration.", "error")
        return render_template("auth.html", page="signup"), 403

    username = _build_username(first_name, last_name)
    if username_exists(username):
        flash("Generated username is already taken. Please contact an admin.", "error")
        return render_template("auth.html", page="signup"), 409

    ok = complete_signup(
        email=email,
        first_name=first_name,
        last_name=last_name,
        username=username,
        password_hash=generate_password_hash(password),
    )
    if not ok:
        flash("Registration could not be completed. Please try again.", "error")
        return render_template("auth.html", page="signup"), 409

    flash(f"Signup successful. Your username is: {username}", "success")
    return redirect(url_for("login"))


@app.route("/login", methods=["GET", "POST"])
def login():
    if _is_logged_in():
        return redirect(url_for("home"))
    if request.method == "GET":
        return render_template("auth.html", page="login")

    username = (request.form.get("username") or "").strip()
    password = request.form.get("password") or ""
    if not username or not password:
        flash("Username and password are required.", "error")
        return render_template("auth.html", page="login"), 400

    user = lookup_login_user(username)
    if not user or not check_password_hash(user.get("PasswordHash") or "", password):
        flash("Invalid username or password.", "error")
        return render_template("auth.html", page="login"), 401

    session["user_id"] = user["Id"]
    session["username"] = user["UserName"]
    session["access"] = user.get("Access") or "User"
    flash("Logged in successfully.", "success")
    return redirect(url_for("home"))


@app.post("/logout")
def logout():
    session.clear()
    flash("You have been logged out.", "success")
    return redirect(url_for("login"))


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=int(os.environ.get("PORT", "5000")), debug=True)
