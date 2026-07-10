"""Flask UI: compare Preprod vs Prod schema snapshots, preview tables, email report.

WHAT THIS FILE IS
-----------------
This is the "front door" of the application. It is a Flask web app, which means it
listens for web requests (someone visiting a URL in their browser) and decides what
to send back (an HTML page, a redirect, etc.).

The big picture of what happens here:
  1. A user opens the site and must log in.
  2. Once logged in, they can compare the Preprod and Production databases
     ("Review Updates"), and preview/send a summary email ("Communication").
  3. Security rules (login required, 5-minute inactivity timeout, admin-only send)
     are enforced automatically on every request.

A note on terminology used throughout the comments:
  - "route"   = a function that handles a specific URL (e.g. "/login").
  - "request" = the incoming visit from the browser.
  - "session" = a small, secure, per-user store the server keeps between requests
                (remembers that you are logged in). Backed by a signed cookie.
  - "template"= an HTML file with placeholders that we fill in and send back.
"""

from __future__ import annotations  # Lets us use modern type hints (e.g. "str | None") on older Python.

# ---------------------------------------------------------------------------
# Imports — bring in the tools (functions/classes) this file relies on.
# ---------------------------------------------------------------------------
# Standard-library modules (built into Python, nothing to install):
import os  # Access environment variables, i.e. configuration that lives outside the code.
import time  # Get the current time as a number; used to measure how long a user has been idle.
import traceback  # Produce a detailed, line-by-line report of an error for debugging.
from datetime import datetime, timedelta, timezone  # Tools for dates, durations, and time zones.
from pathlib import Path  # A convenient, cross-platform way to work with file paths.

# Third-party libraries (installed via requirements.txt):
from dotenv import load_dotenv  # Reads a local ".env" file and loads its values as environment variables.
from flask import Flask, flash, redirect, render_template, request, session, url_for
#   Flask          -> the web-framework class we build the app from.
#   flash          -> queue a one-time message to show the user on the next page (e.g. "Logged in").
#   redirect       -> send the browser to a different URL.
#   render_template-> fill an HTML template with data and return it as the page.
#   request        -> the current incoming web request (form data, method, etc.).
#   session        -> the per-user server-side store described above.
#   url_for        -> build a URL from a route's function name (avoids hard-coding paths).
from werkzeug.security import check_password_hash, generate_password_hash
#   generate_password_hash -> turn a plain password into a secure, irreversible hash for storage.
#   check_password_hash    -> safely compare a typed password against the stored hash.

# Our own project modules (other files in this project):
from compare import combine_change_scripts, compute_schema_diff  # Compare snapshots + merge change scripts.
from database import (  # Functions that talk to the MySQL databases.
    complete_signup,        # Finish registering a pre-approved user.
    fetch_both,             # Read the Preprod and Production schema snapshots.
    fetch_dmz_both,         # Read the dmz_preprod and mobility_dmz_prod schema snapshots.
    lookup_login_user,      # Find a user record for logging in.
    lookup_signup_candidate,# Check whether an email is pre-approved to register.
    username_exists,        # Check whether a username is already taken.
)
from email_service import change_script_filename, email_delivery_summary, send_report_email
#   change_script_filename -> the name to use for the attached SQL script (.txt) file.
#   email_delivery_summary -> subject + recipients the report would be sent to (shown on the page).
#   send_report_email      -> actually send the report email via SMTP.
from html_report import render_combined_report_email_html, render_report_html
#   render_report_html               -> build the on-screen comparison tables.
#   render_combined_report_email_html-> build one HTML email covering multiple environments.

# Read the .env file NOW so that any os.environ lookups below find the values.
# (.env holds secrets like database passwords and SMTP settings, and is never committed to git.)
load_dotenv()

# ---------------------------------------------------------------------------
# Application setup — create and configure the Flask app object.
# ---------------------------------------------------------------------------
# Create the application. "__name__" tells Flask where the app lives so it can
# locate the "templates" and "static" folders relative to this file.
app = Flask(__name__)

# The secret key is used to cryptographically SIGN the session cookie. This stops
# users from tampering with it (e.g. changing their role to "Admin"). In production
# the real key comes from an environment variable; locally we fall back to a dummy.
app.secret_key = os.environ.get("FLASK_SECRET_KEY", "dev-secret-change-me")

# How long a user may sit idle before being logged out: 5 minutes, expressed in seconds.
SESSION_IDLE_TIMEOUT_SECONDS = 5 * 60

# Tell Flask the maximum lifetime of a "permanent" session cookie. We set the cookie
# to permanent at login (see below), and combine it with our own idle check to enforce
# the 5-minute timeout.
app.config["PERMANENT_SESSION_LIFETIME"] = timedelta(seconds=SESSION_IDLE_TIMEOUT_SECONDS)


# ---------------------------------------------------------------------------
# Authentication helpers — small reusable functions about "who is logged in".
# ---------------------------------------------------------------------------
def _is_logged_in() -> bool:
    # We treat someone as logged in only when ALL three identity keys exist in the
    # session. They are placed there during a successful login (see login()).
    return "user_id" in session and "username" in session and "access" in session


def _is_admin() -> bool:
    # The "access" value is the user's role. Only "Admin" may send the report email.
    # session.get(...) returns None (instead of erroring) if the key is missing.
    return session.get("access") == "Admin"


def _base_view_model():
    # A "view model" is just the bundle of data we hand to a template to render a page.
    # This builds the default bundle and attaches the current user's details (or None).
    vm = _empty_dashboard_kwargs()
    vm["current_user"] = {
        "id": session.get("user_id"),
        "username": session.get("username"),
        "access": session.get("access"),
    } if _is_logged_in() else None  # If not logged in, current_user is None.
    return vm


@app.context_processor
def _inject_current_user():
    # A "context processor" is a special Flask hook: whatever dictionary it returns is
    # automatically merged into the data available to EVERY template. That means
    # "current_user" and "last_updated" can be used in any page (e.g. the sidebar and
    # footer) without each route having to pass them in manually.
    return {
        "current_user": {
            "id": session.get("user_id"),
            "username": session.get("username"),
            "access": session.get("access"),
        } if _is_logged_in() else None,
        "last_updated": project_last_updated(),  # Timestamp shown in the home-page footer.
        # Subject + recipients the report email would use (shown on the Communication page).
        "email_summary": email_delivery_summary(),
    }


def _build_username(first_name: str, last_name: str) -> str:
    # Create a username like "John_D": first name, an underscore, then the capitalised
    # first letter of the surname. last_name[0] is the first character of the surname.
    return f"{first_name}_{last_name[0].upper()}"


# ---------------------------------------------------------------------------
# "Last updated" timestamp — shows in the home-page footer when the app last changed.
# ---------------------------------------------------------------------------
# A fixed time zone for display: South Africa Standard Time (UTC+2). The "SAST" label
# is just a name attached to the offset so it can be printed.
_DISPLAY_TZ = timezone(timedelta(hours=2), "SAST")
# The folder containing this file. resolve() turns it into a full, absolute path.
_PROJECT_ROOT = Path(__file__).resolve().parent
# The kinds of files we treat as "source" when deciding when the project last changed.
_LAST_UPDATED_SOURCES = ("*.py", "templates/*.html", "static/*.css")


def project_last_updated() -> str | None:
    """Most recent modification time across the app's source files, formatted for display."""
    # We track the newest modification time we find. 0.0 means "nothing found yet".
    latest = 0.0
    # Go through each file pattern (e.g. all .py files, all templates, all CSS).
    for pattern in _LAST_UPDATED_SOURCES:
        # glob() finds every file matching the pattern inside the project folder.
        for path in _PROJECT_ROOT.glob(pattern):
            try:
                # st_mtime is the file's last-modified time (as a number of seconds).
                # max(...) keeps whichever is newer: what we had, or this file.
                latest = max(latest, path.stat().st_mtime)
            except OSError:
                # If a file can't be read (permissions, deleted mid-scan, etc.), skip it.
                continue
    # If we never found a single file, there is nothing to show.
    if latest <= 0:
        return None
    # Convert the raw number into a readable date/time in our display time zone,
    # e.g. "19 Jun 2026, 14:07 (SAST)".
    return datetime.fromtimestamp(latest, _DISPLAY_TZ).strftime("%d %b %Y, %H:%M (%Z)")


def _touch_session_activity() -> None:
    # Save "right now" into the session as the user's last activity time. Calling this
    # on each request effectively resets the 5-minute idle countdown.
    session["last_activity_ts"] = int(time.time())


# ---------------------------------------------------------------------------
# Security gate — runs automatically BEFORE every single request.
# ---------------------------------------------------------------------------
@app.before_request
def _require_login_for_app_routes():
    # @app.before_request registers this function to run ahead of any route handler.
    # It is our central place to enforce "you must be logged in" and the idle timeout.

    # These endpoints are public — they must work even when nobody is logged in,
    # otherwise users could never reach the login or sign-up screens.
    allowed_endpoints = {"login", "signup", "logout", "logout_beacon", "static"}
    # request.endpoint is the name of the route function about to handle this request.
    endpoint = request.endpoint or ""
    # Let public pages (and any static asset like CSS/images) through immediately.
    if endpoint in allowed_endpoints or endpoint.startswith("static"):
        return  # Returning None means "carry on to the normal route handler".

    # From here on, the page is protected. If the user isn't logged in, send them
    # to the login screen instead of showing the page.
    if not _is_logged_in():
        return redirect(url_for("login"))

    # Idle-timeout check: compare "now" with the last recorded activity time.
    last_activity_ts = session.get("last_activity_ts")
    now = int(time.time())
    if isinstance(last_activity_ts, (int, float)) and (now - int(last_activity_ts)) > SESSION_IDLE_TIMEOUT_SECONDS:
        # Too long since last activity -> wipe the session (log them out), tell them
        # why, and redirect to login.
        session.clear()
        flash("Your session has timed out due to inactivity. Please sign in again.", "error")
        return redirect(url_for("login"))

    # The user is logged in and active, so reset the idle timer and continue.
    _touch_session_activity()


# ---------------------------------------------------------------------------
# Shared helpers used by the page routes below.
# ---------------------------------------------------------------------------
def _run_compare():
    # 1) Read both database snapshots (Preprod + Production).
    # 2) Compute and return the differences between them.
    # The returned object carries the diff tables, row counts, an optional error,
    # and the generated SQL change script.
    pre, prod = fetch_both()
    return compute_schema_diff(pre, prod)


def _run_compare_dmz():
    # Same as _run_compare(), but for the DMZ environments:
    # dmz_preprod (source) vs mobility_dmz_prod (target).
    pre, prod = fetch_dmz_both()
    return compute_schema_diff(pre, prod)


def _empty_dashboard_kwargs():
    # Every render of dashboard.html expects this same set of named values. Returning
    # them all as "empty" by default keeps the template simple: each route fills in
    # only the pieces relevant to it, and the rest stay None (so the template hides them).
    return {
        "tables_html": None,            # Rendered comparison tables (Review Updates page).
        "email_preview_html": None,     # Rendered email preview (Communication page).
        "change_script_preview": None,  # The SQL script text shown on screen.
        "change_script_filename": None, # The filename of the attached .txt script.
        "result_meta": None,            # Summary counts (how many rows were compared).
        "error": None,                  # Any error message to display to the user.
    }


# ---------------------------------------------------------------------------
# Routes — each function below is mapped to a URL by its decorator.
# (@app.get("/x") handles GET requests to /x; @app.route(..., methods=[...]) lists
#  which HTTP methods are allowed. GET = "show me a page"; POST = "submit a form".)
# ---------------------------------------------------------------------------
@app.get("/")
def home():
    # The landing/home page. We pass page="home" so the template knows which section
    # to highlight, plus the standard view-model values.
    return render_template("dashboard.html", page="home", **_base_view_model())


@app.route("/changes", methods=["GET", "POST"])
def changes():
    # "Review Updates" page. It accepts two kinds of request:
    #   GET  -> the user just navigated here, so show the page with no results yet.
    #   POST -> the user clicked "Scan for Updates", so run the comparison.
    if request.method == "GET":
        return render_template("dashboard.html", page="changes", **_base_view_model())

    # We wrap the heavy work in try/except so that any failure shows a helpful message
    # on the page instead of crashing the request.
    try:
        # Run the Preprod vs Prod comparison.
        result = _run_compare()

        # The comparison can report a "known" problem (e.g. missing expected columns).
        # If so, show that message rather than continuing.
        if result.error:
            return render_template(
                "dashboard.html",
                page="changes",
                tables_html=None,
                email_preview_html=None,
                result_meta=None,
                error=result.error,
            )

        # Convert the comparison result into HTML tables for the page.
        # include_document_wrapper=False means "just the tables", not a full HTML doc.
        tables_html = render_report_html(result, include_document_wrapper=False)

        # A small summary: how many column rows were read from each environment.
        meta = {
            "preprod_rows": result.preprod_row_count,
            "prod_rows": result.prod_row_count,
        }

        # Render the page WITH the results.
        return render_template(
            "dashboard.html",
            page="changes",
            tables_html=tables_html,
            email_preview_html=None,
            result_meta=meta,
            error=None,
        )
    except Exception as exc:  # noqa: BLE001  (we intentionally catch everything here)
        # Any unexpected error: show the error type, message, and full traceback so the
        # problem can be diagnosed. format_exc() returns the detailed stack trace text.
        return render_template(
            "dashboard.html",
            page="changes",
            tables_html=None,
            email_preview_html=None,
            result_meta=None,
            error=f"{type(exc).__name__}: {exc}\n\n{traceback.format_exc()}",
        )


@app.route("/dmz", methods=["GET", "POST"])
def dmz():
    # "DMZ Updates" page: compares dmz_preprod against mobility_dmz_prod.
    # Mirrors the /changes route, but uses the DMZ data sources.
    # GET -> show the page; POST -> run the DMZ comparison and show results.
    if request.method == "GET":
        return render_template("dashboard.html", page="dmz", **_base_view_model())

    try:
        # Run the dmz_preprod vs mobility_dmz_prod comparison.
        result = _run_compare_dmz()

        # Show any "known" comparison problem returned by the diff.
        if result.error:
            return render_template(
                "dashboard.html",
                page="dmz",
                tables_html=None,
                email_preview_html=None,
                result_meta=None,
                error=result.error,
            )

        # Turn the comparison result into HTML tables for display.
        tables_html = render_report_html(result, include_document_wrapper=False)
        meta = {
            "preprod_rows": result.preprod_row_count,
            "prod_rows": result.prod_row_count,
        }
        return render_template(
            "dashboard.html",
            page="dmz",
            tables_html=tables_html,
            email_preview_html=None,
            result_meta=meta,
            error=None,
        )
    except Exception as exc:  # noqa: BLE001
        # Show the full error details if something unexpected fails.
        return render_template(
            "dashboard.html",
            page="dmz",
            tables_html=None,
            email_preview_html=None,
            result_meta=None,
            error=f"{type(exc).__name__}: {exc}\n\n{traceback.format_exc()}",
        )


# --- DW Deployment placeholder pages (feature still "coming soon"). ---
@app.get("/deploy-dws")
def deploy_dws():
    # Main "DW Deployment" landing tab. Currently shows a "coming soon" message.
    return render_template("dashboard.html", page="deploy_dws", **_base_view_model())


@app.get("/deploy-dws/client")
def deploy_dws_client():
    # "Client" sub-tab under DW Deployment.
    return render_template("dashboard.html", page="deploy_dws_client", **_base_view_model())


@app.get("/deploy-dws/internal")
def deploy_dws_internal():
    # "Internal" sub-tab under DW Deployment.
    return render_template("dashboard.html", page="deploy_dws_internal", **_base_view_model())


@app.route("/mail", methods=["GET", "POST"])
def mail_page():
    # "Communication" page: preview the report email, and (for admins) send it.
    # GET -> just show the page.
    if request.method == "GET":
        return render_template("dashboard.html", page="email", **_base_view_model())

    # The form includes a hidden "action" field telling us which button was pressed:
    # "preview" (Load Preview) or "send" (Send to Team). We normalise it to lowercase.
    action = (request.form.get("action") or "").strip().lower()

    # Guard: sending is restricted to admins. A non-admin who somehow POSTs a "send"
    # is rejected with a 403 (Forbidden) status and a clear message.
    if action == "send" and not _is_admin():
        flash("Only Admin users can send email reports.", "error")
        return render_template("dashboard.html", page="email", **_base_view_model()), 403

    try:
        # The combined report covers BOTH environments, so run both comparisons:
        #   - Production: preprod vs prod
        #   - DMZ: dmz_preprod vs mobility_dmz_prod
        prod_result = _run_compare()
        dmz_result = _run_compare_dmz()

        # If either comparison reported a known problem, surface it (don't send a
        # partial report). We note which environment it came from.
        if prod_result.error:
            return render_template(
                "dashboard.html",
                page="email",
                tables_html=None,
                email_preview_html=None,
                result_meta=None,
                error=f"Production comparison: {prod_result.error}",
            )
        if dmz_result.error:
            return render_template(
                "dashboard.html",
                page="email",
                tables_html=None,
                email_preview_html=None,
                result_meta=None,
                error=f"DMZ comparison: {dmz_result.error}",
            )

        # Row counts for both environments (shown as a small summary on the page).
        meta = {
            "prod_preprod_rows": prod_result.preprod_row_count,
            "prod_prod_rows": prod_result.prod_row_count,
            "dmz_preprod_rows": dmz_result.preprod_row_count,
            "dmz_prod_rows": dmz_result.prod_row_count,
        }

        # The two environments, each with a heading, for the combined email body.
        groups = [
            ("Production Updates", prod_result),
            ("DMZ Updates", dmz_result),
        ]
        # One combined SQL script: production statements followed by DMZ statements,
        # each under a clear banner. Empty sections are skipped automatically.
        combined_script = combine_change_scripts([
            ("PRODUCTION  (preprod -> prod)", prod_result.change_script),
            ("DMZ  (dmz_preprod -> mobility_dmz_prod)", dmz_result.change_script),
        ])

        # --- SEND branch: build the combined email, attach the combined script, send it. ---
        if action == "send":
            # Build the email body for real sending (uses cid: images for mail clients).
            html_body = render_combined_report_email_html(groups, for_browser_preview=False)
            # Decide the attachment's filename, e.g. "Database_Changes_20260623_MobiLife.txt".
            attachment_name = change_script_filename()
            attachments = []
            # Only attach a file if there are actually scripts to include.
            if combined_script.strip():
                attachments.append((attachment_name, combined_script))
            # Hand off to the email service to deliver it via SMTP.
            send_report_email(html_body, text_attachments=attachments)
            flash("Report email sent successfully.", "success")
            # Re-show the page WITH a browser-friendly preview and the script text, so
            # the user can confirm what was sent. (data: images make the preview render.)
            preview_html = render_combined_report_email_html(groups, for_browser_preview=True)
            return render_template(
                "dashboard.html",
                page="email",
                tables_html=None,
                email_preview_html=preview_html,
                change_script_preview=combined_script,
                change_script_filename=attachment_name,
                result_meta=meta,
                error=None,
            )

        # --- PREVIEW branch (the default when action isn't "send"): show, don't send. ---
        preview_html = render_combined_report_email_html(groups, for_browser_preview=True)
        return render_template(
            "dashboard.html",
            page="email",
            tables_html=None,
            email_preview_html=preview_html,
            change_script_preview=combined_script,   # Show the SQL the user would send.
            change_script_filename=change_script_filename(),
            result_meta=meta,
            error=None,
        )
    except Exception as exc:  # noqa: BLE001
        # Same catch-all error handling as the other routes.
        return render_template(
            "dashboard.html",
            page="email",
            tables_html=None,
            email_preview_html=None,
            result_meta=None,
            error=f"{type(exc).__name__}: {exc}\n\n{traceback.format_exc()}",
        )


# ---------------------------------------------------------------------------
# Sign-up — registration, but only for emails that an admin pre-approved.
# ---------------------------------------------------------------------------
@app.route("/signup", methods=["GET", "POST"])
def signup():
    # If you're already logged in, there's nothing to register — go home.
    if _is_logged_in():
        return redirect(url_for("home"))
    # GET -> show the blank sign-up form.
    if request.method == "GET":
        return render_template("auth.html", page="signup")

    # POST -> process the submitted form. request.form holds the posted fields.
    # We strip() whitespace and lowercase the email so comparisons are consistent.
    email = (request.form.get("email") or "").strip().lower()
    first_name = (request.form.get("first_name") or "").strip()
    last_name = (request.form.get("last_name") or "").strip()
    password = request.form.get("password") or ""
    confirm_password = request.form.get("confirm_password") or ""

    # Validation 1: no field may be empty. 400 = "Bad Request".
    if not email or not first_name or not last_name or not password or not confirm_password:
        flash("All fields are required.", "error")
        return render_template("auth.html", page="signup"), 400
    # Validation 2: the two password entries must match.
    if password != confirm_password:
        flash("Password and confirm password do not match.", "error")
        return render_template("auth.html", page="signup"), 400

    # Pre-approval check: the email must already exist in the users table with an
    # empty FirstName (meaning "invited but not yet registered"). 403 = "Forbidden".
    candidate = lookup_signup_candidate(email)
    if not candidate:
        flash("Your email is not pre-approved for registration.", "error")
        return render_template("auth.html", page="signup"), 403

    # Build the username and make sure nobody else already has it. 409 = "Conflict".
    username = _build_username(first_name, last_name)
    if username_exists(username):
        flash("Generated username is already taken. Please contact an admin.", "error")
        return render_template("auth.html", page="signup"), 409

    # Finalise the account. We store a HASH of the password, never the password itself,
    # so even someone with database access cannot read users' real passwords.
    ok = complete_signup(
        email=email,
        first_name=first_name,
        last_name=last_name,
        username=username,
        password_hash=generate_password_hash(password),
    )
    # complete_signup returns False if it couldn't update exactly one matching row.
    if not ok:
        flash("Registration could not be completed. Please try again.", "error")
        return render_template("auth.html", page="signup"), 409

    # All good: tell the user the username we generated, then send them to the login page.
    flash(f"Signup successful. Your username is: {username}", "success")
    return redirect(url_for("login"))


# ---------------------------------------------------------------------------
# Login — verify credentials and start a session.
# ---------------------------------------------------------------------------
@app.route("/login", methods=["GET", "POST"])
def login():
    # Already logged in? Skip the form and go home.
    if _is_logged_in():
        return redirect(url_for("home"))
    # GET -> show the login form. If we arrived here because of a timeout
    # (?timeout=1 in the URL), display a friendly explanation.
    if request.method == "GET":
        if request.args.get("timeout") == "1":  # request.args = values from the URL's query string.
            flash("Your session has timed out due to inactivity. Please sign in again.", "error")
        return render_template("auth.html", page="login")

    # POST -> read and tidy the submitted credentials.
    username = (request.form.get("username") or "").strip()
    password = request.form.get("password") or ""
    # Both fields are required.
    if not username or not password:
        flash("Username and password are required.", "error")
        return render_template("auth.html", page="login"), 400

    # Look up the user (by username or email). Then take the stored password hash,
    # tolerating a missing user (None) by defaulting to an empty string.
    user = lookup_login_user(username)
    stored_hash = (user.get("PasswordHash") or "").strip() if user else ""
    # Reject if: no user found, no stored hash, OR the password doesn't match the hash.
    # check_password_hash safely compares the typed password to the stored hash.
    # 401 = "Unauthorized". We keep the message vague on purpose (don't reveal which part failed).
    if not user or not stored_hash or not check_password_hash(stored_hash, password):
        flash("Invalid username or password.", "error")
        return render_template("auth.html", page="login"), 401

    # Success: store the user's identity in the session. These three keys are what
    # _is_logged_in() looks for, and "access" drives admin-only features.
    session["user_id"] = user["Id"]
    session["username"] = user["UserName"]
    session["access"] = user.get("Access") or "User"  # If no role is set, treat them as a normal User.
    session.permanent = True       # Apply PERMANENT_SESSION_LIFETIME to this session cookie.
    _touch_session_activity()      # Start (reset) the idle timer.
    flash("Logged in successfully.", "success")
    return redirect(url_for("home"))


# ---------------------------------------------------------------------------
# Logout — two variants.
# ---------------------------------------------------------------------------
@app.post("/logout")
def logout():
    # Normal logout triggered by the "Log out" button. Clearing the session removes
    # all the identity keys, so the user is no longer recognised as logged in.
    session.clear()
    flash("You have been logged out.", "success")
    return redirect(url_for("login"))


@app.post("/logout-beacon")
def logout_beacon():
    # Automatic logout when the browser TAB is closed. The page's JavaScript sends a
    # tiny background "beacon" request to this URL on unload. There is no page to show,
    # so we just clear the session and return "204 No Content" (success, empty body).
    session.clear()
    return ("", 204)


# ---------------------------------------------------------------------------
# Entry point — only runs when this file is executed directly (local development).
# ---------------------------------------------------------------------------
if __name__ == "__main__":
    # __name__ == "__main__" is True only when you run "python app.py" yourself.
    # It is False when a production server (Gunicorn, on Azure) imports the app object,
    # so this development server never starts in production.
    #   host="0.0.0.0" -> listen on all network interfaces (not just this machine).
    #   port           -> use the PORT env var if set, otherwise 5000.
    #   debug=True     -> auto-reload on code changes and show detailed error pages.
    app.run(host="0.0.0.0", port=int(os.environ.get("PORT", "5000")), debug=True)
