# MobiLife Deployment Assistant — Project Overview

## What is this project?

The **MobiLife Deployment Assistant** is an internal web tool that helps the data
team get ready for a database deployment. Before changes are pushed from the
**Preprod** environment to **Production**, the tool automatically works out *what
is about to change* and helps the team communicate those changes clearly.

In short: it takes the guesswork and manual checking out of preparing for a deploy.

## The problem it solves

Before each deployment, someone had to manually compare the Preprod and Production
databases to figure out what was new or different, then write up an email to tell
the rest of the team. This was slow, easy to get wrong, and hard to keep
consistent.

This tool does that comparison automatically and produces a tidy, ready-to-send
summary.

## What it does today

- **Review Updates** — Compares the Preprod and Production database schemas and
  shows exactly what is changing in the next deploy:
  - New tables
  - New or removed columns
  - New indexes
  - Data type / length changes
  - The relevant Production table size for those changes
- **Communication (email draft)** — Builds a polished, formatted email summarising
  the changes, with a MobiLife signature. You can preview it on screen before it
  goes out.
- **SQL script attachment** — Generates a ready-to-review `.txt` file of the MySQL
  scripts needed for the column, index, and data type changes (it deliberately
  leaves out brand-new table creation). You can view these scripts on the same page
  as the email draft, and they are attached to the email when it is sent.
- **Secure sign-in** — Users must log in to use the tool. Accounts are pre-approved,
  passwords are stored securely (hashed), and sessions automatically log out after
  inactivity or when the browser tab is closed.
- **Roles** — Admin users can send the report email to the team; standard users can
  review and preview but cannot send.
- **DW Deployment (coming soon)** — A placeholder area, with Client and Internal
  sub-sections, reserved for the next phase of work.

## How it works (the simple version)

1. The tool securely connects to the Preprod and Production databases and reads
   their structure (tables, columns, indexes, sizes).
2. It compares the two and works out the differences.
3. It turns those differences into:
   - an on-screen summary,
   - a formatted email,
   - and a downloadable/attachable SQL script.
4. An Admin reviews everything and sends the report to the team.

## Where it runs

- It is a **Flask (Python) web application**.
- It is hosted on **Microsoft Azure (App Service)** inside the company environment,
  so it can reach the company databases securely.
- Database credentials and other secrets are kept out of the code and supplied
  through secure environment settings.

## The main pieces

| File | What it's responsible for |
| --- | --- |
| `app.py` | The web app itself — pages, login, and the overall flow. |
| `database.py` | Securely connecting to the databases and reading their structure. |
| `compare.py` | Comparing Preprod vs Production and building the SQL change scripts. |
| `html_report.py` | Turning the comparison into the on-screen tables and the email. |
| `email_service.py` | Sending the report email (with the SQL script attached). |
| `templates/` | The pages you see (dashboard and the login/sign-up screen). |
| `static/styles.css` | The look and feel of the app. |

## What's next

The next major piece of work is to **apply the column changes to the data
warehouses** — updating the relevant warehouse tables and stored procedures to
match the database changes, rather than just reporting on them.

---

*Internal use only — MobiLife.*
