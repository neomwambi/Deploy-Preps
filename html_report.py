"""Render diff DataFrames to styled HTML for the web UI and email."""

from __future__ import annotations

import base64
import html
from pathlib import Path

import numpy as np
import pandas as pd

from compare import SchemaDiffResult

# Inline image for SMTP (Content-ID must match <img src="cid:...">).
SIGNATURE_BANNER_CID = "mobilife-signature-banner"
SIGNATURE_BANNER_FILENAME = "Award signature 33 (1).jpg"

_CONFIDENTIALITY_NOTICE = (
    "The e-mail and attachments are confidential and intended only for selected recipients. "
    "If you have received it in error, you may not in any way disclose or rely on the contents. "
    "You may not keep, copy or distribute the e-mail. Should you receive it, immediately notify "
    "the sender of the error and delete the e-mail. Also note that this form of communication is "
    "not secure, it can be intercepted, and may not necessarily be free of errors and viruses in "
    "spite of reasonable efforts to secure this medium. Any views and opinions expressed herein may "
    "not necessarily be those of the company. The aforementioned does not accept any liability for any "
    "damage, loss or expense arising from this communication and/or from accessing any attachment."
)


def resolve_signature_banner_path() -> Path | None:
    """Banner JPEG next to the app or under static/ (same name the user added)."""
    base = Path(__file__).resolve().parent
    for rel in (
        SIGNATURE_BANNER_FILENAME,
        Path("static") / SIGNATURE_BANNER_FILENAME,
    ):
        p = base / rel
        if p.is_file():
            return p
    return None


def signature_banner_data_uri() -> str | None:
    """For browser email preview (iframe); avoids cid: which only works in real MIME mail."""
    path = resolve_signature_banner_path()
    if path is None:
        return None
    ext = path.suffix.lower()
    mime = "image/jpeg" if ext in (".jpg", ".jpeg") else "image/png"
    raw = path.read_bytes()
    b64 = base64.standard_b64encode(raw).decode("ascii")
    return f"data:{mime};base64,{b64}"


def _email_signoff_html(banner_src: str | None) -> str:
    img_block = ""
    if banner_src:
        safe_src = html.escape(banner_src, quote=True)
        img_block = (
            f'<div class="sig-banner"><img src="{safe_src}" alt="MobiLife" '
            'style="display:block;height:auto;border:0;margin:0;max-width:560px;width:100%;" /></div>'
        )
    return f"""
<div class="email-signature">
  <p>Regards</p>
  <p class="sig-name">Neo Mwambi</p>
  <p class="sig-title">Junior Data Analyst</p>
  {img_block}
  <p class="conf-title">Confidentiality</p>
  <p class="conf-body" style="width:100%;max-width:100%;margin:10px 0 0 0;font-size:10px;color:#666666;line-height:1.45;text-align:justify;">{_esc(_CONFIDENTIALITY_NOTICE)}</p>
</div>
"""


# Left-aligned, full width (many clients ignore margin:auto centering inconsistently).
_DOCUMENT_STYLES = """
html, body { margin: 0 !important; padding: 0 !important; width: 100% !important; text-align: left !important; }
body { font-family: Arial, Helvetica, "Segoe UI", system-ui, sans-serif; font-size: medium; line-height: 1.45; color: #1a1a1a; background: #ffffff; }
.wrap { width: 100% !important; max-width: 100% !important; margin: 0 !important; padding: 8px 12px 28px 0 !important; text-align: left !important; box-sizing: border-box; font-family: inherit; font-size: medium; }
.wrap > p, .wrap p { margin: 0 0 10px; text-align: left; font-family: inherit; font-size: medium; }
h1 { font-size: medium; font-weight: bold; margin: 0 0 8px; text-align: left; font-family: inherit; }
.sub { color: #5c6470; margin: 0 0 20px; font-size: medium; text-align: left; font-family: inherit; }
.section { margin-top: 26px; text-align: left; width: 100%; max-width: 100%; font-family: inherit; font-size: medium; }
.section h2 { font-size: medium; font-weight: bold; margin: 0 0 10px; text-align: left; font-family: inherit; }
.diff-table-wrap { display: inline-block; max-width: 100%; vertical-align: top; }
table.diff { width: auto !important; max-width: 100% !important; table-layout: fixed; border-collapse: collapse; background: #ffffff; border: 1px solid #000000; margin: 0; font-family: inherit; font-size: medium; font-weight: normal; box-shadow: none; border-radius: 0; }
table.diff th, table.diff td { border: 1px solid #000000; padding: 2px 6px; color: #000000; vertical-align: middle; text-align: left; box-sizing: border-box; word-wrap: break-word; overflow-wrap: break-word; }
table.diff th { background: #8EA9DB; font-weight: bold; }
table.diff td { background: #ffffff; font-weight: normal; }
table.diff th.excel-c, table.diff td.excel-c { text-align: center; }
.empty { color: #6b7280; font-style: italic; padding: 8px 0; text-align: left; font-family: inherit; font-size: medium; }
.email-signature { margin-top: 28px; padding-top: 20px; border-top: 1px solid #e5e7eb; font-family: inherit; font-size: medium; line-height: 1.45; color: #1a1a1a; text-align: left; width: 100% !important; max-width: 100% !important; box-sizing: border-box; }
.email-signature > p { margin: 0; text-align: left; font-family: inherit; font-size: medium; }
.email-signature .sig-name { margin-top: 10px; font-weight: bold; font-size: medium; }
.email-signature .sig-title { margin-top: 4px; color: #1a1a1a; font-size: medium; font-weight: normal; }
.email-signature .sig-banner { margin-top: 18px; display: block; max-width: 560px; width: 100%; }
.email-signature .sig-banner img { display: block; max-width: 560px; width: 100%; height: auto; border: 0; margin: 0; }
.email-signature .conf-title { margin-top: 20px; font-weight: 700; font-size: 11px; color: #111827; text-align: left; }
.email-signature .conf-body { margin-top: 10px; font-size: 10px; color: #666666; line-height: 1.45; text-align: justify; width: 100% !important; max-width: 100% !important; box-sizing: border-box; }
"""

_COL_NEW_ADDED = "New Columns Added"
_COL_REMOVED = "Columns Removed"


def _esc(v: object) -> str:
    if v is None:
        return ""
    if isinstance(v, float) and np.isnan(v):
        return ""
    if pd.isna(v):
        return ""
    return html.escape(str(v), quote=True)


def _is_blank_cell(v: object) -> bool:
    if v is None:
        return True
    if isinstance(v, float) and np.isnan(v):
        return True
    if pd.isna(v):
        return True
    s = str(v).strip()
    return s == "" or s.lower() in {"nan", "none"}


def _column_all_blank(df: pd.DataFrame, col: str) -> bool:
    if col not in df.columns:
        return True
    return bool(df[col].map(_is_blank_cell).all())


def _prune_table2_columns(df: pd.DataFrame) -> pd.DataFrame:
    """Drop New Columns Added / Columns Removed when that column has no data anywhere."""
    if df.empty:
        return df
    out = df.copy()
    if _COL_NEW_ADDED in out.columns and _column_all_blank(out, _COL_NEW_ADDED):
        out = out.drop(columns=[_COL_NEW_ADDED])
    if _COL_REMOVED in out.columns and _column_all_blank(out, _COL_REMOVED):
        out = out.drop(columns=[_COL_REMOVED])
    return out


def _iter_dynamic_sections(result: SchemaDiffResult) -> list[tuple[str, pd.DataFrame]]:
    """Build (title, dataframe) pairs only for non-empty sections; field table columns pruned when unused."""
    sections: list[tuple[str, pd.DataFrame]] = []

    if not result.table1_new_tables.empty:
        sections.append(("Table(s) added", result.table1_new_tables))

    t2 = _prune_table2_columns(result.table2_field_changes)
    if not t2.empty:
        sections.append(("Field(s) added / removed", t2))

    if not result.table3_index_added.empty:
        sections.append(("Index added to the following fields", result.table3_index_added))

    if not result.table4_datatype_changes.empty:
        sections.append(("New DataType / lengths", result.table4_datatype_changes))

    return sections


# Columns that Excel would typically centre (numeric).
_EXCEL_CENTER_COLUMNS = frozenset({"Prod Table Rows", "Prod Table Size (GB)"})


def _excel_col_class(col_name: object) -> str:
    if str(col_name).strip() in _EXCEL_CENTER_COLUMNS:
        return ' class="excel-c"'
    return ""


def _column_width_ch(col: str, df: pd.DataFrame) -> int:
    """Width hint: longest cell or header (as sent in HTML) plus two character widths."""
    longest = len(_esc(str(col)))
    series = df[col] if col in df.columns else None
    if series is not None:
        for v in series:
            longest = max(longest, len(_esc(v)))
    return max(1, longest + 2)


def _section_to_html(df: pd.DataFrame, section_title: str) -> str:
    if df.empty:
        return ""

    cols = list(df.columns)
    col_ch = {c: _column_width_ch(c, df) for c in cols}

    thead = "<tr>" + "".join(
        f'<th{_excel_col_class(c)} style="width:{col_ch[c]}ch;">{_esc(c)}</th>' for c in cols
    ) + "</tr>"
    rows_html: list[str] = []
    for _, row in df.iterrows():
        cells: list[str] = []
        for c in cols:
            cc = _excel_col_class(c)
            cells.append(f"<td{cc}>{_esc(row.get(c, ''))}</td>")
        rows_html.append("<tr>" + "".join(cells) + "</tr>")

    table = (
        '<div class="diff-table-wrap">'
        f'<table class="diff" aria-label="{_esc(section_title)}" '
        'style="width:auto;max-width:100%;table-layout:fixed;border-collapse:collapse;'
        'border:1px solid #000000;margin:0;">'
        f"<thead>{thead}</thead><tbody>{''.join(rows_html)}</tbody></table>"
        "</div>"
    )
    return f'<div class="section"><h2>{_esc(section_title)}</h2>{table}</div>'


def _render_sections_list(sections: list[tuple[str, pd.DataFrame]]) -> str:
    if not sections:
        return '<p class="empty">No schema differences were detected for this comparison.</p>'
    return "\n".join(_section_to_html(df, title) for title, df in sections)


def render_report_html(result: SchemaDiffResult, include_document_wrapper: bool = True) -> str:
    sections = _iter_dynamic_sections(result)
    inner = _render_sections_list(sections)
    if not include_document_wrapper:
        return inner
    meta = ""
    if result.preprod_row_count or result.prod_row_count:
        meta = (
            f"<p class=\"sub\">Loaded {result.preprod_row_count:,} preprod column rows and "
            f"{result.prod_row_count:,} production column rows.</p>"
        )
    return f"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="utf-8" />
<meta name="viewport" content="width=device-width, initial-scale=1" />
<title>Schema diff preview</title>
<style>{_DOCUMENT_STYLES}</style>
</head>
<body style="margin:0;padding:0;text-align:left;width:100%;">
  <div class="wrap" style="width:100%;max-width:100%;margin:0;text-align:left;">
    <h1>Schema change preview</h1>
    {meta}
    {inner}
  </div>
</body>
</html>"""


def render_report_email_html(
    result: SchemaDiffResult,
    *,
    for_browser_preview: bool = False,
) -> str:
    """
    Build the HTML email body.

    When ``for_browser_preview`` is True, the banner uses a ``data:`` URI so the in-app iframe can
    display it. When False (SMTP), the banner uses ``cid:...`` if ``Award signature 33 (1).jpg`` exists.
    """
    sections = _iter_dynamic_sections(result)
    inner = _render_sections_list(sections)
    if not sections:
        intro = (
            "<p>Good day all,</p>"
            "<p>There are <strong>no database schema changes</strong> to report for this comparison.</p>"
        )
    else:
        intro = (
            "<p>Good day all,</p>"
            "<p>Please see below the changes affecting the database after deployment.</p>"
        )

    if for_browser_preview:
        banner_src = signature_banner_data_uri()
    elif resolve_signature_banner_path() is not None:
        banner_src = f"cid:{SIGNATURE_BANNER_CID}"
    else:
        banner_src = None

    signoff = _email_signoff_html(banner_src)
    return f"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="utf-8" />
<meta name="viewport" content="width=device-width, initial-scale=1" />
<style>{_DOCUMENT_STYLES}</style>
</head>
<body style="margin:0;padding:0;text-align:left;width:100%;">
<table role="presentation" width="100%" cellpadding="0" cellspacing="0" border="0" style="width:100%;border-collapse:collapse;mso-table-lspace:0pt;mso-table-rspace:0pt;">
<tr>
<td align="left" valign="top" width="100%" style="padding:0;margin:0;text-align:left;width:100%;">
  <div class="wrap" style="width:100%;max-width:100%;margin:0;padding:8px 12px 32px 0;text-align:left;box-sizing:border-box;">
{intro}
{inner}
{signoff}
  </div>
</td>
</tr>
</table>
</body>
</html>"""
