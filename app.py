from __future__ import annotations

import asyncio
import json
from html import escape
import time
from datetime import datetime
from typing import Any

import pandas as pd
import streamlit as st

from crawler_core import CrawlEngine, parse_input_lines
from storage import (
    DEFAULT_SETTINGS,
    append_log,
    append_results,
    clear_cache,
    clear_logs,
    clear_results,
    ensure_dirs,
    import_backup_json,
    load_raw_hits,
    load_results,
    load_settings,
    get_storage_counts,
    read_logs,
    reset_all_local_data,
    rows_to_csv_bytes,
    rows_to_json_bytes,
    save_settings,
    update_results_from_table,
)


st.set_page_config(
    page_title="Educational Group Finder",
    page_icon="🔎",
    layout="wide",
    initial_sidebar_state="expanded",
)

ensure_dirs()


CUSTOM_CSS = """
<style>
    .block-container {padding-top: 1.25rem; padding-bottom: 3rem;}
    .metric-card {
        border: 1px solid #eeeeee;
        border-radius: 18px;
        padding: 14px 16px;
        background: #ffffff;
        box-shadow: 0 1px 12px rgba(0,0,0,0.035);
    }
    .small-muted {font-size: 0.86rem; color: #666;}
    .danger-note {
        border: 1px solid #f1d0d0;
        background: #fff7f7;
        padding: 12px 14px;
        border-radius: 14px;
        color: #4a1111;
    }
    .ok-note {
        border: 1px solid #d8ead8;
        background: #f7fff7;
        padding: 12px 14px;
        border-radius: 14px;
        color: #133d13;
    }
</style>
"""
st.markdown(CUSTOM_CSS, unsafe_allow_html=True)


def init_state() -> None:
    st.session_state.setdefault("last_stats", {})
    st.session_state.setdefault("last_run_results", [])
    st.session_state.setdefault("crawl_running", False)
    st.session_state.setdefault("last_message", "")
    st.session_state.setdefault("event_logs", [])


init_state()


def settings_sidebar() -> dict[str, Any]:
    current = load_settings()

    with st.sidebar:
        st.title("Settings")

        st.caption("Designed for free Streamlit: HTTP first, browser fallback only when needed.")

        st.caption("Use 0 for no app-level limit. Free Streamlit can still stop heavy runs if CPU/RAM/storage is abused.")

        max_pages_total = st.number_input(
            "Max pages total (0 = unlimited)",
            min_value=0,
            max_value=1_000_000,
            value=int(current.get("max_pages_total", DEFAULT_SETTINGS["max_pages_total"])),
            step=100,
        )

        max_depth = st.number_input(
            "Max depth (0 = unlimited)",
            min_value=0,
            max_value=1000,
            value=int(current.get("max_depth", DEFAULT_SETTINGS["max_depth"])),
            step=1,
        )

        max_pages_per_domain = st.number_input(
            "Max pages per domain (0 = unlimited)",
            min_value=0,
            max_value=1_000_000,
            value=int(current.get("max_pages_per_domain", DEFAULT_SETTINGS["max_pages_per_domain"])),
            step=100,
        )

        http_concurrency = st.number_input(
            "HTTP concurrency",
            min_value=1,
            max_value=200,
            value=int(current.get("http_concurrency", DEFAULT_SETTINGS["http_concurrency"])),
            step=1,
        )

        browser_concurrency = st.number_input(
            "Browser concurrency",
            min_value=0,
            max_value=10,
            value=int(current.get("browser_concurrency", DEFAULT_SETTINGS["browser_concurrency"])),
            step=1,
        )

        use_browser_fallback = st.toggle(
            "Use Playwright JS fallback",
            value=bool(current.get("use_browser_fallback", DEFAULT_SETTINGS["use_browser_fallback"])),
        )

        same_domain_only = st.toggle(
            "Follow same-domain funnel only",
            value=bool(current.get("same_domain_only", DEFAULT_SETTINGS["same_domain_only"])),
        )

        request_delay = st.slider(
            "Delay per request",
            min_value=0.0,
            max_value=2.0,
            value=float(current.get("request_delay", DEFAULT_SETTINGS["request_delay"])),
            step=0.05,
        )

        http_timeout = st.slider(
            "HTTP timeout",
            min_value=5.0,
            max_value=40.0,
            value=float(current.get("http_timeout", DEFAULT_SETTINGS["http_timeout"])),
            step=1.0,
        )

        browser_timeout_ms = st.slider(
            "Browser timeout ms",
            min_value=5000,
            max_value=40000,
            value=int(current.get("browser_timeout_ms", DEFAULT_SETTINGS["browser_timeout_ms"])),
            step=1000,
        )

        browser_steps = st.slider(
            "Browser funnel steps",
            min_value=1,
            max_value=30,
            value=int(current.get("browser_steps", DEFAULT_SETTINGS["browser_steps"])),
        )

        ajax_wait_seconds = st.slider(
            "AJAX wait seconds",
            min_value=1.0,
            max_value=20.0,
            value=float(current.get("ajax_wait_seconds", DEFAULT_SETTINGS.get("ajax_wait_seconds", 8.0))),
            step=0.5,
        )

        scroll_rounds = st.slider(
            "Lazy scroll rounds",
            min_value=1,
            max_value=60,
            value=int(current.get("scroll_rounds", DEFAULT_SETTINGS.get("scroll_rounds", 10))),
            step=1,
        )

        settings = {
            "max_depth": int(max_depth),
            "max_pages_total": int(max_pages_total),
            "max_pages_per_domain": int(max_pages_per_domain),
            "http_concurrency": int(http_concurrency),
            "browser_concurrency": int(browser_concurrency),
            "http_timeout": float(http_timeout),
            "request_delay": float(request_delay),
            "use_browser_fallback": bool(use_browser_fallback),
            "browser_timeout_ms": int(browser_timeout_ms),
            "browser_steps": int(browser_steps),
            "ajax_wait_seconds": float(ajax_wait_seconds),
            "scroll_rounds": int(scroll_rounds),
            "same_domain_only": bool(same_domain_only),
            "save_every_results": 5,
        }

        if st.button("Save Settings", use_container_width=True):
            save_settings(settings)
            append_log("INFO", "Settings saved from UI")
            st.success("Settings saved.")

        return settings


settings = settings_sidebar()



def metric_card(label: str, value: Any, help_text: str = "") -> str:
    label_safe = escape(str(label))
    value_safe = escape(str(value))
    help_safe = escape(str(help_text)) if help_text else ""
    help_html = f'<div class="gf-metric-help">{help_safe}</div>' if help_safe else ""
    return f"""
    <div class="gf-metric-card">
        <div class="gf-metric-label">{label_safe}</div>
        <div class="gf-metric-value">{value_safe}</div>
        {help_html}
    </div>
    """


def render_metric(col, label: str, value: Any, help_text: str = "") -> None:
    col.markdown(metric_card(label, value, help_text), unsafe_allow_html=True)


def metric_row(stats: dict[str, Any]) -> None:
    cols = st.columns(8)
    values = [
        ("Visited", stats.get("visited", 0)),
        ("Queued", stats.get("queued", 0)),
        ("Running", stats.get("running", 0)),
        ("Raw hits", stats.get("raw_found", stats.get("found", 0))),
        ("Unique", stats.get("unique_found", stats.get("found", 0))),
        ("Duplicates", stats.get("duplicates", 0)),
        ("Browser", stats.get("browser_rendered", 0)),
        ("Elapsed", f"{stats.get('elapsed', 0)}s"),
    ]

    for col, (label, value) in zip(cols, values):
        render_metric(col, label, value)



def dataframe_from_results(rows: list[dict[str, Any]]) -> pd.DataFrame:
    if not rows:
        return pd.DataFrame(columns=[
            "selected",
            "invite_url",
            "normalized_url",
            "source_page",
            "source_domain",
            "source_query",
            "discovered_at",
            "extraction_method",
            "review_status",
            "keep_status",
            "tags",
            "notes",
        ])

    df = pd.DataFrame(rows)

    for col in [
        "selected",
        "invite_url",
        "normalized_url",
        "source_page",
        "source_domain",
        "source_query",
        "discovered_at",
        "extraction_method",
        "review_status",
        "keep_status",
        "tags",
        "notes",
    ]:
        if col not in df.columns:
            df[col] = False if col == "selected" else ""

    df["selected"] = df["selected"].fillna(False).astype(bool)

    preferred = [
        "selected",
        "invite_url",
        "source_page",
        "source_domain",
        "source_query",
        "discovered_at",
        "extraction_method",
        "review_status",
        "keep_status",
        "tags",
        "notes",
        "normalized_url",
        "click_text",
        "raw_url",
        "saved_at",
        "id",
    ]

    other_cols = [c for c in df.columns if c not in preferred]
    return df[[c for c in preferred if c in df.columns] + other_cols]


async def run_crawl_with_ui(seeds: list[str], settings: dict[str, Any]) -> None:
    status_box = st.empty()
    metrics_box = st.empty()
    current_box = st.empty()
    results_box = st.empty()
    log_box = st.empty()
    progress_bar = st.progress(0)

    save_counter = {"count": 0}

    def on_event(event: dict[str, Any]):
        event_type = event.get("type")
        stats = event.get("stats", {})
        st.session_state["last_stats"] = stats

        if event_type == "log":
            append_log(event.get("level", "INFO"), event.get("message", ""), **{k: v for k, v in event.items() if k not in {"type", "level", "message", "stats"}})
            st.session_state["event_logs"].append(event)

        if event_type == "results":
            rows = event.get("results", []) or []
            save_info = append_results(rows)
            save_counter["count"] += int(save_info.get("unique_added", 0))
            st.session_state["last_run_results"].extend(rows)
            append_log(
                "INFO",
                event.get(
                    "message",
                    f"Saved raw={save_info.get('raw_added', 0)}, unique_new={save_info.get('unique_added', 0)}"
                ),
                raw_added=save_info.get("raw_added", 0),
                unique_added=save_info.get("unique_added", 0),
                duplicate_rows=save_info.get("duplicates", 0),
                total_raw=save_info.get("total_raw", 0),
                total_unique=save_info.get("total_unique", 0),
            )

        if event.get("message"):
            st.session_state["last_message"] = event.get("message")

        status = stats.get("status", event.get("status", "running"))
        current = event.get("current_url") or stats.get("current_url", "")

        counts = get_storage_counts()
        status_box.info(
            f"Status: {status} | {st.session_state.get('last_message', '')} "
            f"| Stored raw: {counts['raw_saved']} | Stored unique: {counts['unique_saved']}"
        )

        with metrics_box.container():
            metric_row(stats)

        current_box.caption(f"Current page: {current}")

        visited = int(stats.get("visited", 0) or 0)
        max_total_setting = int(settings.get("max_pages_total", 0) or 0)
        if max_total_setting > 0:
            progress_bar.progress(min(1.0, visited / max_total_setting))
        else:
            progress_bar.progress(0.0)

        live_rows = load_results()
        if live_rows:
            live_df = dataframe_from_results(live_rows).tail(25)
            results_box.dataframe(live_df, use_container_width=True, height=360)

        recent_logs = read_logs(limit=8)
        if recent_logs:
            log_df = pd.DataFrame(recent_logs)
            log_box.dataframe(log_df.tail(8), use_container_width=True, height=220)

    engine = CrawlEngine(settings=settings, on_event=on_event)
    await engine.run(seeds)

    append_log("INFO", "Crawl UI run finished")
    st.session_state["crawl_running"] = False


def apply_filters(rows: list[dict[str, Any]], key_prefix: str) -> list[dict[str, Any]]:
    if not rows:
        return []

    df = dataframe_from_results(rows)

    c1, c2, c3, c4 = st.columns(4)

    with c1:
        domain_options = ["All"] + sorted([x for x in df["source_domain"].dropna().unique().tolist() if x])
        domain = st.selectbox("Domain", domain_options, key=f"{key_prefix}_domain")

    with c2:
        method_options = ["All"] + sorted([x for x in df["extraction_method"].dropna().unique().tolist() if x])
        method = st.selectbox("Method", method_options, key=f"{key_prefix}_method")

    with c3:
        review_options = ["All"] + sorted([x for x in df["review_status"].dropna().unique().tolist() if x])
        review = st.selectbox("Review", review_options, key=f"{key_prefix}_review")

    with c4:
        keep_options = ["All"] + sorted([x for x in df["keep_status"].dropna().unique().tolist() if x])
        keep = st.selectbox("Keep", keep_options, key=f"{key_prefix}_keep")

    search = st.text_input("Search in URL/source/tags/notes", "", key=f"{key_prefix}_search")

    filtered = df.copy()

    if domain != "All":
        filtered = filtered[filtered["source_domain"] == domain]
    if method != "All":
        filtered = filtered[filtered["extraction_method"] == method]
    if review != "All":
        filtered = filtered[filtered["review_status"] == review]
    if keep != "All":
        filtered = filtered[filtered["keep_status"] == keep]

    if search.strip():
        q = search.strip().lower()
        searchable = filtered.astype(str).agg(" ".join, axis=1).str.lower()
        filtered = filtered[searchable.str.contains(q, regex=False)]

    st.caption(f"Showing {len(filtered)} of {len(df)} saved rows.")
    return filtered.to_dict("records")


st.markdown(
    """
    <section class="gf-hero">
      <div class="gf-hero-inner">
        <div>
          <div class="gf-kicker">PUBLIC PAGE CRAWLER · STREAMLIT CLOUD READY</div>
          <h1 class="gf-title">Group Finder Control Room</h1>
          <p class="gf-subtitle">
            A polished educational crawler dashboard for public pages: HTTP extraction, AJAX/network capture,
            load-more interaction, popup scanning, review controls, and clean CSV/JSON exports.
          </p>
          <div class="gf-badges">
            <span class="gf-badge">HTTP first</span>
            <span class="gf-badge">JS fallback</span>
            <span class="gf-badge">AJAX response scan</span>
            <span class="gf-badge">Load-more clicks</span>
            <span class="gf-badge">Raw + unique exports</span>
          </div>
        </div>
      </div>
    </section>
    """,
    unsafe_allow_html=True,
)

tab_dashboard, tab_crawl, tab_results, tab_exports, tab_logs = st.tabs([
    "Dashboard",
    "Inputs & Crawl",
    "Found Links",
    "Exports",
    "Logs & Maintenance",
])


with tab_dashboard:
    rows = load_results()
    raw_rows = load_raw_hits()
    df = dataframe_from_results(rows)

    st.subheader("Dashboard")

    metric_row(st.session_state.get("last_stats", {}))

    c1, c2, c3, c4 = st.columns(4)
    render_metric(c1, "Raw discoveries saved", len(raw_rows))
    render_metric(c2, "Unique saved links", len(rows))
    render_metric(c3, "Unique domains", df["source_domain"].nunique() if not df.empty else 0)
    render_metric(c4, "Unreviewed", int((df["review_status"] == "unreviewed").sum()) if not df.empty else 0)

    st.markdown(
        """
        <div class="ok-note">
        This version stores both raw discoveries and unique normalized links. The browser fallback also scans AJAX/fetch response bodies, clicks scored load-more/show-more/join controls, scrolls lazy lists, and rescans popups/modals.
        </div>
        """,
        unsafe_allow_html=True,
    )

    if rows:
        st.dataframe(df.tail(20), use_container_width=True, height=430)
    else:
        st.info("No saved results yet. Start from Inputs & Crawl.")


with tab_crawl:
    st.subheader("Inputs & Crawl")

    st.markdown(
        """
        <div class="gf-note">
        Paste seed URLs below. For AJAX-heavy sites, keep Playwright fallback enabled. The crawler will scan
        public HTML first, then use browser rendering to wait 5–8+ seconds, observe AJAX/fetch responses,
        scroll lazy-loaded pages, click load-more/show-more/join controls, and rescan popup content.
        </div>
        """,
        unsafe_allow_html=True,
    )

    seed_text = st.text_area(
        "Seed URLs",
        height=220,
        placeholder="https://example.com/group/invite/abc\nhttps://example.com/category/jobs",
    )

    uploaded = st.file_uploader("Optional: upload TXT/CSV with URLs", type=["txt", "csv"])

    uploaded_text = ""
    if uploaded is not None:
        uploaded_text = uploaded.getvalue().decode("utf-8", errors="ignore")

    seeds = parse_input_lines(seed_text + "\n" + uploaded_text)

    col_a, col_b, col_c = st.columns([1, 1, 2])

    with col_a:
        start = st.button("Start Crawl", type="primary", use_container_width=True)

    with col_b:
        dry_count = st.button("Count Inputs", use_container_width=True)

    with col_c:
        st.caption(f"Detected {len(seeds)} possible seed URL(s).")

    if dry_count:
        st.write(seeds[:100])

    if start:
        if not seeds:
            st.error("Paste at least one seed URL.")
        else:
            st.session_state["crawl_running"] = True
            st.session_state["last_run_results"] = []
            append_log("INFO", f"User started crawl with {len(seeds)} raw input lines")

            try:
                asyncio.run(run_crawl_with_ui(seeds, settings))
                st.success("Crawl finished.")
            except RuntimeError:
                # Some hosts already have a running event loop.
                loop = asyncio.new_event_loop()
                asyncio.set_event_loop(loop)
                loop.run_until_complete(run_crawl_with_ui(seeds, settings))
                st.success("Crawl finished.")
            except Exception as exc:
                st.session_state["crawl_running"] = False
                append_log("ERROR", f"Crawl crashed: {exc}")
                st.error(f"Crawl crashed: {exc}")


with tab_results:
    st.subheader("Found Links Management")

    rows = load_results()
    filtered_rows = apply_filters(rows, key_prefix="found_links")
    df = dataframe_from_results(filtered_rows)

    if df.empty:
        st.info("No matching results.")
    else:
        edited = st.data_editor(
            df,
            use_container_width=True,
            height=520,
            num_rows="fixed",
            column_config={
                "selected": st.column_config.CheckboxColumn("Select"),
                "review_status": st.column_config.SelectboxColumn(
                    "Review",
                    options=["unreviewed", "reviewed", "needs_check"],
                ),
                "keep_status": st.column_config.SelectboxColumn(
                    "Keep",
                    options=["keep", "remove", "duplicate"],
                ),
                "invite_url": st.column_config.LinkColumn("Invite URL"),
                "source_page": st.column_config.LinkColumn("Source Page"),
            },
            disabled=[
                "invite_url",
                "source_page",
                "source_domain",
                "source_query",
                "discovered_at",
                "extraction_method",
                "normalized_url",
                "click_text",
                "raw_url",
                "saved_at",
                "id",
            ],
        )

        c1, c2, c3, c4 = st.columns(4)

        if c1.button("Save Edits", use_container_width=True):
            original = dataframe_from_results(rows)
            edited_records = edited.to_dict("records")

            # Merge edited filtered rows back into full saved table by normalized_url/id.
            edited_by_key = {
                str(r.get("id") or r.get("normalized_url") or r.get("invite_url")): r
                for r in edited_records
            }

            merged = []
            for row in original.to_dict("records"):
                key = str(row.get("id") or row.get("normalized_url") or row.get("invite_url"))
                if key in edited_by_key:
                    row.update(edited_by_key[key])
                merged.append(row)

            update_results_from_table(merged)
            append_log("INFO", "Manager edits saved")
            st.success("Saved.")

        selected_count = int(edited["selected"].sum()) if "selected" in edited.columns else 0

        if c2.button("Mark Selected Reviewed", use_container_width=True):
            all_rows = dataframe_from_results(rows)
            selected_keys = set(
                edited.loc[edited["selected"] == True, "normalized_url"].astype(str).tolist()
            )
            all_rows.loc[all_rows["normalized_url"].astype(str).isin(selected_keys), "review_status"] = "reviewed"
            all_rows.loc[all_rows["normalized_url"].astype(str).isin(selected_keys), "selected"] = False
            update_results_from_table(all_rows.to_dict("records"))
            append_log("INFO", f"Marked {len(selected_keys)} row(s) reviewed")
            st.success(f"Marked {len(selected_keys)} selected row(s) reviewed.")

        if c3.button("Mark Selected Remove", use_container_width=True):
            all_rows = dataframe_from_results(rows)
            selected_keys = set(
                edited.loc[edited["selected"] == True, "normalized_url"].astype(str).tolist()
            )
            all_rows.loc[all_rows["normalized_url"].astype(str).isin(selected_keys), "keep_status"] = "remove"
            all_rows.loc[all_rows["normalized_url"].astype(str).isin(selected_keys), "selected"] = False
            update_results_from_table(all_rows.to_dict("records"))
            append_log("INFO", f"Marked {len(selected_keys)} row(s) remove")
            st.success(f"Marked {len(selected_keys)} selected row(s) remove.")

        if c4.button("Clear Selection", use_container_width=True):
            all_rows = dataframe_from_results(rows)
            all_rows["selected"] = False
            update_results_from_table(all_rows.to_dict("records"))
            st.success("Selection cleared.")

        st.caption(f"Selected rows: {selected_count}")


with tab_exports:
    st.subheader("Exports")

    rows = load_results()
    raw_rows = load_raw_hits()
    filtered_rows = apply_filters(rows, key_prefix="exports")

    c0, c1, c2, c3 = st.columns(4)

    unique_rows = []
    seen = set()
    for row in filtered_rows:
        key = str(row.get("normalized_url") or row.get("invite_url"))
        if key and key not in seen:
            unique_rows.append(row)
            seen.add(key)

    reviewed_rows = [r for r in filtered_rows if r.get("review_status") == "reviewed"]
    kept_rows = [r for r in filtered_rows if r.get("keep_status") == "keep"]

    with c0:
        render_metric(st, "Raw discoveries", len(raw_rows))
        st.download_button(
            "Download ALL RAW CSV",
            data=rows_to_csv_bytes(raw_rows),
            file_name=f"group_finder_ALL_RAW_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv",
            mime="text/csv",
            use_container_width=True,
        )
        st.download_button(
            "Download ALL RAW JSON",
            data=rows_to_json_bytes(raw_rows),
            file_name=f"group_finder_ALL_RAW_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json",
            mime="application/json",
            use_container_width=True,
        )

    with c1:
        render_metric(st, "Filtered unique", len(filtered_rows))
        st.download_button(
            "Download Filtered Unique CSV",
            data=rows_to_csv_bytes(filtered_rows),
            file_name=f"group_finder_filtered_unique_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv",
            mime="text/csv",
            use_container_width=True,
        )
        st.download_button(
            "Download Filtered Unique JSON",
            data=rows_to_json_bytes(filtered_rows),
            file_name=f"group_finder_filtered_unique_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json",
            mime="application/json",
            use_container_width=True,
        )

    with c2:
        render_metric(st, "All unique saved", len(rows))
        st.download_button(
            "Download ALL UNIQUE CSV",
            data=rows_to_csv_bytes(rows),
            file_name=f"group_finder_ALL_UNIQUE_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv",
            mime="text/csv",
            use_container_width=True,
        )
        st.download_button(
            "Download ALL UNIQUE JSON",
            data=rows_to_json_bytes(rows),
            file_name=f"group_finder_ALL_UNIQUE_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json",
            mime="application/json",
            use_container_width=True,
        )

    with c3:
        render_metric(st, "Reviewed / Kept", f"{len(reviewed_rows)} / {len(kept_rows)}")
        st.download_button(
            "Download Reviewed CSV",
            data=rows_to_csv_bytes(reviewed_rows),
            file_name=f"group_finder_reviewed_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv",
            mime="text/csv",
            use_container_width=True,
        )
        st.download_button(
            "Download Kept CSV",
            data=rows_to_csv_bytes(kept_rows),
            file_name=f"group_finder_kept_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv",
            mime="text/csv",
            use_container_width=True,
        )

    st.warning(
        "If the app says many raw hits but unique CSV is small, that is usually duplicates. "
        "Use ALL RAW export for every discovery event, and ALL UNIQUE export for cleaned links."
    )

    st.divider()

    st.subheader("Import Backup")
    backup = st.file_uploader("Upload previous JSON export", type=["json"], key="backup_importer")

    if backup is not None and st.button("Import Backup JSON", use_container_width=True):
        ok, msg = import_backup_json(backup.getvalue())
        if ok:
            append_log("INFO", msg)
            st.success(msg)
        else:
            append_log("ERROR", msg)
            st.error(msg)


with tab_logs:
    st.subheader("Logs & Maintenance")

    logs = read_logs(limit=500)
    if logs:
        st.dataframe(pd.DataFrame(logs), use_container_width=True, height=420)
    else:
        st.info("No logs yet.")

    st.divider()
    st.subheader("Maintenance")

    c1, c2, c3, c4 = st.columns(4)

    if c1.button("Clear Cache", use_container_width=True):
        clear_cache()
        st.success("Cache cleared.")

    if c2.button("Clear Logs", use_container_width=True):
        clear_logs()
        st.success("Logs cleared.")

    if c3.button("Clear Results", use_container_width=True):
        clear_results()
        st.success("Results cleared.")

    with c4:
        confirm = st.checkbox("Confirm reset all")
        if st.button("Reset All", use_container_width=True, disabled=not confirm):
            reset_all_local_data()
            st.success("All local data reset.")
