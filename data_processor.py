"""
Data Processor — fetches Google Sheets data and computes all marketing metrics.
No I/O side-effects beyond the initial sheet read; all computation is pure.
"""

import os
import logging
from datetime import datetime, timedelta
from typing import Optional

import numpy as np
import pandas as pd
import gspread
from google.oauth2.service_account import Credentials
from dotenv import load_dotenv

load_dotenv()
logger = logging.getLogger(__name__)

SCOPES = [
    "https://www.googleapis.com/auth/spreadsheets.readonly",
    "https://www.googleapis.com/auth/drive.readonly",
]

# ─── Google Sheets connector ─────────────────────────────────────────────────

def get_sheets_client() -> gspread.Client:
    # Prefer JSON string in env var (Vercel/cloud), fall back to local file
    creds_json = os.getenv("GOOGLE_CREDENTIALS_JSON")
    if creds_json:
        import json
        from google.oauth2.service_account import Credentials as _Creds
        info = json.loads(creds_json)
        creds = _Creds.from_service_account_info(info, scopes=SCOPES)
    else:
        creds_file = os.getenv("GOOGLE_CREDENTIALS_FILE", "credentials.json")
        creds = Credentials.from_service_account_file(creds_file, scopes=SCOPES)
    return gspread.authorize(creds)


def fetch_all_sheets() -> dict[str, pd.DataFrame]:
    """Fetch all 4 sheets in one pass and return as DataFrames."""
    client = get_sheets_client()
    spreadsheet_id = os.getenv("SPREADSHEET_ID")
    sh = client.open_by_key(spreadsheet_id)

    names = {
        "ads":          os.getenv("ADS_SHEET_NAME", "Ads"),
        "leads":        os.getenv("LEADS_SHEET_NAME", "Leads"),
        "appointments": os.getenv("APPOINTMENTS_SHEET_NAME", "Appointments"),
        "sales":        os.getenv("SALES_SHEET_NAME", "Sales"),
    }

    frames: dict[str, pd.DataFrame] = {}
    for key, sheet_name in names.items():
        try:
            ws = sh.worksheet(sheet_name)
            all_values = ws.get_all_values()
            if not all_values:
                frames[key] = pd.DataFrame()
                logger.info("Fetched sheet '%s' — 0 rows", sheet_name)
                continue
            headers = [c.strip().lower().replace(" ", "_") for c in all_values[0]]
            data = all_values[1:]
            df = pd.DataFrame(data, columns=headers)
            # Drop columns with empty or duplicate headers
            df = df.loc[:, df.columns != ""]
            df = df.loc[:, ~df.columns.duplicated()]
            frames[key] = df
            logger.info("Fetched sheet '%s' — %d rows", sheet_name, len(df))
        except gspread.WorksheetNotFound:
            logger.error("Sheet '%s' not found; using empty DataFrame.", sheet_name)
            frames[key] = pd.DataFrame()

    return frames


# ─── Column mapping helpers ──────────────────────────────────────────────────

# These map semantic names to likely column headers (normalised to lowercase_underscore)
ADS_COLS = {
    "date":        ["date", "data"],
    "campaign":    ["campaign_name", "campaign", "campanha", "nome_da_campanha"],
    "adset":       ["ad_set_name", "adset_name", "adset", "conjunto_de_anuncios", "nome_do_conjunto_de_anuncios"],
    "creative":    ["ad_name", "creative", "criativo", "nome_do_anuncio", "nome_do_criativo"],
    "spend":       ["spend", "amount_spent", "valor_gasto", "gasto", "spend_(cost,_amount_spent)"],
    "impressions": ["impressions", "impressoes"],
    "clicks":      ["clicks", "cliques", "action_link_clicks"],
    "reach":       ["reach_(estimated)", "reach", "alcance"],
}
LEADS_COLS = {
    "date":     ["data_padrão", "submitted_at", "date", "data"],
    "email":    ["qual_é_o_e-mail_você_mais_utiliza?", "email"],
    "campaign": ["utm_campaign", "campaign", "campaign_name", "campanha"],
    "adset":    ["utm_medium", "ad_set_name", "adset", "conjunto_de_anuncios"],
    "creative": ["utm_content", "ad_name", "creative", "criativo", "anuncio"],
}
APPT_COLS = {
    "date":     ["data_entrada_agendamento", "date", "data"],
    "email":    ["email"],
    "campaign": ["utm_campaign", "campaign", "campanha"],
    "adset":    ["utm_medium", "adset"],
    "creative": ["utm_content", "creative", "criativo"],
}
SALES_COLS = {
    "date":    ["data_compra", "date", "data"],
    "email":   ["e-mail_lead", "email"],
    "revenue": ["valor_total_da_venda_(ticket)", "revenue", "receita", "valor", "sale_value"],
    "campaign": ["utm_campaign", "campaign", "campanha"],
    "adset":   ["utm_medium", "adset"],
    "creative": ["utm_content", "creative", "criativo"],
    "product": ["produto"],
    "origin":  ["origem_macro", "origem", "origin"],
}


def _find_col(df: pd.DataFrame, candidates: list[str]) -> Optional[str]:
    for c in candidates:
        if c in df.columns:
            return c
    return None


def _resolve(df: pd.DataFrame, mapping: dict) -> dict[str, Optional[str]]:
    return {k: _find_col(df, v) for k, v in mapping.items()}


# ─── Core metric calculation ─────────────────────────────────────────────────

def safe_div(num, den):
    """Return num/den or 0.0 when den is zero."""
    return float(num) / float(den) if den and float(den) != 0 else 0.0


def _parse_dates(df: pd.DataFrame, col: str) -> pd.DataFrame:
    if col and col in df.columns:
        cleaned = df[col].astype(str).str.strip().str.strip("'\"")
        result = pd.to_datetime(cleaned, format="%Y-%m-%d", errors="coerce")
        mask = result.isna()
        if mask.any():
            result = result.where(~mask, pd.to_datetime(cleaned, format="%d/%m/%Y", errors="coerce"))
        mask = result.isna()
        if mask.any():
            result = result.where(~mask, pd.to_datetime(cleaned, format="%d/%m/%Y %H:%M:%S", errors="coerce"))
        mask = result.isna()
        if mask.any():
            result = result.where(~mask, pd.to_datetime(cleaned, format="%d-%m-%Y", errors="coerce"))
        mask = result.isna()
        if mask.any():
            result = result.where(~mask, pd.to_datetime(cleaned, format="%d-%m-%Y %H:%M:%S", errors="coerce"))
        mask = result.isna()
        if mask.any():
            result = result.where(~mask, pd.to_datetime(cleaned, dayfirst=True, errors="coerce"))
        df[col] = result
    return df


def _parse_number(val) -> float:
    s = str(val).strip()
    if not s or s in ("", "-", "nan", "None"):
        return 0.0
    # Keep only digits, dots, commas
    import re
    s = re.sub(r"[^\d,\.]", "", s)
    if not s:
        return 0.0
    last_comma = s.rfind(",")
    last_dot   = s.rfind(".")
    if last_comma > last_dot:
        # BR format: 1.234,56 or 34,84
        s = s.replace(".", "").replace(",", ".")
    else:
        # US format: 1,234.56 or 34.84
        s = s.replace(",", "")
    try:
        return float(s)
    except ValueError:
        return 0.0


def _to_numeric(df: pd.DataFrame, cols: list[str]) -> pd.DataFrame:
    for c in cols:
        if c and c in df.columns:
            df[c] = df[c].apply(_parse_number)
    return df


# ─── Performance score ───────────────────────────────────────────────────────

def _normalise_series(s: pd.Series, invert: bool = False) -> pd.Series:
    """Min-max normalise; invert=True for metrics where lower is better (CPL)."""
    mn, mx = s.min(), s.max()
    if mx == mn:
        return pd.Series([0.5] * len(s), index=s.index)
    norm = (s - mn) / (mx - mn)
    return (1 - norm) if invert else norm


def compute_performance_scores(df: pd.DataFrame) -> pd.DataFrame:
    """Add a 'performance_score' (0-100) and 'score_band' column."""
    if df.empty:
        return df

    has_roas = "roas" in df.columns and df["roas"].sum() > 0
    has_cpl  = "cpl"  in df.columns and df["cpl"].sum()  > 0
    has_ctr  = "ctr"  in df.columns and df["ctr"].sum()  > 0

    weights = {"roas": 0.45, "cpl": 0.35, "ctr": 0.20}
    score = pd.Series(0.0, index=df.index)

    if has_roas:
        score += _normalise_series(df["roas"],  invert=False) * weights["roas"]
    if has_cpl:
        score += _normalise_series(df["cpl"],   invert=True)  * weights["cpl"]
    if has_ctr:
        score += _normalise_series(df["ctr"],   invert=False) * weights["ctr"]

    df["performance_score"] = (score * 100).round().astype(int).clip(0, 100)

    def band(v):
        if v >= 80: return "excellent"
        if v >= 60: return "good"
        if v >= 40: return "average"
        if v >= 20: return "poor"
        return "critical"

    df["score_band"] = df["performance_score"].apply(band)
    return df


# ─── Main aggregation pipeline ───────────────────────────────────────────────

def build_dashboard_data(
    frames: dict[str, pd.DataFrame],
    date_start: Optional[str] = None,
    date_end:   Optional[str] = None,
) -> dict:
    # Default range: Dec 2025 to today
    if not date_start:
        date_start = "2025-12-01"
    if not date_end:
        date_end = datetime.utcnow().strftime("%Y-%m-%d")
    """
    Joins all 4 DataFrames, computes every metric, returns the full
    dashboard payload ready for JSON serialisation.
    """
    ads_df  = frames.get("ads",          pd.DataFrame()).copy()
    leads   = frames.get("leads",        pd.DataFrame()).copy()
    appts   = frames.get("appointments", pd.DataFrame()).copy()
    sales   = frames.get("sales",        pd.DataFrame()).copy()

    if ads_df.empty:
        return _empty_payload()

    # Resolve column names
    a = _resolve(ads_df, ADS_COLS)
    l = _resolve(leads,  LEADS_COLS)
    p = _resolve(appts,  APPT_COLS)
    s = _resolve(sales,  SALES_COLS)


    # Parse + clean ads
    ads_df = _parse_dates(ads_df, a["date"])
    ads_df = _to_numeric(ads_df, [a["spend"], a["impressions"], a["clicks"], a["reach"]])

    # Date filter
    if date_start:
        ads_df = ads_df[ads_df[a["date"]] >= pd.Timestamp(date_start)]
    if date_end:
        ads_df = ads_df[ads_df[a["date"]] <= pd.Timestamp(date_end)]

    # Parse leads/appts/sales
    if not leads.empty and l["email"] and l["campaign"]:
        logger.info("LEADS total rows: %d | email col: %s | campaign col: %s", len(leads), l["email"], l["campaign"])
        # Only traffic leads: utm_campaign must contain "PRO"
        leads = leads[leads[l["campaign"]].str.upper().str.contains("PRO", na=False)]
        logger.info("LEADS after PRO filter: %d rows", len(leads))
        leads = _parse_dates(leads, l["date"])
        nat = leads[l["date"]].isna().sum() if l["date"] and l["date"] in leads.columns else 0
        if l["date"] and l["date"] in leads.columns:
            raw_leads_dates = frames.get("leads", pd.DataFrame()).copy()
            if l["date"] in raw_leads_dates.columns:
                raw_col = raw_leads_dates[l["date"]].astype(str).str.strip().str.strip("'\"")
                parsed_tmp = pd.to_datetime(raw_col, dayfirst=True, errors="coerce")
                failed_samples = raw_col[parsed_tmp.isna()].head(5).tolist()
                logger.info("LEADS date NaT: %d | failing samples repr: %s", nat, [repr(x) for x in failed_samples])
            else:
                logger.info("LEADS date NaT: %d", nat)
        if date_start and l["date"] and l["date"] in leads.columns:
            before = len(leads)
            leads = leads[leads[l["date"]] >= pd.Timestamp(date_start)]
            logger.info("LEADS after date_start filter: %d (was %d)", len(leads), before)
        if date_end and l["date"] and l["date"] in leads.columns:
            before = len(leads)
            leads = leads[leads[l["date"]].dt.normalize() <= pd.Timestamp(date_end)]
            logger.info("LEADS after date_end filter: %d (was %d)", len(leads), before)
        leads["_email_norm"] = leads[l["email"]].str.strip().str.lower()
        leads["_campaign"]   = leads[l["campaign"]].str.strip().str.lower()
        logger.info("LEADS unique emails: %d", leads["_email_norm"].nunique())

    if not appts.empty and p["email"]:
        logger.info("APPTS total rows: %d | campaign col: %s", len(appts), p["campaign"])
        # Only count traffic appointments (those with utm_campaign filled)
        if p["campaign"] and p["campaign"] in appts.columns:
            appts = appts[appts[p["campaign"]].str.strip() != ""]
        logger.info("APPTS after utm filter: %d rows", len(appts))
        appts = _parse_dates(appts, p["date"])
        if date_start and p["date"] and p["date"] in appts.columns:
            appts = appts[appts[p["date"]] >= pd.Timestamp(date_start)]
        if date_end and p["date"] and p["date"] in appts.columns:
            appts = appts[appts[p["date"]].dt.normalize() <= pd.Timestamp(date_end)]
        appts["_email_norm"] = appts[p["email"]].str.strip().str.lower()

    if not sales.empty and s["email"]:
        # Filter: product = "Mentoria Optical PRO" AND origin = "Tráfego Pago"
        if s.get("product") and s["product"] in sales.columns:
            sales = sales[sales[s["product"]].str.strip().str.upper() == "MENTORIA OPTICAL PRO"]
        if s.get("origin") and s["origin"] in sales.columns:
            sales = sales[sales[s["origin"]].str.strip().str.upper() == "TRÁFEGO PAGO"]
        sales = _parse_dates(sales, s["date"])
        if date_start and s["date"] and s["date"] in sales.columns:
            sales = sales[sales[s["date"]] >= pd.Timestamp(date_start)]
        if date_end and s["date"] and s["date"] in sales.columns:
            sales = sales[sales[s["date"]].dt.normalize() <= pd.Timestamp(date_end)]
        sales = _to_numeric(sales, [s["revenue"]])
        sales["_email_norm"] = sales[s["email"]].str.strip().str.lower()

    # Build lead/appt/sale lookup dicts keyed by email (for joining into creative rows)
    lead_emails  = set(leads["_email_norm"].tolist()) if not leads.empty and "_email_norm" in leads.columns else set()
    appt_emails  = set(appts["_email_norm"].tolist()) if not appts.empty and "_email_norm" in appts.columns else set()
    sale_emails  = set(sales["_email_norm"].tolist()) if not sales.empty and "_email_norm" in sales.columns else set()
    sale_revenue = (
        sales.groupby("_email_norm")[s["revenue"]].sum().to_dict()
        if not sales.empty and s["revenue"] and s["revenue"] in sales.columns
        else {}
    )

    # Build campaign→adset→creative lead/appt/sale maps from leads sheet
    def _campaign_lead_map(df, email_col, campaign_col, adset_col=None, creative_col=None):
        if df.empty or email_col not in df.columns:
            return {}, {}, {}
        camp_leads   = {}
        adset_leads  = {}
        creat_leads  = {}
        for _, row in df.iterrows():
            em   = str(row.get(email_col, "")).strip().lower()
            camp = str(row.get(campaign_col, "")).strip().lower() if campaign_col else ""
            ads  = str(row.get(adset_col,   "")).strip().lower() if adset_col   else ""
            cre  = str(row.get(creative_col,"")).strip().lower() if creative_col else ""

            if camp:
                camp_leads.setdefault(camp, set()).add(em)
            if camp and ads:
                adset_leads.setdefault((camp, ads), set()).add(em)
            if camp and ads and cre:
                creat_leads.setdefault((camp, ads, cre), set()).add(em)
        return camp_leads, adset_leads, creat_leads

    camp_leads, adset_leads, creat_leads = _campaign_lead_map(
        leads if not leads.empty else pd.DataFrame(),
        "_email_norm",
        l.get("campaign") or "",
        l.get("adset") or "",
        l.get("creative") or "",
    )

    # Same for appointments (only campaign available)
    camp_appts = {}
    if not appts.empty and "_email_norm" in appts.columns and p["campaign"] and p["campaign"] in appts.columns:
        for _, row in appts.iterrows():
            em   = str(row["_email_norm"]).strip().lower()
            camp = str(row[p["campaign"]]).strip().lower()
            camp_appts.setdefault(camp, set()).add(em)
    logger.info("CAMP_APPTS keys (%d): %s", len(camp_appts), [repr(k) for k in list(camp_appts)])

    # Same for sales
    camp_sales   = {}
    camp_revenue = {}
    if not sales.empty and "_email_norm" in sales.columns:
        sale_camp_col = s.get("campaign") if s.get("campaign") in sales.columns else None
        for _, row in sales.iterrows():
            em   = str(row["_email_norm"]).strip().lower()
            rev  = float(row[s["revenue"]]) if s["revenue"] and s["revenue"] in sales.columns else 0.0
            camp = str(row.get(sale_camp_col, "")).strip().lower() if sale_camp_col else ""
            if camp:
                camp_sales.setdefault(camp, set()).add(em)
                camp_revenue[camp] = camp_revenue.get(camp, 0.0) + rev
    logger.info("CAMP_SALES keys (%d): %s", len(camp_sales), [repr(k) for k in list(camp_sales)])

    # ── Campaign level aggregation ────────────────────────────────────────────
    camp_col  = a["campaign"]
    adset_col = a["adset"]
    creat_col = a["creative"]

    if not camp_col:
        return _empty_payload()

    ads_df[camp_col] = ads_df[camp_col].str.strip()
    ads_df["_campaign_norm"] = ads_df[camp_col].str.lower()
    if adset_col:
        ads_df[adset_col] = ads_df[adset_col].str.strip()
        ads_df["_adset_norm"] = ads_df[adset_col].str.lower()
    if creat_col:
        ads_df[creat_col] = ads_df[creat_col].str.strip()
        ads_df["_creat_norm"] = ads_df[creat_col].str.lower()

    def _agg_ads(group_cols: list[str]):
        agg = {
            a["spend"]:       "sum",
            a["impressions"]: "sum",
            a["clicks"]:      "sum",
        }
        if a.get("reach") and a["reach"] in ads_df.columns:
            agg[a["reach"]] = "sum"
        if group_cols:
            return ads_df.groupby(group_cols).agg(agg).reset_index()
        return ads_df.agg(agg).to_frame().T

    # ── Global totals from source sheets (independent of campaign name matching) ─
    total_leads = leads["_email_norm"].nunique() if not leads.empty and "_email_norm" in leads.columns else 0
    total_appts = appts["_email_norm"].nunique() if not appts.empty and "_email_norm" in appts.columns else 0
    total_sales = sales["_email_norm"].nunique() if not sales.empty and "_email_norm" in sales.columns else 0
    total_revenue_val = float(sales[s["revenue"]].sum()) if not sales.empty and s["revenue"] and s["revenue"] in sales.columns else 0.0
    logger.info("TOTALS → leads=%d appts=%d sales=%d revenue=%.2f", total_leads, total_appts, total_sales, total_revenue_val)

    # ── Build hierarchical table ──────────────────────────────────────────────
    performance_table = []
    total_spend = total_impressions = total_clicks = total_reach = 0.0

    # --- Campaign level ---
    camp_agg = _agg_ads(["_campaign_norm"])
    # Restore display name (first occurrence per normalized key)
    camp_agg[camp_col] = camp_agg["_campaign_norm"].map(
        ads_df.groupby("_campaign_norm")[camp_col].first()
    )
    camp_rows = []
    ads_keys  = set(camp_agg["_campaign_norm"].tolist())
    appt_keys = set(camp_appts.keys())
    sale_keys = set(camp_sales.keys())
    logger.info("ADS campaign keys (%d): %s", len(ads_keys), [repr(k) for k in sorted(ads_keys)])
    logger.info("APPTS ∩ ADS: %s", [repr(k) for k in sorted(appt_keys & ads_keys)])
    logger.info("SALES ∩ ADS: %s", [repr(k) for k in sorted(sale_keys & ads_keys)])

    for _, c_row in camp_agg.iterrows():
        ck = str(c_row["_campaign_norm"])
        c_spend  = float(c_row[a["spend"]])
        c_imp    = float(c_row[a["impressions"]])
        c_clicks = float(c_row[a["clicks"]])
        c_reach  = float(c_row[a["reach"]]) if a.get("reach") and a["reach"] in c_row.index else 0.0

        c_leads  = len(camp_leads.get(ck, set()))
        c_appts  = len(camp_appts.get(ck, set()))
        c_sales  = len(camp_sales.get(ck, set()))
        c_rev    = camp_revenue.get(ck, 0.0)
        if c_appts or c_sales:
            logger.info("CAMPAIGN MATCH ck=%r leads=%d appts=%d sales=%d rev=%.2f", ck, c_leads, c_appts, c_sales, c_rev)

        c_ctr  = safe_div(c_clicks, c_imp) * 100
        c_cpl  = safe_div(c_spend, c_leads)
        c_cac  = safe_div(c_spend, c_sales)
        c_roas = safe_div(c_rev, c_spend)
        c_cvr  = safe_div(c_leads, c_clicks) * 100
        c_appt_rate = safe_div(c_appts, c_leads) * 100
        c_close_rate = safe_div(c_sales, c_appts) * 100

        total_spend       += c_spend
        total_impressions += c_imp
        total_clicks      += c_clicks
        total_reach       += c_reach

        # --- Ad Set level ---
        adset_rows = []
        if adset_col and "_adset_norm" in ads_df.columns:
            camp_mask = ads_df["_campaign_norm"] == ck
            as_agg = ads_df[camp_mask].groupby("_adset_norm").agg(
                {a["spend"]: "sum", a["impressions"]: "sum", a["clicks"]: "sum"}
            ).reset_index()
            as_agg[adset_col] = as_agg["_adset_norm"].map(
                ads_df[camp_mask].groupby("_adset_norm")[adset_col].first()
            )

            for _, as_row in as_agg.iterrows():
                ask   = str(as_row["_adset_norm"])
                as_spend  = float(as_row[a["spend"]])
                as_imp    = float(as_row[a["impressions"]])
                as_clicks = float(as_row[a["clicks"]])

                as_lead_emails = adset_leads.get((ck, ask), set())
                as_leads = len(as_lead_emails)
                as_appts = len(as_lead_emails & appt_emails)
                as_sales = len(as_lead_emails & sale_emails)
                as_rev   = sum(sale_revenue.get(em, 0.0) for em in as_lead_emails & sale_emails)

                as_ctr  = safe_div(as_clicks, as_imp) * 100
                as_cpl  = safe_div(as_spend, as_leads)
                as_cac  = safe_div(as_spend, as_sales)
                as_roas = safe_div(as_rev, as_spend)
                as_cvr  = safe_div(as_leads, as_clicks) * 100
                as_appt_rate  = safe_div(as_appts, as_leads) * 100
                as_close_rate = safe_div(as_sales, as_appts) * 100

                # --- Creative level ---
                creative_rows = []
                if creat_col and "_creat_norm" in ads_df.columns:
                    adset_mask = camp_mask & (ads_df["_adset_norm"] == ask)
                    cr_agg = ads_df[adset_mask].groupby("_creat_norm").agg(
                        {a["spend"]: "sum", a["impressions"]: "sum", a["clicks"]: "sum"}
                    ).reset_index()
                    cr_agg[creat_col] = cr_agg["_creat_norm"].map(
                        ads_df[adset_mask].groupby("_creat_norm")[creat_col].first()
                    )

                    for _, cr_row in cr_agg.iterrows():
                        crk     = str(cr_row["_creat_norm"])
                        cr_spend  = float(cr_row[a["spend"]])
                        cr_imp    = float(cr_row[a["impressions"]])
                        cr_clicks = float(cr_row[a["clicks"]])

                        cr_lead_emails = creat_leads.get((ck, ask, crk), set())
                        cr_leads = len(cr_lead_emails)
                        cr_appts = len(cr_lead_emails & appt_emails)
                        cr_sales = len(cr_lead_emails & sale_emails)
                        cr_rev   = sum(sale_revenue.get(em, 0.0) for em in cr_lead_emails & sale_emails)
                        cr_ctr   = safe_div(cr_clicks, cr_imp) * 100
                        cr_cpl   = safe_div(cr_spend, cr_leads)
                        cr_cac   = safe_div(cr_spend, cr_sales)
                        cr_roas  = safe_div(cr_rev, cr_spend)
                        cr_cvr   = safe_div(cr_leads, cr_clicks) * 100
                        cr_appt_rate  = safe_div(cr_appts, cr_leads) * 100
                        cr_close_rate = safe_div(cr_sales, cr_appts) * 100

                        creative_rows.append({
                            "level":         "creative",
                            "id":            f"{ck}|{ask}|{crk}",
                            "name":          str(cr_row[creat_col]),
                            "campaign_name": str(c_row[camp_col]),
                            "adset_name":    str(as_row[adset_col]),
                            "spend":         round(cr_spend, 2),
                            "impressions":   int(cr_imp),
                            "clicks":        int(cr_clicks),
                            "leads":         cr_leads,
                            "appointments":  cr_appts,
                            "sales":         cr_sales,
                            "revenue":       round(cr_rev, 2),
                            "ctr":           round(cr_ctr, 2),
                            "cpl":           round(cr_cpl, 2),
                            "cac":           round(cr_cac, 2),
                            "roas":          round(cr_roas, 2),
                            "conversion_rate":  round(cr_cvr, 2),
                            "appointment_rate": round(cr_appt_rate, 2),
                            "close_rate":       round(cr_close_rate, 2),
                            "children":      [],
                        })

                adset_rows.append({
                    "level":        "adset",
                    "id":           f"{ck}|{ask}",
                    "name":         str(as_row[adset_col]),
                    "campaign_name": str(c_row[camp_col]),
                    "spend":        round(as_spend, 2),
                    "impressions":  int(as_imp),
                    "clicks":       int(as_clicks),
                    "leads":        as_leads,
                    "appointments": as_appts,
                    "sales":        as_sales,
                    "revenue":      round(as_rev, 2),
                    "ctr":          round(as_ctr, 2),
                    "cpl":          round(as_cpl, 2),
                    "cac":          round(as_cac, 2),
                    "roas":         round(as_roas, 2),
                    "conversion_rate":  round(as_cvr, 2),
                    "appointment_rate": round(as_appt_rate, 2),
                    "close_rate":       round(as_close_rate, 2),
                    "children":     creative_rows,
                })

        camp_rows.append({
            "level":        "campaign",
            "id":           ck,
            "name":         str(c_row[camp_col]),
            "spend":        round(c_spend, 2),
            "impressions":  int(c_imp),
            "clicks":       int(c_clicks),
            "leads":        c_leads,
            "appointments": c_appts,
            "sales":        c_sales,
            "revenue":      round(c_rev, 2),
            "ctr":          round(c_ctr, 2),
            "cpl":          round(c_cpl, 2),
            "cac":          round(c_cac, 2),
            "roas":         round(c_roas, 2),
            "conversion_rate":  round(c_cvr, 2),
            "appointment_rate": round(c_appt_rate, 2),
            "close_rate":       round(c_close_rate, 2),
            "children":     adset_rows,
        })

    # ── Compute performance scores on creative rows ───────────────────────────
    all_creatives = []
    for camp in camp_rows:
        for adset in camp["children"]:
            for cre in adset["children"]:
                all_creatives.append(cre)

    if all_creatives:
        cre_df = pd.DataFrame(all_creatives)
        cre_df = compute_performance_scores(cre_df)
        for i, cre in enumerate(all_creatives):
            cre["performance_score"] = int(cre_df.iloc[i]["performance_score"])
            cre["score_band"]        = cre_df.iloc[i]["score_band"]

    # Also score campaigns
    if camp_rows:
        cd = pd.DataFrame(camp_rows)
        cd = compute_performance_scores(cd)
        for i, c in enumerate(camp_rows):
            c["performance_score"] = int(cd.iloc[i]["performance_score"])
            c["score_band"]        = cd.iloc[i]["score_band"]

    performance_table = camp_rows

    # ── Flatten all adsets ───────────────────────────────────────────────────────
    all_adsets = []
    for camp in camp_rows:
        for adset in camp["children"]:
            all_adsets.append(adset)

    # ── Generic consolidation helper (recursive — merges children too) ──────────
    def _consolidate(rows: list[dict], level: str) -> list[dict]:
        merged: dict[str, dict] = {}
        raw_children: dict[str, list] = {}

        for row in rows:
            key = row["name"].strip().lower()
            if key not in merged:
                merged[key] = {
                    "level": level, "id": key, "name": row["name"],
                    "spend": 0.0, "impressions": 0, "clicks": 0,
                    "leads": 0, "appointments": 0, "sales": 0, "revenue": 0.0,
                    "children": [],
                }
                raw_children[key] = []
            m = merged[key]
            m["spend"]        += row["spend"]
            m["impressions"]  += row["impressions"]
            m["clicks"]       += row["clicks"]
            m["leads"]        += row["leads"]
            m["appointments"] += row["appointments"]
            m["sales"]        += row["sales"]
            m["revenue"]      += row["revenue"]
            raw_children[key].extend(row.get("children") or [])

        result = []
        for key, m in merged.items():
            m["spend"]   = round(m["spend"],   2)
            m["revenue"] = round(m["revenue"], 2)
            m["ctr"]  = round(safe_div(m["clicks"], m["impressions"]) * 100, 2)
            m["cpl"]  = round(safe_div(m["spend"], m["leads"]), 2)
            m["cac"]  = round(safe_div(m["spend"], m["sales"]), 2)
            m["roas"] = round(safe_div(m["revenue"], m["spend"]), 2)
            m["conversion_rate"]  = round(safe_div(m["leads"], m["clicks"]) * 100, 2)
            m["appointment_rate"] = round(safe_div(m["appointments"], m["leads"]) * 100, 2)
            m["close_rate"]       = round(safe_div(m["sales"], m["appointments"]) * 100, 2)
            # Recursively consolidate children
            children = raw_children[key]
            if children:
                child_level = children[0]["level"]
                m["children"] = _consolidate(children, child_level)
            result.append(m)

        if result:
            df_tmp = pd.DataFrame(result)
            df_tmp = compute_performance_scores(df_tmp)
            for i, r in enumerate(result):
                r["performance_score"] = int(df_tmp.iloc[i]["performance_score"])
                r["score_band"]        = df_tmp.iloc[i]["score_band"]
        return result

    campaigns_flat = _consolidate(camp_rows,     "campaign")
    adsets_flat    = _consolidate(all_adsets,    "adset")
    creatives_flat = _consolidate(all_creatives, "creative")

    # ── Creative rankings ─────────────────────────────────────────────────────
    scored = sorted(creatives_flat, key=lambda x: x.get("performance_score", 0), reverse=True)
    top_5    = scored[:5]
    bottom_5 = list(reversed(scored[-5:])) if len(scored) >= 5 else list(reversed(scored))

    # ── Global KPIs ──────────────────────────────────────────────────────────
    overall_cpl       = safe_div(total_spend, total_leads)
    overall_cac       = safe_div(total_spend, total_sales)
    overall_roas      = safe_div(total_revenue_val, total_spend)
    overall_ctr       = safe_div(total_clicks, total_impressions) * 100
    lead_to_appt      = safe_div(total_appts, total_leads) * 100
    appt_to_sale      = safe_div(total_sales, total_appts) * 100
    overall_cpm       = safe_div(total_spend, total_impressions) * 1000
    overall_frequency = safe_div(total_impressions, total_reach)
    overall_connect_rate = safe_div(total_clicks, total_reach) * 100
    overall_page_cvr  = safe_div(total_leads, total_clicks) * 100

    # Date range info
    date_min = ads_df[a["date"]].min() if a["date"] and a["date"] in ads_df.columns else None
    date_max = ads_df[a["date"]].max() if a["date"] and a["date"] in ads_df.columns else None

    return {
        "meta": {
            "last_sync":   datetime.utcnow().isoformat() + "Z",
            "stale":       False,
            "date_range": {
                "start": date_min.strftime("%Y-%m-%d") if date_min and pd.notna(date_min) else None,
                "end":   date_max.strftime("%Y-%m-%d") if date_max and pd.notna(date_max) else None,
            },
        },
        "kpis": {
            "total_spend":        round(total_spend, 2),
            "total_revenue":      round(total_revenue_val, 2),
            "overall_roas":       round(overall_roas, 2),
            "overall_cpl":        round(overall_cpl, 2),
            "overall_ctr":        round(overall_ctr, 2),
            "total_leads":        total_leads,
            "total_appointments": total_appts,
            "total_sales":        total_sales,
            "overall_cac":        round(overall_cac, 2),
            "lead_to_appt_rate":  round(lead_to_appt, 2),
            "appt_to_sale_rate":  round(appt_to_sale, 2),
            "overall_cpm":        round(overall_cpm, 2),
            "total_reach":        int(total_reach),
            "overall_frequency":  round(overall_frequency, 2),
            "overall_connect_rate": round(overall_connect_rate, 2),
            "overall_page_cvr":   round(overall_page_cvr, 2),
        },
        "performance_table": performance_table,
        "campaigns_flat": campaigns_flat,
        "adsets_flat":    adsets_flat,
        "creatives_flat": creatives_flat,
        "creative_rankings": {
            "top_5":    top_5,
            "bottom_5": bottom_5,
        },
    }


def _empty_payload() -> dict:
    return {
        "meta": {
            "last_sync":  datetime.utcnow().isoformat() + "Z",
            "stale":      False,
            "date_range": {"start": None, "end": None},
        },
        "kpis": {
            "total_spend": 0, "total_revenue": 0, "overall_roas": 0,
            "overall_cpl": 0, "overall_ctr": 0, "total_leads": 0,
            "total_appointments": 0, "total_sales": 0,
            "overall_cac": 0, "lead_to_appt_rate": 0, "appt_to_sale_rate": 0,
            "overall_cpm": 0, "total_reach": 0, "overall_frequency": 0,
            "overall_connect_rate": 0, "overall_page_cvr": 0,
        },
        "performance_table": [],
        "campaigns_flat": [],
        "adsets_flat":    [],
        "creatives_flat": [],
        "creative_rankings": {"top_5": [], "bottom_5": []},
    }
