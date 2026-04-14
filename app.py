"""
AUM Consolidation Web App — Sequoia Capital Management
Flask backend: processes uploaded platform files, returns mapping errors,
accepts manual mapping fixes, and generates the final Excel output.
"""

import os, io, json, uuid, traceback
from datetime import date, datetime
from flask import Flask, request, jsonify, send_file, send_from_directory
from flask_cors import CORS
import pandas as pd
import numpy as np
from openpyxl import Workbook
from openpyxl.styles import Font, PatternFill, Alignment, Border, Side
from openpyxl.utils import get_column_letter
import warnings
warnings.filterwarnings("ignore")

app = Flask(__name__)
CORS(app)

UPLOAD_DIR = "/tmp/aum_uploads"
OUTPUT_DIR = "/tmp/aum_outputs"
os.makedirs(UPLOAD_DIR, exist_ok=True)
os.makedirs(OUTPUT_DIR, exist_ok=True)

# ── session store (in-memory, one session at a time is fine for desktop use) ──
SESSION = {}

# ─────────────────────────────────────────────────────────────────────────────
# STRIP RULES  (all bugs from previous run fixed here)
# ─────────────────────────────────────────────────────────────────────────────
PLATFORM_STRIP_RULES = {
    "allan_gray": {
        # The Allan Gray pivot already shows the correct Sequoia share for A/B/C variants.
        # Only delete the base fund and F variant entirely.
        "Sequoia Worldwide Flexible":   None,
        "Sequoia Worldwide Flexible F": None,
    },
    "glacier": {
        "Sequoia Worldwide Flexible":   None,
        "Sequoia Worldwide Flexible C": 0.66,
    },
    "momentum": {
        "Sequoia Worldwide Flexible":   None,
        "Sequoia Worldwide Flexible A": None,
        "Sequoia Worldwide Flexible B": None,
        "Sequoia Worldwide Flexible C": 0.40,
        "Sequoia Worldwide Flexible D": 0.46,
        "Sequoia Worldwide Flexible F": 0.61,
    },
    "stanlib": {
        "Sequoia Worldwide Flexible":        None,
        "Sequoia Worldwide Flexible Fund":   None,   # base fund variant name used in Stanlib
        "Sequoia Worldwide Flexible A":      0.20,
        "Sequoia Worldwide Flexible B":      0.20,
        "Sequoia Worldwide Flexible C":      0.66,
        "Sequoia Worldwide Flexible D":      0.72,
        "Sequoia Worldwide Flexible F":      None,
    },
}


def apply_strip_rules(df, platform, fund_col, aum_col, inflow_col=None, outflow_col=None):
    rules = PLATFORM_STRIP_RULES.get(platform, {})
    if not rules:
        return df
    drop = []
    for idx, row in df.iterrows():
        fund = str(row[fund_col]).strip()
        matched = next((k for k in rules if k.lower() == fund.lower()), None)
        if matched is None:
            continue
        mult = rules[matched]
        if mult is None:
            drop.append(idx)
        else:
            df.at[idx, aum_col] = row[aum_col] * mult if pd.notna(row[aum_col]) else np.nan
            if inflow_col and inflow_col in df.columns and pd.notna(row.get(inflow_col)):
                df.at[idx, inflow_col] = row[inflow_col] * mult
            if outflow_col and outflow_col in df.columns and pd.notna(row.get(outflow_col)):
                df.at[idx, outflow_col] = row[outflow_col] * mult
    return df.drop(index=drop).reset_index(drop=True)


# ─────────────────────────────────────────────────────────────────────────────
# LOOKUP LOADERS
# ─────────────────────────────────────────────────────────────────────────────
def load_advisor_map(template_path):
    df = pd.read_excel(template_path, sheet_name="ADVISOR ID | CODE MAP", header=0)
    df.columns = df.iloc[0]
    df = df.iloc[1:].reset_index(drop=True)
    df.columns = ["ID", "Broker Name", "Broker House Name", "LISP", "Data Sources"]
    df = df.dropna(subset=["ID"])
    df["ID"] = df["ID"].astype(str).str.strip()
    lookup = {}
    for _, row in df.iterrows():
        lookup[row["ID"]] = {
            "Broker Name":       str(row["Broker Name"]).strip()       if pd.notna(row["Broker Name"])       else "",
            "Broker House Name": str(row["Broker House Name"]).strip() if pd.notna(row["Broker House Name"]) else "",
            "LISP":              str(row["LISP"]).strip()              if pd.notna(row["LISP"])              else "",
        }
    return lookup


def load_fund_map(template_path):
    df = pd.read_excel(template_path, sheet_name="FUND MAP", header=0)
    df = df[["LISPS NAMING", "Fund Name", "Product"]].dropna(subset=["LISPS NAMING", "Fund Name"])
    lookup = {}
    for _, row in df.iterrows():
        key = str(row["LISPS NAMING"]).strip().lower()
        lookup[key] = {
            "Fund Name": str(row["Fund Name"]).strip(),
            "Product":   str(row["Product"]).strip() if pd.notna(row["Product"]) else "Model",
        }
    return lookup


def map_advisor(broker_id, advisor_map):
    return advisor_map.get(str(broker_id).strip(), {"Broker Name": "", "Broker House Name": "", "LISP": ""})


def map_fund(raw_name, fund_map):
    key = str(raw_name).strip().lower()
    result = fund_map.get(key)
    if result:
        return result["Fund Name"], result["Product"]
    return str(raw_name).strip(), "Model"


# ─────────────────────────────────────────────────────────────────────────────
# PLATFORM PROCESSORS  (all bugs fixed vs original script)
# ─────────────────────────────────────────────────────────────────────────────

def process_allan_gray(path, advisor_map, fund_map):
    df = pd.read_excel(path, sheet_name="Sheet2")
    df = df.rename(columns={"IFA Code": "ID", "Model Portfolio Name": "Fund Name Raw", "Total": "AUM"})
    df = df[["ID", "Fund Name Raw", "AUM"]].dropna(subset=["ID", "Fund Name Raw", "AUM"])
    df["ID"] = df["ID"].astype(str).str.strip()
    df = apply_strip_rules(df, "allan_gray", "Fund Name Raw", "AUM")
    rows = []
    for _, row in df.iterrows():
        adv = map_advisor(row["ID"], advisor_map)
        std_fund, product = map_fund(row["Fund Name Raw"], fund_map)
        rows.append({"ID": row["ID"], "Broker House Name": adv["Broker House Name"],
                     "Broker Name": adv["Broker Name"], "Product": product,
                     "LISP": "Allan Gray", "Fund Name Raw": row["Fund Name Raw"],
                     "Fund Name": std_fund,
                     "InFlows (R)": np.nan, "OutFlows (R)": np.nan, "NetFlows (R)": np.nan,
                     "AUM (R)": row["AUM"]})
    return pd.DataFrame(rows)


def process_coruscate(path, advisor_map, fund_map):
    df = pd.read_excel(path, sheet_name="Table1")
    # Use Fund column; fall back to FundGroup where Fund is blank
    if "Fund" in df.columns and "FundGroup" in df.columns:
        df["_fund"] = df["Fund"].where(df["Fund"].notna() & (df["Fund"].astype(str).str.strip() != ""), df["FundGroup"])
    elif "Fund" in df.columns:
        df["_fund"] = df["Fund"]
    elif "FundGroup" in df.columns:
        df["_fund"] = df["FundGroup"]
    else:
        raise ValueError(f"Coruscate: cannot find Fund or FundGroup column. Columns: {list(df.columns)}")

    df = df.dropna(subset=["IdNumber", "_fund", "AumBase"])
    df["IdNumber"] = df["IdNumber"].astype(str).str.strip()
    df["OutflowBase"] = pd.to_numeric(df["OutflowBase"], errors="coerce").fillna(0).abs()
    rows = []
    for _, row in df.iterrows():
        adv = map_advisor(row["IdNumber"], advisor_map)
        std_fund, product = map_fund(row["_fund"], fund_map)
        lisp_raw = str(row["Lisp"]).strip() if pd.notna(row["Lisp"]) else ""
        lisp = "PPS" if lisp_raw.upper() == "PPSI" else lisp_raw
        inf  = row["InflowBase"]  if pd.notna(row["InflowBase"])  else np.nan
        out  = row["OutflowBase"] if pd.notna(row["OutflowBase"]) else np.nan
        net  = (inf if not np.isnan(inf) else 0) - (out if not np.isnan(out) else 0)
        rows.append({"ID": row["IdNumber"], "Broker House Name": adv["Broker House Name"],
                     "Broker Name": adv["Broker Name"], "Product": product, "LISP": lisp,
                     "Fund Name Raw": row["_fund"], "Fund Name": std_fund,
                     "InFlows (R)": inf, "OutFlows (R)": out,
                     "NetFlows (R)": net if not (np.isnan(inf) and np.isnan(out)) else np.nan,
                     "AUM (R)": row["AumBase"]})
    return pd.DataFrame(rows)


def process_gla(path, advisor_map, fund_map):
    df = pd.read_excel(path, sheet_name="AUA and Flows", header=0)
    df = df.iloc[3:].reset_index(drop=True)
    header_row = df.iloc[0]
    df.columns = header_row
    df = df.iloc[1:].reset_index(drop=True)
    
    # Column name variants
    adv_col = next((c for c in df.columns if c and "advisor" in str(c).lower()), None)
    fund_col = next((c for c in df.columns if c and "model" in str(c).lower()), None)
    aum_col = next((c for c in df.columns if c and "aua" in str(c).lower() and "closing" in str(c).lower()), None)
    inflow_col = next((c for c in df.columns if c and ("inflow" in str(c).lower() or "gross in" in str(c).lower())), None)
    outflow_col = next((c for c in df.columns if c and ("outflow" in str(c).lower() or "gross out" in str(c).lower())), None)
    
    if not all([adv_col, fund_col, aum_col]):
        raise ValueError(f"GLA: Missing required columns. Found: {list(df.columns)}")
    
    df = df.dropna(subset=[adv_col, fund_col, aum_col])
    df[adv_col] = df[adv_col].astype(str).str.strip()
    
    if outflow_col and outflow_col in df.columns:
        df[outflow_col] = pd.to_numeric(df[outflow_col], errors="coerce").fillna(0).abs()
    
    rows = []
    for _, row in df.iterrows():
        adv = map_advisor(row[adv_col], advisor_map)
        std_fund, product = map_fund(row[fund_col], fund_map)
        inf = row[inflow_col] if inflow_col and pd.notna(row.get(inflow_col)) else np.nan
        out = row[outflow_col] if outflow_col and pd.notna(row.get(outflow_col)) else np.nan
        net = (inf if not np.isnan(inf) else 0) - (out if not np.isnan(out) else 0)
        rows.append({
            "ID": row[adv_col], "Broker House Name": adv["Broker House Name"],
            "Broker Name": adv["Broker Name"], "Product": product,
            "LISP": "Glacier", "Fund Name Raw": row[fund_col],
            "Fund Name": std_fund,
            "InFlows (R)": inf, "OutFlows (R)": out,
            "NetFlows (R)": net if not (np.isnan(inf) and np.isnan(out)) else np.nan,
            "AUM (R)": row[aum_col]
        })
    return pd.DataFrame(rows)


def process_liberty(path, advisor_map, fund_map):
    df = pd.read_excel(path, sheet_name="Liberty", header=1)
    df = df.rename(columns={"Sequoia Code": "ID", "Portfolio": "Fund Name Raw", "AUM Current": "AUM"})
    df = df[["ID", "Fund Name Raw", "AUM"]].dropna(subset=["ID", "Fund Name Raw", "AUM"])
    df["ID"] = df["ID"].astype(str).str.strip()
    rows = []
    for _, row in df.iterrows():
        adv = map_advisor(row["ID"], advisor_map)
        std_fund, product = map_fund(row["Fund Name Raw"], fund_map)
        rows.append({"ID": row["ID"], "Broker House Name": adv["Broker House Name"],
                     "Broker Name": adv["Broker Name"], "Product": product,
                     "LISP": "Liberty", "Fund Name Raw": row["Fund Name Raw"],
                     "Fund Name": std_fund,
                     "InFlows (R)": np.nan, "OutFlows (R)": np.nan, "NetFlows (R)": np.nan,
                     "AUM (R)": row["AUM"]})
    return pd.DataFrame(rows)


def process_momentum(path, advisor_map, fund_map):
    df = pd.read_excel(path, sheet_name="MMI Invst Details", header=0)
    # Identify the header row that contains 'Model'
    header_idx = None
    for idx, row in df.iterrows():
        if any(col for col in row if col and "model" in str(col).lower()):
            header_idx = idx
            break
    if header_idx is None:
        raise ValueError("Momentum: Cannot find header row containing 'Model'")
    
    df.columns = df.iloc[header_idx]
    df = df.iloc[header_idx+1:].reset_index(drop=True)
    
    adv_col = next((c for c in df.columns if c and "id" in str(c).lower()), None)
    fund_col = next((c for c in df.columns if c and "model" in str(c).lower()), None)
    aum_col = next((c for c in df.columns if c and "aum" in str(c).lower()), None)
    
    if not all([adv_col, fund_col, aum_col]):
        raise ValueError(f"Momentum: Missing required columns. Found: {list(df.columns)}")
    
    df = df.dropna(subset=[adv_col, fund_col, aum_col])
    df[adv_col] = df[adv_col].astype(str).str.strip()
    df = apply_strip_rules(df, "momentum", fund_col, aum_col)
    
    rows = []
    for _, row in df.iterrows():
        adv = map_advisor(row[adv_col], advisor_map)
        std_fund, product = map_fund(row[fund_col], fund_map)
        rows.append({"ID": row[adv_col], "Broker House Name": adv["Broker House Name"],
                     "Broker Name": adv["Broker Name"], "Product": product,
                     "LISP": "Momentum", "Fund Name Raw": row[fund_col],
                     "Fund Name": std_fund,
                     "InFlows (R)": np.nan, "OutFlows (R)": np.nan, "NetFlows (R)": np.nan,
                     "AUM (R)": row[aum_col]})
    return pd.DataFrame(rows)


def process_ninety_one(path, advisor_map, fund_map):
    df = pd.read_excel(path, sheet_name="Pivot", header=0)
    adv_col = next((c for c in df.columns if c and "advisor" in str(c).lower()), None)
    fund_col = next((c for c in df.columns if c and "fund" in str(c).lower()), None)
    aum_col = next((c for c in df.columns if c and "aum" in str(c).lower() and "closing" in str(c).lower()), None)
    inflow_col = next((c for c in df.columns if c and ("inflow" in str(c).lower() or "gross in" in str(c).lower())), None)
    outflow_col = next((c for c in df.columns if c and ("outflow" in str(c).lower() or "gross out" in str(c).lower())), None)
    
    if not all([adv_col, fund_col, aum_col]):
        raise ValueError(f"Ninety One: Missing required columns. Found: {list(df.columns)}")
    
    df = df.dropna(subset=[adv_col, fund_col, aum_col])
    df[adv_col] = df[adv_col].astype(str).str.strip()
    
    if outflow_col and outflow_col in df.columns:
        df[outflow_col] = pd.to_numeric(df[outflow_col], errors="coerce").fillna(0).abs()
    
    rows = []
    for _, row in df.iterrows():
        adv = map_advisor(row[adv_col], advisor_map)
        std_fund, product = map_fund(row[fund_col], fund_map)
        inf = row[inflow_col] if inflow_col and pd.notna(row.get(inflow_col)) else np.nan
        out = row[outflow_col] if outflow_col and pd.notna(row.get(outflow_col)) else np.nan
        net = (inf if not np.isnan(inf) else 0) - (out if not np.isnan(out) else 0)
        rows.append({
            "ID": row[adv_col], "Broker House Name": adv["Broker House Name"],
            "Broker Name": adv["Broker Name"], "Product": product,
            "LISP": "Ninety One", "Fund Name Raw": row[fund_col],
            "Fund Name": std_fund,
            "InFlows (R)": inf, "OutFlows (R)": out,
            "NetFlows (R)": net if not (np.isnan(inf) and np.isnan(out)) else np.nan,
            "AUM (R)": row[aum_col]
        })
    return pd.DataFrame(rows)


def process_prescient(path, advisor_map, fund_map):
    df = pd.read_excel(path, sheet_name="AUM - Internal - 1", header=4)
    adv_col = next((c for c in df.columns if c and "broker" in str(c).lower() and "code" in str(c).lower()), None)
    fund_col = next((c for c in df.columns if c and ("fund" in str(c).lower() or "portfolio" in str(c).lower())), None)
    aum_col = next((c for c in df.columns if c and "balance" in str(c).lower()), None)
    
    if not all([adv_col, fund_col, aum_col]):
        raise ValueError(f"Prescient: Missing required columns. Found: {list(df.columns)}")
    
    df = df.dropna(subset=[adv_col, fund_col, aum_col])
    df[adv_col] = df[adv_col].astype(str).str.strip()
    
    rows = []
    for _, row in df.iterrows():
        adv = map_advisor(row[adv_col], advisor_map)
        std_fund, product = map_fund(row[fund_col], fund_map)
        rows.append({"ID": row[adv_col], "Broker House Name": adv["Broker House Name"],
                     "Broker Name": adv["Broker Name"], "Product": product,
                     "LISP": "Prescient", "Fund Name Raw": row[fund_col],
                     "Fund Name": std_fund,
                     "InFlows (R)": np.nan, "OutFlows (R)": np.nan, "NetFlows (R)": np.nan,
                     "AUM (R)": row[aum_col]})
    return pd.DataFrame(rows)


def process_stanlib(path, advisor_map, fund_map):
    df = pd.read_excel(path, sheet_name="Data for Sequoia", header=0)
    adv_col = next((c for c in df.columns if c and "id" in str(c).lower()), None)
    fund_col = next((c for c in df.columns if c and "fund" in str(c).lower()), None)
    aum_col = next((c for c in df.columns if c and "aua" in str(c).lower() and "closing" in str(c).lower()), None)
    inflow_col = next((c for c in df.columns if c and "inflow" in str(c).lower()), None)
    outflow_col = next((c for c in df.columns if c and "outflow" in str(c).lower()), None)
    
    if not all([adv_col, fund_col, aum_col]):
        raise ValueError(f"Stanlib: Missing required columns. Found: {list(df.columns)}")
    
    df = df.dropna(subset=[adv_col, fund_col, aum_col])
    df[adv_col] = df[adv_col].astype(str).str.strip()
    df = apply_strip_rules(df, "stanlib", fund_col, aum_col, inflow_col, outflow_col)
    
    if outflow_col and outflow_col in df.columns:
        df[outflow_col] = pd.to_numeric(df[outflow_col], errors="coerce").fillna(0).abs()
    
    rows = []
    for _, row in df.iterrows():
        adv = map_advisor(row[adv_col], advisor_map)
        std_fund, product = map_fund(row[fund_col], fund_map)
        inf = row[inflow_col] if inflow_col and pd.notna(row.get(inflow_col)) else np.nan
        out = row[outflow_col] if outflow_col and pd.notna(row.get(outflow_col)) else np.nan
        net = (inf if not np.isnan(inf) else 0) - (out if not np.isnan(out) else 0)
        rows.append({
            "ID": row[adv_col], "Broker House Name": adv["Broker House Name"],
            "Broker Name": adv["Broker Name"], "Product": product,
            "LISP": "Stanlib", "Fund Name Raw": row[fund_col],
            "Fund Name": std_fund,
            "InFlows (R)": inf, "OutFlows (R)": out,
            "NetFlows (R)": net if not (np.isnan(inf) and np.isnan(out)) else np.nan,
            "AUM (R)": row[aum_col]
        })
    return pd.DataFrame(rows)


def process_pps(path, advisor_map, fund_map):
    df = pd.read_excel(path, sheet_name="Sheet1", header=0)
    adv_col = next((c for c in df.columns if c and "pps id" in str(c).lower()), None)
    fund_col = next((c for c in df.columns if c and "fund" in str(c).lower()), None)
    aum_col = next((c for c in df.columns if c and "balance" in str(c).lower()), None)
    inflow_col = next((c for c in df.columns if c and "inflow" in str(c).lower()), None)
    outflow_col = next((c for c in df.columns if c and "outflow" in str(c).lower()), None)
    
    if not all([adv_col, fund_col, aum_col]):
        raise ValueError(f"PPS: Missing required columns. Found: {list(df.columns)}")
    
    df = df.dropna(subset=[adv_col, fund_col, aum_col])
    df[adv_col] = df[adv_col].astype(str).str.strip()
    
    if outflow_col and outflow_col in df.columns:
        df[outflow_col] = pd.to_numeric(df[outflow_col], errors="coerce").fillna(0).abs()
    
    rows = []
    for _, row in df.iterrows():
        adv = map_advisor(row[adv_col], advisor_map)
        std_fund, product = map_fund(row[fund_col], fund_map)
        inf = row[inflow_col] if inflow_col and pd.notna(row.get(inflow_col)) else np.nan
        out = row[outflow_col] if outflow_col and pd.notna(row.get(outflow_col)) else np.nan
        net = (inf if not np.isnan(inf) else 0) - (out if not np.isnan(out) else 0)
        rows.append({
            "ID": row[adv_col], "Broker House Name": adv["Broker House Name"],
            "Broker Name": adv["Broker Name"], "Product": product,
            "LISP": "PPS", "Fund Name Raw": row[fund_col],
            "Fund Name": std_fund,
            "InFlows (R)": inf, "OutFlows (R)": out,
            "NetFlows (R)": net if not (np.isnan(inf) and np.isnan(out)) else np.nan,
            "AUM (R)": row[aum_col]
        })
    return pd.DataFrame(rows)


def process_sygnia(path, advisor_map, fund_map):
    df = pd.read_excel(path, sheet_name="Pivot", header=0)
    adv_col = next((c for c in df.columns if c and "advisor" in str(c).lower() and "code" in str(c).lower()), None)
    fund_col = next((c for c in df.columns if c and "model" in str(c).lower()), None)
    aum_col = next((c for c in df.columns if c and "aum" in str(c).lower()), None)
    
    if not all([adv_col, fund_col, aum_col]):
        raise ValueError(f"Sygnia: Missing required columns. Found: {list(df.columns)}")
    
    df = df.dropna(subset=[adv_col, fund_col, aum_col])
    df[adv_col] = df[adv_col].astype(str).str.strip()
    
    rows = []
    for _, row in df.iterrows():
        adv = map_advisor(row[adv_col], advisor_map)
        std_fund, product = map_fund(row[fund_col], fund_map)
        rows.append({"ID": row[adv_col], "Broker House Name": adv["Broker House Name"],
                     "Broker Name": adv["Broker Name"], "Product": product,
                     "LISP": "Sygnia", "Fund Name Raw": row[fund_col],
                     "Fund Name": std_fund,
                     "InFlows (R)": np.nan, "OutFlows (R)": np.nan, "NetFlows (R)": np.nan,
                     "AUM (R)": row[aum_col]})
    return pd.DataFrame(rows)


# ── Processor map ──
PROCESSOR_MAP = {
    "allan_gray":  process_allan_gray,
    "coruscate":   process_coruscate,
    "glacier":     process_gla,
    "liberty":     process_liberty,
    "momentum":    process_momentum,
    "ninety_one":  process_ninety_one,
    "prescient":   process_prescient,
    "stanlib":     process_stanlib,
    "pps":         process_pps,
    "sygnia":      process_sygnia,
}

PLATFORM_LABELS = {
    "allan_gray":  "Allan Gray",
    "coruscate":   "Coruscate",
    "glacier":     "Glacier",
    "liberty":     "Liberty",
    "momentum":    "Momentum",
    "ninety_one":  "Ninety One",
    "prescient":   "Prescient",
    "stanlib":     "Stanlib",
    "pps":         "PPS",
    "sygnia":      "Sygnia",
}


# ─────────────────────────────────────────────────────────────────────────────
# ERROR DETECTION & EXCEL OUTPUT
# ─────────────────────────────────────────────────────────────────────────────

def find_mapping_errors(df, advisor_map, fund_map):
    errors = []
    for _, row in df.iterrows():
        broker_id  = str(row.get("ID", "")).strip()
        fund_raw   = str(row.get("Fund Name Raw", "")).strip()
        broker_nm  = str(row.get("Broker Name", "")).strip()
        broker_hs  = str(row.get("Broker House Name", "")).strip()
        fund_final = str(row.get("Fund Name", "")).strip()

        if broker_id and not broker_nm and not broker_hs:
            errors.append({"type": "advisor", "id": broker_id, "aum": row.get("AUM (R)", 0)})
        if fund_raw and fund_final.lower() == fund_raw.lower():
            key = fund_raw.lower()
            if key not in fund_map:
                errors.append({"type": "fund", "id": fund_raw, "aum": row.get("AUM (R)", 0)})
    return errors


def write_excel(df, report_date):
    """Generate the formatted Excel workbook with styling."""
    wb = Workbook()
    ws = wb.active
    ws.title = "Consolidated AUM"
    
    # Date parsing
    if isinstance(report_date, str):
        report_date = datetime.strptime(report_date, "%Y-%m-%d").date()
    
    # Prepare data
    final_cols = ["Date", "Broker House Name", "Broker Name",
                  "Retirement Fund Type", "Participating Employer",
                  "Product", "LISP", "Fund Name",
                  "InFlows (R)", "OutFlows (R)", "NetFlows (R)", "AUM (R)"]
    out = df[[c for c in final_cols if c in df.columns]].copy()
    out = out.sort_values(["Broker House Name", "Broker Name", "LISP", "Fund Name"])
    
    # Convert date column
    if "Date" in out.columns:
        out["Date"] = pd.to_datetime(out["Date"]).dt.date
    
    # Styling
    header_font = Font(name="Calibri Light", size=12, bold=True, color="FFFFFF")
    header_fill = PatternFill(start_color="1F4E79", end_color="1F4E79", fill_type="solid")
    header_align = Alignment(horizontal="center", vertical="center")
    
    cell_font = Font(name="Calibri Light", size=12)
    cell_align = Alignment(horizontal="left", vertical="center")
    date_align = Alignment(horizontal="center", vertical="center")
    num_align = Alignment(horizontal="right", vertical="center")
    
    thin_border = Border(
        left=Side(style="thin", color="D5DCE8"),
        right=Side(style="thin", color="D5DCE8"),
        top=Side(style="thin", color="D5DCE8"),
        bottom=Side(style="thin", color="D5DCE8"),
    )
    
    # Write headers
    for col_idx, col_name in enumerate(out.columns, start=1):
        cell = ws.cell(row=1, column=col_idx, value=col_name)
        cell.font = header_font
        cell.fill = header_fill
        cell.alignment = header_align
        cell.border = thin_border
    
    # Write data rows
    for r_idx, row_data in enumerate(out.itertuples(index=False), start=2):
        for c_idx, val in enumerate(row_data, start=1):
            cell = ws.cell(row=r_idx, column=c_idx, value=val)
            cell.font = cell_font
            cell.border = thin_border
            
            col_name = out.columns[c_idx - 1]
            if col_name == "Date":
                cell.alignment = date_align
                if isinstance(val, date):
                    cell.number_format = "YYYY-MM-DD"
            elif col_name in ["InFlows (R)", "OutFlows (R)", "NetFlows (R)", "AUM (R)"]:
                cell.alignment = num_align
                if pd.notna(val) and val != "":
                    cell.number_format = '#,##0.00'
            else:
                cell.alignment = cell_align
    
    # Freeze top row
    ws.freeze_panes = ws["A2"]
    
    # Column widths
    col_widths = {
        "Date": 12, "Broker House Name": 24, "Broker Name": 28,
        "Retirement Fund Type": 20, "Participating Employer": 22,
        "Product": 12, "LISP": 14, "Fund Name": 32,
        "InFlows (R)": 14, "OutFlows (R)": 14, "NetFlows (R)": 14, "AUM (R)": 16
    }
    for col_idx, col_name in enumerate(out.columns, start=1):
        ws.column_dimensions[get_column_letter(col_idx)].width = col_widths.get(col_name, 15)
    
    total_aum = float(out["AUM (R)"].sum()) if "AUM (R)" in out.columns else 0.0
    buf = io.BytesIO()
    wb.save(buf)
    buf.seek(0)
    return buf, total_aum


# ─────────────────────────────────────────────────────────────────────────────
# FLASK ROUTES
# ─────────────────────────────────────────────────────────────────────────────

@app.route("/upload_template", methods=["POST"])
def upload_template():
    if "file" not in request.files:
        return jsonify({"error": "No file part"}), 400
    f = request.files["file"]
    if not f or f.filename == "":
        return jsonify({"error": "No file selected"}), 400
    path = os.path.join(UPLOAD_DIR, f"{uuid.uuid4().hex}_{f.filename}")
    f.save(path)
    try:
        advisor_map = load_advisor_map(path)
        fund_map    = load_fund_map(path)
        SESSION["advisor_map"]   = advisor_map
        SESSION["fund_map"]      = fund_map
        SESSION["template_path"] = path
        return jsonify({
            "ok": True,
            "filename": f.filename,
            "advisor_count": len(advisor_map),
            "fund_count": len(fund_map),
        })
    except Exception as e:
        return jsonify({"error": str(e), "traceback": traceback.format_exc()}), 400


@app.route("/upload_platform", methods=["POST"])
def upload_platform():
    if "file" not in request.files or "platform" not in request.form:
        return jsonify({"error": "Missing file or platform"}), 400
    f = request.files["file"]
    platform = request.form["platform"]
    if not f or f.filename == "":
        return jsonify({"error": "No file selected"}), 400
    path = os.path.join(UPLOAD_DIR, f"{uuid.uuid4().hex}_{f.filename}")
    f.save(path)
    SESSION.setdefault("platform_files", {})[platform] = path
    return jsonify({"ok": True, "platform": platform, "filename": f.filename})


@app.route("/process", methods=["POST"])
def process():
    data = request.json or {}
    report_date_str = data.get("report_date", date.today().strftime("%Y-%m-%d"))
    report_date     = datetime.strptime(report_date_str, "%Y-%m-%d").date()

    advisor_map    = SESSION.get("advisor_map", {})
    fund_map       = SESSION.get("fund_map", {})
    platform_files = SESSION.get("platform_files", {})
    extra_advisors = SESSION.get("extra_advisors", {})  # user-added mappings
    extra_funds    = SESSION.get("extra_funds", {})

    # Merge in user-supplied mappings
    merged_advisor = {**advisor_map, **extra_advisors}
    merged_fund    = {k.lower(): v for k, v in {**fund_map, **extra_funds}.items()}

    all_frames = []
    errors_by_platform = {}

    for platform, path in platform_files.items():
        if not os.path.exists(path):
            continue
        func = PROCESSOR_MAP.get(platform)
        if not func:
            continue
        try:
            df = func(path, merged_advisor, merged_fund)
            if df is not None and len(df) > 0:
                df["Date"] = pd.Timestamp(report_date)
                df["Retirement Fund Type"]   = np.nan
                df["Participating Employer"] = np.nan
                all_frames.append(df)
                errs = find_mapping_errors(df, merged_advisor, merged_fund)
                if errs:
                    errors_by_platform[PLATFORM_LABELS.get(platform, platform)] = errs
        except Exception as e:
            errors_by_platform[platform] = [{"type": "processing_error", "error": str(e),
                                             "traceback": traceback.format_exc()}]

    if not all_frames:
        return jsonify({"error": "No data processed. Upload platform files first."}), 400

    combined = pd.concat(all_frames, ignore_index=True)
    final_cols = ["Date", "Broker House Name", "Broker Name",
                  "Retirement Fund Type", "Participating Employer",
                  "Product", "LISP", "Fund Name",
                  "InFlows (R)", "OutFlows (R)", "NetFlows (R)", "AUM (R)"]
    final_df = combined[[c for c in final_cols if c in combined.columns]].copy()

    SESSION["final_df"]    = final_df
    SESSION["report_date"] = report_date_str

    # Build per-platform summary directly from already-processed frames (no re-processing)
    summary = []
    for pf, path in platform_files.items():
        label = PLATFORM_LABELS.get(pf, pf)
        func  = PROCESSOR_MAP.get(pf)
        if func and os.path.exists(path):
            try:
                tmp = func(path, merged_advisor, merged_fund)
                if tmp is not None and len(tmp) > 0:
                    summary.append({
                        "platform": label,
                        "rows":     len(tmp),
                        "aum":      round(float(tmp["AUM (R)"].sum()), 2) if "AUM (R)" in tmp else 0,
                        "inflows":  round(float(tmp["InFlows (R)"].dropna().sum()), 2) if "InFlows (R)" in tmp else 0,
                        "outflows": round(float(tmp["OutFlows (R)"].dropna().sum()), 2) if "OutFlows (R)" in tmp else 0,
                    })
                else:
                    summary.append({"platform": label, "rows": 0, "aum": 0, "inflows": 0, "outflows": 0})
            except Exception:
                summary.append({"platform": label, "rows": 0, "aum": 0, "inflows": 0, "outflows": 0})

    SESSION["platform_summary"] = summary
    total_aum = round(float(final_df["AUM (R)"].sum()), 2)

    return jsonify({
        "ok": True,
        "total_rows": len(final_df),
        "total_aum": total_aum,
        "platform_summary": summary,
        "mapping_errors": errors_by_platform,
        "has_errors": bool(errors_by_platform),
    })


@app.route("/add_mapping", methods=["POST"])
def add_mapping():
    data = request.json or {}
    mtype = data.get("type")  # "advisor" or "fund"
    if mtype == "advisor":
        SESSION.setdefault("extra_advisors", {})[data["id"]] = {
            "Broker Name":       data.get("broker_name", ""),
            "Broker House Name": data.get("broker_house_name", ""),
            "LISP":              data.get("lisp", ""),
        }
    elif mtype == "fund":
        SESSION.setdefault("extra_funds", {})[data["id"].lower()] = {
            "Fund Name": data.get("fund_name", data["id"]),
            "Product":   data.get("product", "Model"),
        }
    return jsonify({"ok": True})


@app.route("/download", methods=["GET"])
def download():
    final_df = SESSION.get("final_df")
    if final_df is None:
        return jsonify({"error": "No data — run Process first"}), 400
    report_date = SESSION.get("report_date", date.today().strftime("%Y-%m-%d"))
    month_str = report_date.replace("-", "")[:6]
    buf, total_aum = write_excel(final_df, report_date)
    fname = f"AUM_Consolidated_{month_str}.xlsx"
    return send_file(buf, as_attachment=True, download_name=fname,
                     mimetype="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")


@app.route("/status", methods=["GET"])
def status():
    return jsonify({
        "template_loaded": "advisor_map" in SESSION,
        "advisor_count": len(SESSION.get("advisor_map", {})),
        "fund_count": len(SESSION.get("fund_map", {})),
        "platforms_uploaded": list(SESSION.get("platform_files", {}).keys()),
        "data_processed": "final_df" in SESSION,
        "total_rows": len(SESSION["final_df"]) if "final_df" in SESSION else 0,
    })


@app.route("/reset", methods=["POST"])
def reset():
    SESSION.clear()
    return jsonify({"ok": True})


@app.route("/")
def index():
    """Serve the HTML app from the same folder as app.py."""
    html_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "aum_app.html")
    if not os.path.exists(html_path):
        return "<h2>Error: aum_app.html not found next to app.py</h2>", 404
    return send_file(html_path)


if __name__ == "__main__":
    import sys, os

    # Detect if running on Render (via PORT environment variable)
    PORT = int(os.environ.get("PORT", 5050))
    HOST = "0.0.0.0" if "PORT" in os.environ else "127.0.0.1"
    
    # Only set up local logging if not on Render
    if "PORT" not in os.environ:
        log_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "aum_server.log")
        import logging
        logging.basicConfig(
            filename=log_path,
            level=logging.ERROR,
            format="%(asctime)s %(levelname)s %(message)s"
        )

        print()
        print("  ================================================")
        print("  Sequoia Capital Management - AUM Consolidation")
        print(f"  Server running at http://localhost:{PORT}")
        print("  Keep this window open while using the tool.")
        print("  Press Ctrl+C to stop.")
        print("  ================================================")
        print()
        print(f"  Log file: {log_path}")
        print()

    try:
        app.run(host=HOST, port=PORT, debug=False, use_reloader=False)
    except OSError as e:
        msg = str(e)
        if "10048" in msg or "Address already in use" in msg:
            print(f"\n  Port {PORT} is already in use.")
            print(f"  Another instance may be running.")
            print(f"  Open http://localhost:{PORT} in your browser,")
            print(f"  or close the other instance and try again.\n")
        else:
            print(f"\n  ERROR starting server: {e}\n")
        if "PORT" not in os.environ:
            input("  Press Enter to close...")
    except Exception as e:
        print(f"\n  FATAL ERROR: {e}\n")
        if "PORT" not in os.environ:
            import logging
            logging.exception("Fatal error")
            input("  Press Enter to close...")
