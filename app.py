# -*- coding: utf-8 -*-
"""
KPI App – Định Hóa (v3.22)
- Form GHIM CỨNG (sticky) trên cùng
- Tiêu đề nhỏ hơn
- 4 nút tác vụ mỗi nút 1 màu
- Tổng điểm KPI (tạm tính)
"""

import re, io, base64, math, ast
from pathlib import Path
from datetime import datetime
import pandas as pd
import streamlit as st
import gspread
from google.oauth2.service_account import Credentials

# Drive API (tùy chọn)
try:
    from googleapiclient.discovery import build as gbuild
    from googleapiclient.http import MediaIoBaseUpload
    from googleapiclient.errors import HttpError
except Exception:
    gbuild = None
    MediaIoBaseUpload = None
    HttpError = Exception

# ------------------- CẤU HÌNH -------------------
st.set_page_config(page_title="KPI – Định Hóa", layout="wide")

GOOGLE_SHEET_ID_DEFAULT = "1nXFKJrn8oHwQgUzv5QYihoazYRhhS1PeN-xyo7Er2iM"
KPI_SHEET_DEFAULT = "KPI"
LOGO_URL = "https://raw.githubusercontent.com/phamlong666/kpi/main/logo_hinh_tron.png"

defaults = {
    "spreadsheet_id": GOOGLE_SHEET_ID_DEFAULT,
    "kpi_sheet_name": KPI_SHEET_DEFAULT,
    "drive_root_id": "",
    "_selected_idx": None,
    "_csv_loaded_sig": "",
    "auto_save_drive": False,
}
for k, v in defaults.items():
    if k not in st.session_state:
        st.session_state[k] = v

# ------------------- TIỆN ÍCH -------------------
def toast(msg, icon="ℹ️"):
    try: st.toast(msg, icon=icon)
    except Exception: pass

def extract_sheet_id(text: str) -> str:
    if not text: return ""
    m = re.search(r"/d/([a-zA-Z0-9-_]+)", text.strip())
    return m.group(1) if m else text.strip()

def extract_drive_folder_id(s: str) -> str:
    if not s: return ""
    m = re.search(r"/folders/([a-zA-Z0-9_-]+)", s.strip())
    return m.group(1) if m else s.strip()

def get_gs_clients():
    try:
        svc = dict(st.secrets["gdrive_service_account"])
        if "private_key" in svc:
            svc["private_key"] = (svc["private_key"]
                .replace("\\r\\n", "\\n").replace("\\r", "\\n").replace("\\\\n", "\\n"))
        scopes = [
            "https://www.googleapis.com/auth/spreadsheets",
            "https://www.googleapis.com/auth/drive",
        ]
        creds = Credentials.from_service_account_info(svc, scopes=scopes)
        gclient = gspread.authorize(creds)
        return gclient, creds
    except Exception as e:
        st.session_state["_gs_error"] = f"SECRETS_ERROR: {e}"
        return None, None

def open_spreadsheet(sid_or_url: str):
    sid = extract_sheet_id(sid_or_url or GOOGLE_SHEET_ID_DEFAULT) or GOOGLE_SHEET_ID_DEFAULT
    gclient, creds = st.session_state.get("_gs_pair", (None, None))
    if gclient is None:
        gclient, creds = get_gs_clients()
        st.session_state["_gs_pair"] = (gclient, creds)
    if gclient is None:
        raise RuntimeError("Chưa cấu hình service account trong st.secrets.")
    return gclient.open_by_key(sid)

def df_from_ws(ws) -> pd.DataFrame:
    records = ws.get_all_records(expected_headers=ws.row_values(1))
    return pd.DataFrame(records)

ALIAS = {
    "USE (mã đăng nhập)": ["USE (mã đăng nhập)", r"Tài khoản (USE\\username)", "Tài khoản (USE/username)", "Tài khoản", "Username", "USE", "User"],
    "Mật khẩu mặc định": ["Mật khẩu mặc định", "Password mặc định", "Password", "Mật khẩu"],
    "Tên chỉ tiêu (KPI)": ["Tên chỉ tiêu (KPI)", "Tên KPI", "Chỉ tiêu"],
    "Đơn vị tính": ["Đơn vị tính", "Unit"],
    "Kế hoạch": ["Kế hoạch", "Plan", "Target", "Kế hoạch (tháng)"],
    "Thực hiện": ["Thực hiện", "Thực hiện (tháng)", "Actual (month)"],
    "Trọng số": ["Trọng số", "Weight"],
    "Bộ phận/người phụ trách": ["Bộ phận/người phụ trách", "Phụ trách"],
    "Tháng": ["Tháng", "Month"],
    "Năm": ["Năm", "Year"],
    "Điểm KPI": ["Điểm KPI", "Score"],
    "Ghi chú": ["Ghi chú", "Notes"],
    "Tên đơn vị": ["Tên đơn vị", "Đơn vị"],
    "Phương pháp đo kết quả": ["Phương pháp đo kết quả", "Cách tính", "Công thức"],
    "Ngưỡng dưới": ["Ngưỡng dưới", "Min"],
    "Ngưỡng trên": ["Ngưỡng trên", "Max"],
}
def normalize_columns(df: pd.DataFrame) -> pd.DataFrame:
    if df is None or df.empty: return df
    cols_lower = {c.strip().lower(): c for c in df.columns}
    rename = {}
    for std, cands in ALIAS.items():
        if std in df.columns: continue
        for c in cands:
            key = c.strip().lower()
            if key in cols_lower:
                rename[cols_lower[key]] = std
                break
    if rename: df = df.rename(columns=rename)
    if "Thực hiện (tháng)" in df.columns and "Thực hiện" not in df.columns:
        df = df.rename(columns={"Thực hiện (tháng)":"Thực hiện"})
    if "Kế hoạch (tháng)" in df.columns and "Kế hoạch" not in df.columns:
        df = df.rename(columns={"Kế hoạch (tháng)":"Kế hoạch"})
    return df

def format_vn_number(x, decimals=2):
    try: f = float(x)
    except Exception: return ""
    s = f"{f:,.{decimals}f}"
    return s.replace(",", "_").replace(".", ",").replace("_", ".")

def parse_vn_number(s):
    if s is None: return None
    txt = str(s).strip()
    if txt == "" or txt.lower() in ("none", "nan"): return None
    txt = txt.replace(".", "").replace(",", ".")
    try: return float(txt)
    except Exception: return None

def parse_float(x):
    if isinstance(x,(int,float)): return float(x)
    return parse_vn_number(x)

def to_percent(val):
    v = parse_float(val)
    if v is None: return None
    return v*100.0 if abs(v)<=1.0 else v

# ===================== RULE ENGINE (tóm lược) =====================
_RULES_CACHE = None
_RULES_DEFAULT = [
    {"Code":"PENALTY_ERR_004","Type":"PENALTY_ERR","thr":1.5,"step":0.1,"pen":0.04,"cap":3.0,"keywords":"dự báo tổng thương phẩm; sai số ±1,5%; trừ 0,04; tru 0,04"},
    {"Code":"PENALTY_ERR_002","Type":"PENALTY_ERR","thr":1.5,"step":0.1,"pen":0.02,"cap":3.0,"keywords":"sai số ±1,5%; trừ 0,02; tru 0,02"},
    {"Code":"PENALTY_FLAG_025","Type":"PENALTY_FLAG","pen":0.25,"keywords":"vượt chỉ tiêu; 0,25; saifi; saidi"},
    {"Code":"RATIO_UP","Type":"RATIO_UP","keywords":"tăng tốt hơn; >="},
    {"Code":"RATIO_DOWN","Type":"RATIO_DOWN","keywords":"giảm tốt hơn; <="},
    {"Code":"PASS_FAIL","Type":"PASS_FAIL","keywords":"đạt/không đạt"},
    {"Code":"RANGE","Type":"RANGE","keywords":"khoảng; range"},
]
def _to_float(x): 
    try: return float(x)
    except: return None
def _coerce_weight(w):
    w = _to_float(w) or 0.0
    return w/100.0 if w>1 else max(w,0.0)
def _safe_eval_expr(expr, env):
    allowed_names = {"min":min,"max":max,"abs":abs,"round":round,"math":math}
    allowed_vars  = {k:(v if v is not None else 0.0) for k,v in env.items()}
    code = ast.parse(expr, mode="eval")
    for node in ast.walk(code):
        if isinstance(node, ast.Call):
            if isinstance(node.func, ast.Name):
                if node.func.id not in allowed_names: raise ValueError("Func not allowed")
            elif isinstance(node.func, ast.Attribute):
                if not (isinstance(node.func.value, ast.Name) and node.func.value.id=="math"):
                    raise ValueError("Only math.* allowed")
        elif not isinstance(node,(ast.Expression,ast.BinOp,ast.UnaryOp,ast.Num,ast.Name,ast.Load,
                                  ast.Add,ast.Sub,ast.Mult,ast.Div,ast.Pow,ast.Mod,ast.FloorDiv,
                                  ast.USub,ast.UAdd,ast.Call,ast.Attribute,ast.Constant,ast.Compare,
                                  ast.Gt,ast.Lt,ast.GtE,ast.LtE,ast.Eq,ast.NotEq,ast.BoolOp,ast.And,ast.Or,ast.IfExp)):
            raise ValueError("Unsafe")
    return eval(compile(code,"<expr>","eval"),{"__builtins__":{},**allowed_names},allowed_vars)
def load_rules_registry():
    global _RULES_CACHE
    if _RULES_CACHE is not None: return _RULES_CACHE
    try:
        sh = open_spreadsheet(st.session_state.get("spreadsheet_id",""))
        try:
            ws = sh.worksheet("RULES")
            recs = ws.get_all_records(expected_headers=ws.row_values(1))
            rules = []
            for r in recs:
                rule = {k.strip(): v for k,v in r.items()}
                rule["Code"] = str(rule.get("Code") or "").strip()
                rule["Type"] = str(rule.get("Type") or "").strip().upper()
                for k in ("thr","step","pen","cap"):
                    rule[k] = _to_float(rule.get(k)) if (str(rule.get(k) or "")!="") else None
                for k in ("op","lo","hi"):
                    rule[k] = rule.get(k) if str(rule.get(k) or "")!="" else None
                rule["expr"] = str(rule.get("expr") or "").strip()
                rule["keywords"] = str(rule.get("keywords") or "").lower()
                if rule["Code"] and rule["Type"]:
                    rules.append(rule)
            if rules:
                _RULES_CACHE = rules
                return _RULES_CACHE
        except Exception:
            pass
    except Exception:
        pass
    _RULES_CACHE = _RULES_DEFAULT
    return _RULES_CACHE
def _parse_overrides(txt):
    code, overrides = None, {}
    m = re.search(r"\[([A-Za-z0-9_]+)\]", str(txt))
    if m: code = m.group(1).strip().upper()
    for k,v in re.findall(r"([A-Za-z_]+)\s*=\s*([0-9\.,-]+)", str(txt)):
        k = k.strip().lower(); v = v.strip().replace(".","").replace(",",".")
        overrides[k] = _to_float(v) if k!="op" else v
    mop = re.search(r"op\s*=\s*(<=|>=)", str(txt))
    if mop: overrides["op"] = mop.group(1)
    return code, overrides
def _match_rule(method_text, kpi_name=None):
    rules = load_rules_registry()
    txt = (method_text or "").strip()
    code, overrides = _parse_overrides(txt)
    if code:
        for r in rules:
            if r.get("Code","").upper()==code: return r, overrides
    t = txt.lower()
    for r in rules:
        kw = r.get("keywords","")
        if any(k.strip() and k.strip() in t for k in kw.split(";")):
            return r, {}
    if kpi_name:
        name = str(kpi_name)
        if "≤" in name or "<=" in name.lower(): return {"Code":"RATIO_DOWN_AUTO","Type":"RATIO_DOWN"}, {}
        if "≥" in name or ">=" in name.lower(): return {"Code":"RATIO_UP_AUTO","Type":"RATIO_UP"}, {}
    return None, {}
def _deduce_op_from_name(row):
    name = str(row.get("Tên chỉ tiêu (KPI)") or "")
    name_l = name.lower()
    if "≤" in name or "<=" in name_l or "≤ kế hoạch" in name_l: return "<="
    if "≥" in name or ">=" in name_l: return ">="
    return "<="
def _score_penalty_err(row, rule, overrides):
    plan   = parse_vn_number(st.session_state.get("plan_txt","")) if "plan_txt" in st.session_state else None
    actual = parse_vn_number(st.session_state.get("actual_txt","")) if "actual_txt" in st.session_state else None
    if plan is None:   plan   = parse_float(row.get("Kế hoạch"))
    if actual is None: actual = parse_float(row.get("Thực hiện"))
    thr  = overrides.get("thr",  rule.get("thr",1.5))
    step = overrides.get("step", rule.get("step",0.1))
    pen  = overrides.get("pen",  rule.get("pen",0.04))
    cap  = overrides.get("cap",  rule.get("cap",3.0))
    unit = str(row.get("Đơn vị tính") or "").lower()
    err_pct = None
    if actual is not None:
        if actual<=5 or ("%" in unit and actual<=100):
            err_pct = to_percent(actual)
        elif plan not in (None,0):
            err_pct = abs(actual-plan)/abs(plan)*100.0
    exceed = max(0.0, (err_pct or 0.0)-(thr or 0.0))
    steps  = int(exceed // (step or 0.1))
    penalty = min(cap or 3.0, steps*(pen or 0.04))
    return -round(penalty,2)
def _score_penalty_flag(row, rule, overrides):
    plan   = parse_vn_number(st.session_state.get("plan_txt","")) if "plan_txt" in st.session_state else None
    actual = parse_vn_number(st.session_state.get("actual_txt","")) if "actual_txt" in st.session_state else None
    if plan is None:   plan   = parse_float(row.get("Kế hoạch"))
    if actual is None: actual = parse_float(row.get("Thực hiện"))
    pen = overrides.get("pen", rule.get("pen",0.25))
    op  = overrides.get("op",  rule.get("op")) or _deduce_op_from_name(row)
    if plan is None or actual is None: return None
    violated = (actual>plan) if op=="<=" else (actual<plan)
    return -float(pen) if violated else 0.0
def _score_ratio_up(row):
    plan   = parse_vn_number(st.session_state.get("plan_txt","")) if "plan_txt" in st.session_state else None
    actual = parse_vn_number(st.session_state.get("actual_txt","")) if "actual_txt" in st.session_state else None
    if plan is None:   plan   = parse_float(row.get("Kế hoạch"))
    if actual is None: actual = parse_float(row.get("Thực hiện"))
    w = _coerce_weight(row.get("Trọng số"))
    if plan in (None,0) or actual is None: return None
    return round(max(min(actual/plan,2.0),0.0)*10*w,2)
def _score_ratio_down(row):
    plan   = parse_vn_number(st.session_state.get("plan_txt","")) if "plan_txt" in st.session_state else None
    actual = parse_vn_number(st.session_state.get("actual_txt","")) if "actual_txt" in st.session_state else None
    if plan is None:   plan   = parse_float(row.get("Kế hoạch"))
    if actual is None: actual = parse_float(row.get("Thực hiện"))
    w = _coerce_weight(row.get("Trọng số"))
    if plan in (None,0) or actual is None: return None
    ratio = 1.0 if actual<=plan else max(min(plan/actual,2.0),0.0)
    return round(ratio*10*w,2)
def _score_pass_fail(row):
    plan   = parse_vn_number(st.session_state.get("plan_txt","")) if "plan_txt" in st.session_state else None
    actual = parse_vn_number(st.session_state.get("actual_txt","")) if "actual_txt" in st.session_state else None
    if plan is None:   plan   = parse_float(row.get("Kế hoạch"))
    if actual is None: actual = parse_float(row.get("Thực hiện"))
    w = _coerce_weight(row.get("Trọng số"))
    if plan is None or actual is None: return None
    return round((10.0 if actual>=plan else 0.0)*w,2)
def _score_range(row, overrides):
    lo = overrides.get("lo", parse_float(row.get("Ngưỡng dưới")))
    hi = overrides.get("hi", parse_float(row.get("Ngưỡng trên")))
    actual = parse_vn_number(st.session_state.get("actual_txt","")) if "actual_txt" in st.session_state else None
    if actual is None: actual = parse_float(row.get("Thực hiện"))
    w = _coerce_weight(row.get("Trọng số"))
    if lo is None or hi is None or actual is None: return None
    return round((10.0 if (lo<=actual<=hi) else 0.0)*w,2)
def _score_expr(row, expr):
    plan   = parse_vn_number(st.session_state.get("plan_txt","")) if "plan_txt" in st.session_state else None
    actual = parse_vn_number(st.session_state.get("actual_txt","")) if "actual_txt" in st.session_state else None
    if plan is None:   plan   = parse_float(row.get("Kế hoạch"))
    if actual is None: actual = parse_float(row.get("Thực hiện"))
    w = _coerce_weight(row.get("Trọng số"))
    lo = parse_float(row.get("Ngưỡng dưới")); hi = parse_float(row.get("Ngưỡng trên"))
    try:
        val = _safe_eval_expr(expr, {"PLAN":plan,"ACTUAL":actual,"W":w,"LO":lo,"HI":hi})
        return None if val is None else float(val)
    except Exception:
        return None
def compute_score_with_method(row):
    method_text = str(row.get("Phương pháp đo kết quả") or "").strip()
    rule, overrides = _match_rule(method_text, kpi_name=row.get("Tên chỉ tiêu (KPI)"))
    if rule:
        t = rule.get("Type","").upper()
        if   t=="PENALTY_ERR":  return _score_penalty_err(row, rule, overrides)
        elif t=="PENALTY_FLAG": return _score_penalty_flag(row, rule, overrides)
        elif t=="RATIO_UP":     return _score_ratio_up(row)
        elif t=="RATIO_DOWN":   return _score_ratio_down(row)
        elif t=="PASS_FAIL":    return _score_pass_fail(row)
        elif t=="RANGE":        return _score_range(row, overrides)
        elif t=="EXPR" and rule.get("expr"): return _score_expr(row, rule["expr"])
    # fallback hợp lý
    plan   = parse_vn_number(st.session_state.get("plan_txt","")) if "plan_txt" in st.session_state else None
    actual = parse_vn_number(st.session_state.get("actual_txt","")) if "actual_txt" in st.session_state else None
    if plan is None:   plan   = parse_float(row.get("Kế hoạch"))
    if actual is None: actual = parse_float(row.get("Thực hiện"))
    weight = parse_float(row.get("Trọng số")) or 0.0
    if plan in (None,0) or actual is None: return None
    w = weight/100.0 if (weight and weight>1) else (weight or 0.0)
    ratio = max(min(actual/plan,2.0),0.0)
    return round(ratio*10*w,2)

NUMERIC_COLS = ["Kế hoạch","Thực hiện","Trọng số","Ngưỡng dưới","Ngưỡng trên","Điểm KPI"]
def coerce_numeric_cols(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    for c in NUMERIC_COLS:
        if c in df.columns:
            df[c] = pd.to_numeric(df[c], errors="coerce")
    return df

# ------------------- ĐĂNG NHẬP -------------------
def find_use_worksheet(sh):
    try: return sh.worksheet("USE")
    except Exception:
        for ws in sh.worksheets():
            try: headers = [h.strip() for h in ws.row_values(1)]
            except Exception: continue
            if (("USE (mã đăng nhập)" in headers) or ("Tài khoản (USE\\username)" in headers)
                or ("Tài khoản" in headers) or ("Username" in headers) or ("USE" in headers)) \
               and ("Mật khẩu mặc định" in headers or "Password" in headers or "Mật khẩu" in headers):
                return ws
        raise gspread.exceptions.WorksheetNotFound("Không tìm thấy sheet USE.")
def load_users_df():
    sh = open_spreadsheet(st.session_state.get("spreadsheet_id",""))
    ws = find_use_worksheet(sh)
    return normalize_columns(df_from_ws(ws))
def check_credentials(use_name: str, password: str) -> bool:
    df = load_users_df()
    if df.empty: return False
    col_use = next((c for c in df.columns if c.strip().lower() in ["tài khoản (use\\username)","tài khoản","username","use (mã đăng nhập)","use"]), None)
    col_pw  = next((c for c in df.columns if c.strip().lower() in ["mật khẩu mặc định","password mặc định","password","mật khẩu"]), None)
    if not col_use or not col_pw: return False
    u = (use_name or "").strip().lower(); p = (password or "").strip()
    row = df.loc[df[col_use].astype(str).str.strip().str.lower()==u]
    return (not row.empty) and (str(row.iloc[0][col_pw]).strip()==p)

# ------------------- DRIVE -------------------
def get_drive_service():
    if gbuild is None:
        st.warning("Thiếu google-api-python-client để thao tác Drive.")
        return None
    gclient, creds = st.session_state.get("_gs_pair",(None,None))
    if creds is None:
        gclient, creds = get_gs_clients()
        st.session_state["_gs_pair"] = (gclient, creds)
    if creds is None: return None
    return gbuild("drive","v3",credentials=creds)
def ensure_parent_ok(service, parent_id):
    try: service.files().get(fileId=parent_id, fields="id,name").execute()
    except HttpError as e: raise RuntimeError(f"Không truy cập được thư mục gốc ID: {parent_id}") from e
def ensure_folder(service, parent_id, name):
    ensure_parent_ok(service, parent_id)
    q = f"mimeType='application/vnd.google-apps.folder' and name='{name}' and '{parent_id}' in parents and trashed=false"
    res = service.files().list(q=q, spaces="drive", supportsAllDrives=True, includeItemsFromAllDrives=True, fields="files(id,name)").execute()
    items = res.get("files", [])
    if items: return items[0]["id"]
    meta = {"name":name,"mimeType":"application/vnd.google-apps.folder","parents":[parent_id]}
    folder = service.files().create(body=meta, fields="id", supportsAllDrives=True).execute()
    return folder["id"]
def upload_new(service, parent_id, filename, data, mime):
    media = MediaIoBaseUpload(io.BytesIO(data), mimetype=mime, resumable=False)
    meta = {"name":filename,"parents":[parent_id]}
    f = service.files().create(body=meta, media_body=media, fields="id", supportsAllDrives=True).execute()
    return f["id"]
def save_report_to_drive(excel_bytes, x_ext, x_mime, pdf_bytes=None):
    service = get_drive_service()
    if service is None:
        st.warning("Chưa cài google-api-python-client."); return False, "no_client"
    root_raw = st.session_state.get("drive_root_id","").strip()
    if not root_raw:
        st.error("Chưa khai báo ID/URL thư mục gốc (của đơn vị)."); return False, "no_root"
    root_id = extract_drive_folder_id(root_raw)
    try:
        folder_kpi = ensure_folder(service, root_id, "Báo cáo KPI")
        month_name = datetime.now().strftime("%Y-%m")
        folder_month = ensure_folder(service, folder_kpi, month_name)
        ts = datetime.now().strftime("%Y-%m-%d_%H%M")
        fname_x = f"KPI_{ts}.{x_ext}"
        upload_new(service, folder_month, fname_x, excel_bytes, x_mime)
        toast(f"✅ Đã lưu Drive: /Báo cáo KPI/{month_name}/{fname_x}", "✅")
        if pdf_bytes:
            try:
                fname_pdf = f"KPI_{ts}.pdf"
                upload_new(service, folder_month, fname_pdf, pdf_bytes, "application/pdf")
                toast(f"✅ Đã lưu thêm PDF: {fname_pdf}", "✅")
            except Exception as e:
                st.info(f"Không tạo được PDF: {e}")
        return True, "ok"
    except Exception as e:
        st.error(f"Lỗi lưu Google Drive: {e}")
        return False, str(e)

# ------------------- EXPORT -------------------
def df_to_report_bytes(df: pd.DataFrame):
    try:
        buf = io.BytesIO()
        with pd.ExcelWriter(buf, engine="openpyxl") as writer:
            df.to_excel(writer, index=False, sheet_name="KPI")
        return buf.getvalue(),"xlsx","application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
    except Exception:
        pass
    try:
        buf = io.BytesIO()
        with pd.ExcelWriter(buf, engine="xlsxwriter") as writer:
            df.to_excel(writer, index=False, sheet_name="KPI")
        return buf.getvalue(),"xlsx","application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
    except Exception:
        data = df.to_csv(index=False).encode("utf-8")
        return data,"csv","text/csv"
def generate_pdf_from_df(df: pd.DataFrame, title="BÁO CÁO KPI"):
    try:
        from reportlab.lib.pagesizes import A4, landscape
        from reportlab.platypus import SimpleDocTemplate, Table, TableStyle, Paragraph, Spacer
        from reportlab.lib import colors
        from reportlab.lib.units import cm
        from reportlab.lib.styles import getSampleStyleSheet
        buf = io.BytesIO()
        doc = SimpleDocTemplate(buf, pagesize=landscape(A4), rightMargin=20,leftMargin=20,topMargin=20,bottomMargin=20)
        styles = getSampleStyleSheet()
        story = [Paragraph(title, styles["Title"]), Spacer(1, 0.3*cm)]
        cols = list(df.columns)
        data = [cols] + df.fillna("").astype(str).values.tolist()
        t = Table(data, repeatRows=1)
        t.setStyle(TableStyle([("BACKGROUND",(0,0),(-1,0),colors.lightgrey),
                               ("GRID",(0,0),(-1,-1),0.25,colors.grey),
                               ("FONTSIZE",(0,0),(-1,-1),8),("ALIGN",(0,0),(-1,-1),"CENTER")]))
        story.append(t); doc.build(story)
        return buf.getvalue()
    except Exception:
        return b""

# ------------------- SIDEBAR -------------------
with st.sidebar:
    st.header("🔒 Đăng nhập")
    if "_user" not in st.session_state:
        with st.form("login_form", clear_on_submit=False):
            use_input = st.text_input("USE (vd: PCTN\\KVDHA)")
            pwd_input = st.text_input("Mật khẩu", type="password")
            ok = st.form_submit_button("Đăng nhập", use_container_width=True)
        if ok:
            if check_credentials(use_input, pwd_input):
                st.session_state["_user"] = use_input.strip()
                toast("Đăng nhập thành công.","✅"); st.rerun()
            else:
                st.error("USE hoặc mật khẩu không đúng.")
    else:
        st.success(f"Đang đăng nhập: **{st.session_state['_user']}**")
        st.subheader("🧩 Kết nối Google Sheets")
        st.text_input("ID/URL Google Sheet", key="spreadsheet_id")
        st.text_input("Tên sheet KPI", key="kpi_sheet_name")
        st.subheader("📁 Lưu Google Drive (mỗi đơn vị dùng ROOT của chính mình)")
        st.text_input("ID/URL thư mục gốc (của đơn vị)", key="drive_root_id",
                      help="Dán URL thư mục hoặc ID. Service account phải có quyền Editor/Content manager.")
        st.checkbox("Tự động lưu Drive khi Ghi/Xuất", key="auto_save_drive")
        if st.button("Đăng xuất", use_container_width=True):
            st.session_state.pop("_user", None); toast("Đã đăng xuất.","✅"); st.rerun()

# ------------------- HEADER & CSS -------------------
def _img64_local(path: Path):
    try:
        if path.exists(): return base64.b64encode(path.read_bytes()).decode("utf-8")
    except Exception: pass
    return None
LOGO_PATH = Path("assets/logo.png")
logo64 = _img64_local(LOGO_PATH)

st.markdown(f"""
<style>
.app-header {{ display:flex; align-items:center; gap:12px; margin:2px 0 10px; }}
.app-logo {{ width:52px; height:52px; border-radius:50%; box-shadow:0 0 0 3px #fff, 0 0 0 6px #ff4b4b20; }}
.app-title {{ margin:0; line-height:1.05; font-size:22px; font-weight:800; letter-spacing:.15px;
  background: linear-gradient(90deg,#0ea5e9 0%,#22c55e 50%,#a855f7 100%);
  -webkit-background-clip:text; -webkit-text-fill-color:transparent; }}
.app-sub {{ margin:0; color:#64748b; font-size:12px; }}

/* STICKY form box */
.kpi-sticky {{ position: sticky; top: 8px; z-index: 60; background:#fff; border:1px solid #eef2f7;
  border-radius:14px; padding:12px 14px; box-shadow:0 6px 18px -10px rgba(0,0,0,.18); }}

/* Nút nhiều màu – dùng marker + nút liền kề để tránh ảnh hưởng nút khác */
div.btn-save + div.stButton > button {{ background:#22c55e !important; color:#fff !important; border-color:#22c55e !important; }}
div.btn-refresh + div.stButton > button {{ background:#f59e0b !important; color:#111 !important; border-color:#f59e0b !important; }}
div.btn-export + div.stButton > button {{ background:#3b82f6 !important; color:#fff !important; border-color:#3b82f6 !important; }}
div.btn-drive + div.stButton > button {{ background:#8b5cf6 !important; color:#fff !important; border-color:#8b5cf6 !important; }}

/* Metric tổng điểm đậm hơn 1 chút */
div[data-testid="stMetricValue"] {{ font-weight: 800; }}
</style>
<div class="app-header">
  {"<img class='app-logo' src='"+LOGO_URL+"'>" if LOGO_URL else (("<img class='app-logo' src='data:image/png;base64,"+logo64+"'/>") if logo64 else "<div></div>")}
  <div><h1 class="app-title">KPI – Đội quản lý Điện lực khu vực Định Hóa</h1>
  <p class="app-sub">Biểu mẫu nhập &amp; báo cáo KPI</p></div>
</div>
""", unsafe_allow_html=True)

if "_user" not in st.session_state:
    st.info("Vui lòng đăng nhập để làm việc."); st.stop()

# ------------------- STATE & CỘT KPI -------------------
KPI_COLS = ["Tên chỉ tiêu (KPI)","Đơn vị tính","Kế hoạch","Thực hiện","Trọng số","Bộ phận/người phụ trách",
            "Tháng","Năm","Phương pháp đo kết quả","Ngưỡng dưới","Ngưỡng trên","Điểm KPI","Ghi chú","Tên đơn vị"]

def get_sheet_and_name():
    sid_cfg = st.session_state.get("spreadsheet_id","") or GOOGLE_SHEET_ID_DEFAULT
    sheet_name = st.session_state.get("kpi_sheet_name") or KPI_SHEET_DEFAULT
    sh = open_spreadsheet(sid_cfg)
    return sh, sheet_name

def write_kpi_to_sheet(sh, sheet_name, df):
    df = normalize_columns(df.copy()); df = coerce_numeric_cols(df)
    if "Điểm KPI" not in df.columns:
        df["Điểm KPI"] = df.apply(compute_score_with_method, axis=1)
    cols = [c for c in KPI_COLS if c in df.columns] + [c for c in df.columns if c not in KPI_COLS]
    data = [cols] + df[cols].fillna("").astype(str).values.tolist()
    try:
        try:
            ws = sh.worksheet(sheet_name); ws.clear()
        except Exception:
            ws = sh.add_worksheet(title=sheet_name, rows=len(data)+10, cols=max(12,len(cols)))
        ws.update(data, value_input_option="USER_ENTERED"); return True
    except Exception as e:
        st.error(f"Lưu KPI thất bại: {e}"); return False

if "_csv_form" not in st.session_state:
    st.session_state["_csv_form"] = {
        "Tên chỉ tiêu (KPI)":"", "Đơn vị tính":"", "Kế hoạch":0.0, "Thực hiện":0.0, "Trọng số":100.0,
        "Bộ phận/người phụ trách":"", "Tháng":str(datetime.now().month), "Năm":str(datetime.now().year),
        "Phương pháp đo kết quả":"Tăng tốt hơn", "Ngưỡng dưới":"", "Ngưỡng trên":"", "Ghi chú":"", "Tên đơn vị":""
    }

st.session_state.setdefault("plan_txt",   format_vn_number(st.session_state["_csv_form"].get("Kế hoạch") or 0.0, 2))
st.session_state.setdefault("actual_txt", format_vn_number(st.session_state["_csv_form"].get("Thực hiện") or 0.0, 2))

# ------------------- FORM (GHIM CỨNG) -------------------
st.subheader("✍️ Biểu mẫu nhập tay")
with st.container():
    st.markdown('<div class="kpi-sticky">', unsafe_allow_html=True)

    f = st.session_state["_csv_form"]

    def _on_change_plan():
        val = parse_vn_number(st.session_state["plan_txt"])
        if val is not None: st.session_state["_csv_form"]["Kế hoạch"] = val
        st.session_state["plan_txt"] = format_vn_number(st.session_state["_csv_form"]["Kế hoạch"] or 0, 2)
    def _on_change_actual():
        val = parse_vn_number(st.session_state["actual_txt"])
        if val is not None: st.session_state["_csv_form"]["Thực hiện"] = val
        st.session_state["actual_txt"] = format_vn_number(st.session_state["_csv_form"]["Thực hiện"] or 0, 2)

    c0 = st.columns([2,1,1,1])
    with c0[0]: f["Tên chỉ tiêu (KPI)"] = st.text_input("Tên chỉ tiêu (KPI)", value=f["Tên chỉ tiêu (KPI)"])
    with c0[1]: f["Đơn vị tính"] = st.text_input("Đơn vị tính", value=f["Đơn vị tính"])
    with c0[2]: f["Bộ phận/người phụ trách"] = st.text_input("Bộ phận/người phụ trách", value=f["Bộ phận/người phụ trách"])
    with c0[3]: f["Tên đơn vị"] = st.text_input("Tên đơn vị", value=f["Tên đơn vị"])

    c1 = st.columns(3)
    with c1[0]: st.text_input("Kế hoạch", key="plan_txt", on_change=_on_change_plan)
    with c1[1]: st.text_input("Thực hiện", key="actual_txt", on_change=_on_change_actual)
    with c1[2]: f["Trọng số"] = st.number_input("Trọng số (%)", value=float(f.get("Trọng số") or 0.0))

    c2 = st.columns(3)
    with c2[0]:
        options_methods = [
            "Tăng tốt hơn","Giảm tốt hơn","Đạt/Không đạt","Trong khoảng",
            "Phạt khi vi phạm (trừ cố định)",
            "Sai số ±1,5%: trừ 0,04 điểm/0,1% (max 3)",
            "Sai số ±1,5%: trừ 0,02 điểm/0,1% (max 3)",
        ]
        cur = f.get("Phương pháp đo kết quả","Tăng tốt hơn")
        f["Phương pháp đo kết quả"] = st.selectbox("Phương pháp đo kết quả", options=options_methods,
                                                   index=options_methods.index(cur) if cur in options_methods else 0)
    with c2[1]:
        tmp_row = {k:f.get(k) for k in f.keys()}
        tmp_row["Điểm KPI"] = compute_score_with_method(tmp_row)
        label_metric = "Điểm trừ (tự tính)" if (tmp_row["Điểm KPI"] is not None and tmp_row["Điểm KPI"]<0) else "Điểm KPI (tự tính)"
        st.metric(label_metric, tmp_row["Điểm KPI"] if tmp_row["Điểm KPI"] is not None else "—")
    with c2[2]:
        f["Ghi chú"] = st.text_input("Ghi chú", value=f["Ghi chú"])

    if "khoảng" in f["Phương pháp đo kết quả"].lower():
        c3 = st.columns(2)
        with c3[0]: f["Ngưỡng dưới"] = st.text_input("Ngưỡng dưới", value=str(f.get("Ngưỡng dưới") or ""))
        with c3[1]: f["Ngưỡng trên"] = st.text_input("Ngưỡng trên", value=str(f.get("Ngưỡng trên") or ""))

    c4 = st.columns(2)
    with c4[0]: f["Tháng"] = st.text_input("Tháng", value=str(f["Tháng"]))
    with c4[1]: f["Năm"]   = st.text_input("Năm",   value=str(f["Năm"]))

    # Núi màu
    b1,b2,b3,b4,b5 = st.columns([1,1,1,1,1.4])
    with b1:
        st.markdown('<div class="btn-save"></div>', unsafe_allow_html=True)
        save_csv_clicked = st.button("💾 Ghi CSV tạm vào sheet KPI", use_container_width=True)
    with b2:
        st.markdown('<div class="btn-refresh"></div>', unsafe_allow_html=True)
        refresh_clicked = st.button("🔁 Làm mới bảng CSV", use_container_width=True)
    with b3:
        st.markdown('<div class="btn-export"></div>', unsafe_allow_html=True)
        export_clicked = st.button("📤 Xuất báo cáo (Excel/PDF)", use_container_width=True)
    with b4:
        st.markdown('<div class="btn-drive"></div>', unsafe_allow_html=True)
        save_drive_clicked = st.button("☁️ Lưu dữ liệu vào Google Drive (thủ công)", use_container_width=True)
    with b5:
        # Tổng điểm KPI (tạm tính) – lấy từ cache hiện tại
        total_score = None
        if "_csv_cache" in st.session_state and not st.session_state["_csv_cache"].empty:
            total_score = round(pd.to_numeric(st.session_state["_csv_cache"].get("Điểm KPI", pd.Series(dtype=float)), errors="coerce").fillna(0).sum(), 2)
        st.metric("Tổng điểm KPI (tạm tính)", total_score if total_score is not None else "—")

    st.markdown('</div>', unsafe_allow_html=True)  # đóng .kpi-sticky

# ------------------- CSV khu vực dưới -------------------
st.subheader("⬇️ Nhập CSV vào KPI")
up = st.file_uploader("Tải file CSV", type=["csv"])

if "_csv_cache" not in st.session_state:
    st.session_state["_csv_cache"] = pd.DataFrame(columns=KPI_COLS)

if up is not None:
    up_bytes = up.getvalue()
    sig = f"{getattr(up,'name','')}:{len(up_bytes)}"
    if st.session_state.get("_csv_loaded_sig") != sig or st.session_state["_csv_cache"].empty:
        try: tmp = pd.read_csv(io.BytesIO(up_bytes))
        except Exception: tmp = pd.read_csv(io.BytesIO(up_bytes), encoding="utf-8-sig")
        tmp = normalize_columns(tmp); tmp = coerce_numeric_cols(tmp)
        if "Điểm KPI" not in tmp.columns:
            tmp["Điểm KPI"] = tmp.apply(compute_score_with_method, axis=1)
        st.session_state["_csv_cache"] = tmp
        st.session_state["_csv_loaded_sig"] = sig

base = st.session_state["_csv_cache"]
df_show = base.copy()
if "✓ Chọn" not in df_show.columns:
    df_show.insert(0,"✓ Chọn",False)
df_show["✓ Chọn"] = df_show["✓ Chọn"].astype("bool")
sel = st.session_state.get("_selected_idx", None)
if sel is not None and sel in df_show.index:
    df_show.loc[sel,"✓ Chọn"] = True

df_edit = st.data_editor(
    df_show, use_container_width=True, hide_index=True, num_rows="dynamic",
    column_config={"✓ Chọn": st.column_config.CheckboxColumn(label="✓ Chọn", default=False,
                                                             help="Chọn 1 dòng để nạp lên biểu mẫu")},
    key="csv_editor",
)

df_cache = df_edit.drop(columns=["✓ Chọn"], errors="ignore").copy()
df_cache = coerce_numeric_cols(df_cache)
st.session_state["_csv_cache"] = df_cache

new_selected_idxs = df_edit.index[df_edit["✓ Chọn"]==True].tolist()
new_sel = new_selected_idxs[0] if new_selected_idxs else None
if new_sel != st.session_state.get("_selected_idx"):
    st.session_state["_selected_idx"] = new_sel
    if new_sel is not None:
        st.session_state["_csv_form"].update({k: df_cache.loc[new_sel].get(k, "") for k in KPI_COLS})
        st.session_state["plan_txt"]   = format_vn_number(parse_float(df_cache.loc[new_sel].get("Kế hoạch")  or 0), 2)
        st.session_state["actual_txt"] = format_vn_number(parse_float(df_cache.loc[new_sel].get("Thực hiện") or 0), 2)
    st.rerun()

# --- Apply form vào cache (dùng chung cho các nút) ---
def apply_form_to_cache():
    base = st.session_state["_csv_cache"].copy()
    base = coerce_numeric_cols(base)
    new_row = {c: st.session_state["_csv_form"].get(c,"") for c in KPI_COLS}
    new_row["Kế hoạch"] = parse_vn_number(st.session_state.get("plan_txt",""))
    new_row["Thực hiện"] = parse_vn_number(st.session_state.get("actual_txt",""))
    new_row["Điểm KPI"] = compute_score_with_method(new_row)
    sel = st.session_state.get("_selected_idx", None)
    if sel is not None and sel in base.index:
        for k,v in new_row.items():
            if k in NUMERIC_COLS:
                base.loc[sel,k] = pd.to_numeric(pd.Series([v]), errors="coerce").iloc[0]
            else:
                base.loc[sel,k] = "" if v is None else str(v)
    else:
        base = pd.concat([base, pd.DataFrame([new_row])], ignore_index=True)
        base = coerce_numeric_cols(base)
    st.session_state["_csv_cache"] = base

# --------- Hành động nút ----------
if save_csv_clicked:
    try:
        apply_form_to_cache()
        sh, sheet_name = get_sheet_and_name()
        if write_kpi_to_sheet(sh, sheet_name, st.session_state["_csv_cache"]):
            toast(f"Đã ghi vào sheet '{sheet_name}'.","✅")
            st.rerun()
    except Exception as e:
        st.error(f"Lỗi khi ghi Sheets: {e}")

if refresh_clicked:
    if "confirm_refresh" not in st.session_state:
        st.session_state["confirm_refresh"] = True
    else:
        st.session_state["confirm_refresh"] = not st.session_state["confirm_refresh"]

if st.session_state.get("confirm_refresh", False):
    with st.expander("❓ Làm mới dữ liệu? (Sẽ mất thay đổi chưa ghi)", expanded=True):
        c = st.columns(2)
        if c[0].button("Có, làm mới ngay", type="primary"):
            st.session_state["_csv_cache"] = pd.DataFrame(columns=KPI_COLS)
            st.session_state["_selected_idx"] = None
            st.session_state["confirm_refresh"] = False
            toast("Đã làm mới CSV tạm.","✅"); st.rerun()
        if c[1].button("Không, giữ nguyên"):
            st.session_state["confirm_refresh"] = False; toast("Đã hủy làm mới.","ℹ️")

if export_clicked:
    apply_form_to_cache()
    x_bytes,x_ext,x_mime = df_to_report_bytes(st.session_state["_csv_cache"])
    ts_name = datetime.now().strftime("KPI_%Y-%m-%d_%H%M")
    st.download_button("⬇️ Tải báo cáo", data=x_bytes, file_name=f"{ts_name}.{x_ext}", mime=x_mime)
    pdf_bytes = generate_pdf_from_df(st.session_state["_csv_cache"], "BÁO CÁO KPI")
    if pdf_bytes:
        st.download_button("⬇️ Tải PDF báo cáo", data=pdf_bytes, file_name=f"{ts_name}.pdf", mime="application/pdf")

if save_drive_clicked:
    try:
        apply_form_to_cache()
        x_bytes,x_ext,x_mime = df_to_report_bytes(st.session_state["_csv_cache"])
        pdf_bytes = generate_pdf_from_df(st.session_state["_csv_cache"], "BÁO CÁO KPI")
        save_report_to_drive(x_bytes,x_ext,x_mime,pdf_bytes if pdf_bytes else None)
    except Exception as e:
        st.error(f"Lỗi lưu Google Drive: {e}")
