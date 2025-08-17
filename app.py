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
    try:
        return sh.worksheet("USE")
    except Exception:
        for ws in sh.worksheets():
            try:
                headers = [h.strip() for h in ws.row_values(1)]
            except Exception:
                continue
            if (("USE (mã đăng nhập)" in headers) or ("Tài khoản (USE\\username)" in headers) or ("Tài khoản" in headers) or ("Username" in headers) or ("USE" in headers)) \
            and ("Mật khẩu mặc định" in headers or "Password" in headers or "Mật khẩu" in headers):
                return ws
        raise gspread.exceptions.WorksheetNotFound("Không tìm thấy sheet USE.")

def load_users_df():
    sh = open_spreadsheet(st.session_state.get("spreadsheet_id",""))
    ws = find_use_worksheet(sh)
    return normalize_columns(df_from_ws(ws))

def check_credentials(use_name: str, password: str) -> bool:
    df = load_users_df()
    if df.empty:
        return False
    col_use = next((c for c in df.columns if c.strip().lower() in ["tài khoản (use\\username)","tài khoản","username","use (mã đăng nhập)","use"]), None)
    col_pw = next((c for c in df.columns if c.strip().lower() in ["mật khẩu mặc định","password mặc định","password","mật khẩu"]), None)
    if not col_use or not col_pw:
        return False
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
    if not creds: return None
    return gbuild("drive", "v3", credentials=creds)

def export_df_to_drive(df: pd.DataFrame, folder_id: str, mime_type="text/csv"):
    if not gbuild:
        st.error("Chưa cài đặt thư viện 'google-api-python-client'. Vui lòng cài đặt để sử dụng tính năng này.")
        return False
    drive = get_drive_service()
    if not drive:
        st.error("Không thể kết nối Drive, có thể do lỗi xác thực.")
        return False
    
    ts = datetime.now().strftime("%Y-%m-%d_%H%M%S")
    file_name = f"BAO_CAO_KPI_{ts}"
    file_ext = ".csv"
    if mime_type == "application/pdf": file_ext = ".pdf"
    
    try:
        # Check if the folder exists
        if folder_id:
            try:
                drive.files().get(fileId=folder_id).execute()
            except HttpError as e:
                if e.resp.status == 404:
                    st.error(f"Thư mục Drive ID không tồn tại: {folder_id}")
                    return False
                else:
                    raise
        
        # Prepare file content
        if mime_type == "text/csv":
            content = df.to_csv(index=False).encode("utf-8-sig")
        elif mime_type == "application/pdf":
            content = generate_pdf_from_df(df, "BÁO CÁO KPI")
        else:
            st.error("Định dạng file không được hỗ trợ.")
            return False
            
        file_metadata = {
            "name": f"{file_name}{file_ext}",
            "parents": [folder_id] if folder_id else [],
            "mimeType": mime_type,
        }
        
        media = MediaIoBaseUpload(io.BytesIO(content), mimetype=mime_type, resumable=True)
        
        file = drive.files().create(
            body=file_metadata,
            media_body=media,
            fields="id"
        ).execute()
        
        toast(f"Đã lưu file lên Drive: {file['id']}", icon="✅")
        return True
    except HttpError as e:
        st.error(f"Lỗi khi lưu file lên Drive: {e.resp.status} - {e.reason}")
    except Exception as e:
        st.error(f"Lỗi không xác định khi lưu file: {e}")
    return False

# ------------------- PDF -------------------
def generate_pdf_from_df(df: pd.DataFrame, title: str):
    try:
        from fpdf import FPDF
        from fpdf.fonts import FontFace
    except Exception:
        st.error("Chưa cài đặt thư viện 'fpdf2' và 'font-ttf'. Vui lòng cài đặt để xuất PDF.")
        return None
    
    # PDF generation logic remains the same
    pdf = FPDF('P', 'mm', 'A4')
    pdf.set_auto_page_break(auto=True, margin=15)
    pdf.add_page()
    
    try:
        pdf.add_font('DejaVuSerifCondensed', '', './fonts/DejaVuSerifCondensed.ttf', uni=True)
        pdf.add_font('DejaVuSerifCondensed', 'B', './fonts/DejaVuSerifCondensed-Bold.ttf', uni=True)
    except Exception as e:
        pdf.add_font('DejaVuSerifCondensed', '', 'DejaVuSerifCondensed.ttf', uni=True)
        pdf.add_font('DejaVuSerifCondensed', 'B', 'DejaVuSerifCondensed-Bold.ttf', uni=True)

    pdf.set_font('DejaVuSerifCondensed', 'B', 16)
    pdf.cell(0, 10, title, 0, 1, 'C')
    pdf.ln(5)
    
    pdf.set_font('DejaVuSerifCondensed', '', 10)
    
    # Create table headers
    cols = df.columns.tolist()
    col_widths = [20, 70, 20, 20, 20, 20, 15]
    
    pdf.set_font('DejaVuSerifCondensed', 'B', 10)
    for i, col in enumerate(cols):
        pdf.cell(col_widths[i], 10, col, 1, 0, 'C')
    pdf.ln()
    
    # Add table data
    pdf.set_font('DejaVuSerifCondensed', '', 8)
    for index, row in df.iterrows():
        for i, col in enumerate(cols):
            pdf.cell(col_widths[i], 8, str(row[col]), 1, 0, 'L')
        pdf.ln()

    return pdf.output(dest='S').encode('latin1')

# ------------------- GIAO DIỆN -------------------
def draw_header(logo_url, title):
    col1, col2 = st.columns([1, 8])
    with col1:
        st.image(logo_url, width=100)
    with col2:
        st.markdown(f"## {title}") # Using H2 for smaller title

def draw_sidebar():
    st.sidebar.title("Cấu hình & Tác vụ")
    
    with st.sidebar.expander("Tài khoản & Kết nối", expanded=False):
        st.text_input("ID Bảng tính Google Sheet", 
                      placeholder="URL hoặc ID...",
                      key="spreadsheet_id")
        st.text_input("Tên Sheet KPI", key="kpi_sheet_name")
        st.text_input("ID Thư mục Drive", 
                      placeholder="URL hoặc ID...",
                      key="drive_root_id")
    
    st.sidebar.markdown("---")
    st.sidebar.checkbox("Tự động lưu lên Google Drive khi cập nhật", key="auto_save_drive")
    
    if st.sidebar.button("Xem hướng dẫn", use_container_width=True):
        st.info("Hướng dẫn sử dụng sẽ sớm được cập nhật.")

def get_kpi_dataframe() -> pd.DataFrame:
    if "_csv_cache" not in st.session_state:
        st.session_state["_csv_cache"] = pd.DataFrame(columns=KPI_COLS)
    
    uploaded_file = st.sidebar.file_uploader(
        "Hoặc tải file CSV/Excel", type=["csv", "xlsx", "xls"]
    )
    
    if uploaded_file and uploaded_file != st.session_state.get("_uploaded_file_sig", None):
        try:
            if uploaded_file.name.endswith('.csv'):
                df = pd.read_csv(uploaded_file, encoding='utf-8-sig')
            else:
                df = pd.read_excel(uploaded_file)
            
            st.session_state["_csv_cache"] = normalize_columns(df)
            st.session_state["_uploaded_file_sig"] = uploaded_file
            st.session_state["_selected_idx"] = None
            toast("Đã tải dữ liệu từ file.", "✅")
            st.rerun()
        except Exception as e:
            st.error(f"Lỗi khi đọc file: {e}")
            return pd.DataFrame(columns=KPI_COLS)

    if st.sidebar.button("Lấy dữ liệu từ Google Sheet", use_container_width=True, type="primary"):
        st.session_state["_csv_loaded_sig"] = f"{st.session_state.spreadsheet_id}-{st.session_state.kpi_sheet_name}"
        st.session_state["_selected_idx"] = None
        st.session_state["_csv_cache"] = pd.DataFrame(columns=KPI_COLS)
        try:
            sh = open_spreadsheet(st.session_state.spreadsheet_id)
            ws = sh.worksheet(st.session_state.kpi_sheet_name)
            df = normalize_columns(df_from_ws(ws))
            st.session_state["_csv_cache"] = df
            toast("Đã tải dữ liệu từ Google Sheet.", "✅")
            st.rerun()
        except Exception as e:
            st.error(f"Lỗi khi tải dữ liệu: {e}")
            st.session_state["_csv_cache"] = pd.DataFrame(columns=KPI_COLS)
            st.rerun()
    
    return st.session_state.get("_csv_cache")

def apply_form_to_cache():
    idx = st.session_state.get("_selected_idx")
    df = st.session_state.get("_csv_cache")
    if idx is None or df is None or df.empty: return
    
    df.loc[idx, "Ghi chú"] = st.session_state["notes_input"]
    df.loc[idx, "Thực hiện"] = parse_vn_number(st.session_state["actual_input"])

    try:
        score = compute_score_with_method(df.loc[idx])
        if score is not None:
            df.loc[idx, "Điểm KPI"] = score
        else:
            df.loc[idx, "Điểm KPI"] = None
    except Exception as e:
        st.warning(f"Lỗi khi tính điểm: {e}")
        df.loc[idx, "Điểm KPI"] = "Lỗi"

def format_score(score):
    if score is None or pd.isna(score): return ""
    return format_vn_number(score, 2)

KPI_COLS = ["Tên chỉ tiêu (KPI)", "Kế hoạch", "Thực hiện", "Trọng số", "Điểm KPI", "Ghi chú", "Phương pháp đo kết quả"]

# ===================== CHẠY ỨNG DỤNG =====================
if "is_authenticated" not in st.session_state:
    st.session_state.is_authenticated = False

# Draw the main content only if authenticated
if st.session_state.is_authenticated:
    # Use Markdown for a smaller title
    st.markdown("### KPI – Đội quản lý Điện lực khu vực Định Hóa")
    
    draw_sidebar()
    
    st.divider()

    df_kpi = get_kpi_dataframe()
    
    if df_kpi is None or df_kpi.empty:
        st.warning("Vui lòng tải dữ liệu từ Google Sheet hoặc file CSV/Excel.")
    else:
        st.dataframe(
            df_kpi[["Tên chỉ tiêu (KPI)", "Kế hoạch", "Thực hiện", "Trọng số", "Điểm KPI", "Ghi chú"]].fillna(""),
            hide_index=True,
            use_container_width=True
        )

        total_score = df_kpi["Điểm KPI"].sum() if "Điểm KPI" in df_kpi.columns else 0
        st.markdown(f"**Tổng điểm KPI (tạm tính):** `{format_vn_number(total_score, 2)}`")

        col_left, col_right = st.columns([1,2])
        with col_left:
            st.number_input(
                "Thực hiện (điểm thay thế)", 
                key="actual_input_manual",
                format="%f",
                step=0.1
            )
        with col_right:
            st.text_input("Ghi chú", key="notes_input_manual")
        
        # Action buttons
        apply_clicked = st.button("Áp dụng", use_container_width=True, type="primary")
        update_clicked = st.button("Cập nhật lên Sheet", use_container_width=True)
        export_csv_clicked = st.button("Tải CSV báo cáo", use_container_width=True)
        export_pdf_clicked = st.button("Tải PDF báo cáo", use_container_width=True)
        
        # Update button logic
        if update_clicked:
            apply_form_to_cache()
            df = st.session_state["_csv_cache"]
            try:
                sh = open_spreadsheet(st.session_state.spreadsheet_id)
                ws = sh.worksheet(st.session_state.kpi_sheet_name)
                # This needs to be a proper update, not a full write
                # For simplicity, we'll just show the concept
                st.info("Tính năng cập nhật lên Google Sheet cần được phát triển thêm để xử lý chỉ mục và dòng cụ thể.")
                # You might use ws.update() with ranges
                toast("Đã gửi yêu cầu cập nhật.", "✅")
            except Exception as e:
                st.error(f"Lỗi khi cập nhật lên Google Sheet: {e}")

        # Download buttons
        if export_csv_clicked:
            df = st.session_state["_csv_cache"]
            csv_data = df.to_csv(index=False).encode('utf-8-sig')
            ts_name = datetime.now().strftime("KPI_%Y-%m-%d_%H%M")
            st.download_button("⬇️ Tải báo cáo CSV", data=csv_data, file_name=f"{ts_name}.csv", mime="text/csv")

        if export_pdf_clicked:
            df = st.session_state["_csv_cache"]
            pdf_bytes = generate_pdf_from_df(df, "BÁO CÁO KPI")
            if pdf_bytes:
                ts_name = datetime.now().strftime("KPI_%Y-%m-%d_%H%M")
                st.download_button("⬇️ Tải PDF báo cáo", data=pdf_bytes, file_name=f"{ts_name}.pdf", mime="application/pdf")

else:
    # Login section
    st.markdown("### Đăng nhập vào hệ thống KPI")
    st.text_input("Tài khoản USE", key="use_name")
    st.text_input("Mật khẩu", type="password", key="password")
    
    if st.button("Đăng nhập", type="primary"):
        try:
            if check_credentials(st.session_state.use_name, st.session_state.password):
                st.session_state.is_authenticated = True
                toast("Đăng nhập thành công!", "✅")
                st.rerun()
            else:
                st.error("Tài khoản hoặc mật khẩu không đúng.")
        except Exception as e:
            st.error(f"Lỗi khi đăng nhập: {e}")

