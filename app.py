# -*- coding: utf-8 -*-
"""
KPI App – Định Hóa (v3.30 chỉnh sửa)
- Form GHIM CỨNG (sticky) trên cùng
- Bỏ ô Tên đơn vị, Ghi chú, Ngưỡng dưới/trên
- Đưa Tháng/Năm lên hàng với Phương pháp đo
- Uploader CSV dời xuống cuối
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
                                  ast.USub,ast.Uadd,ast.Call,ast.Attribute,ast.Constant,ast.Compare,
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
    plan = parse_vn_number(st.session_state.get("plan_txt","")) if "plan_txt" in st.session_state else None
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
    users = ws.get_all_records(expected_headers=ws.row_values(1))
    return pd.DataFrame(users)

def login(users_df, username, password):
    if users_df is None or users_df.empty: return False, "Lỗi: Không có dữ liệu người dùng"
    users_df = users_df.apply(lambda x: x.astype(str).str.strip())
    # Lấy tên cột chuẩn
    user_col = next((c for c in ["USE (mã đăng nhập)", "Tài khoản (USE/username)", "Tài khoản", "Username", "USE", "User"] if c in users_df.columns), None)
    pass_col = next((c for c in ["Mật khẩu mặc định", "Password mặc định", "Password", "Mật khẩu"] if c in users_df.columns), None)

    if not user_col or not pass_col: return False, "Lỗi: Không tìm thấy cột 'Tài khoản' hoặc 'Mật khẩu'."

    user_row = users_df[(users_df[user_col].str.lower() == username.lower()) & (users_df[pass_col] == password)]
    if not user_row.empty:
        st.session_state["_username"] = username
        st.session_state["_is_logged_in"] = True
        return True, "Đăng nhập thành công!"
    else:
        st.session_state.pop("_is_logged_in", None)
        return False, "Sai tài khoản hoặc mật khẩu."

def logout():
    st.session_state.pop("_is_logged_in", None)
    st.session_state.pop("_username", None)
    st.rerun()

def get_current_user_info():
    users_df = load_users_df()
    user_col = next((c for c in ["USE (mã đăng nhập)", "Tài khoản (USE/username)", "Tài khoản", "Username", "USE", "User"] if c in users_df.columns), None)
    user_info = users_df[users_df[user_col].str.lower() == st.session_state["_username"].lower()].iloc[0]
    return user_info

def save_csv_to_drive(file_obj, folder_id):
    if gbuild is None:
        toast("Google Drive API chưa sẵn sàng. Không thể lưu file.", icon="⚠️")
        return None
    try:
        gclient, creds = st.session_state.get("_gs_pair", (None, None))
        if gclient is None:
            gclient, creds = get_gs_clients()
            st.session_state["_gs_pair"] = (gclient, creds)
        drive_service = gbuild("drive", "v3", credentials=creds)

        file_metadata = {
            "name": Path(file_obj.name).name,
            "parents": [folder_id],
            "mimeType": "text/csv",
        }
        media = MediaIoBaseUpload(io.BytesIO(file_obj.getvalue()), mimetype="text/csv", resumable=True)
        file = drive_service.files().create(body=file_metadata, media_body=media, fields="id").execute()
        return file.get("id")
    except HttpError as error:
        toast(f"Lỗi khi lưu vào Drive: {error}", icon="❌")
        return None
    except Exception as e:
        toast(f"Lỗi không xác định khi lưu Drive: {e}", icon="❌")
        return None

# ------------------- XỬ LÝ BẢNG DỮ LIỆU KPI -------------------
def load_kpi_data() -> pd.DataFrame:
    if "df_kpi_cache" in st.session_state:
        df = st.session_state["df_kpi_cache"]
        if not df.empty and "_last_kpi_load" in st.session_state and (datetime.now()-st.session_state["_last_kpi_load"]).total_seconds() < 10:
            return df
    try:
        sh = open_spreadsheet(st.session_state.get("spreadsheet_id",""))
        ws = sh.worksheet(st.session_state.get("kpi_sheet_name",""))
        df = df_from_ws(ws)
        df = normalize_columns(df)
        st.session_state["df_kpi_cache"] = df
        st.session_state["_last_kpi_load"] = datetime.now()
        toast("Đã tải dữ liệu thành công!", icon="✅")
        return df
    except Exception as e:
        st.error(f"Lỗi khi tải dữ liệu: {e}")
        return pd.DataFrame()

def save_to_gsheet(df: pd.DataFrame, sheet_name: str):
    try:
        sh = open_spreadsheet(st.session_state.get("spreadsheet_id",""))
        ws = sh.worksheet(sheet_name)
        # Cập nhật dữ liệu từ DataFrame vào sheet
        ws.clear()
        ws.update([df.columns.values.tolist()] + df.values.tolist())
        st.session_state["df_kpi_cache"] = df
        toast("Đã lưu dữ liệu thành công!", icon="✅")
    except Exception as e:
        st.error(f"Lỗi khi lưu dữ liệu: {e}")

# ------------------- HIỂN THỊ UI -------------------
def display_sticky_form(selected_row):
    st.subheader("📝 Nhập dữ liệu")

    if st.session_state.get("_is_logged_in"):
        user_info = get_current_user_info()
        st.markdown(f"""
        <div style='position: sticky; top: 0; background: #fff; z-index: 1; padding: 1rem 0; border-bottom: 1px solid #eee;'>
            <div style='display: flex; align-items: center; justify-content: space-between;'>
                <div style='display: flex; align-items: center;'>
                    <img src="{LOGO_URL}" style="width: 50px; height: 50px; border-radius: 50%; margin-right: 15px;">
                    <div>
                        <h5 style='margin: 0;'>Chào, {user_info.get('Tên đầy đủ', st.session_state["_username"])}</h5>
                        <small style='color: gray;'>Đăng nhập: {st.session_state["_username"]}</small>
                    </div>
                </div>
                <button onclick="window.parent.postMessage({{'st_action': 'logout'}}, '*')" style="background-color: #f44336; color: white; border: none; padding: 10px 20px; border-radius: 5px; cursor: pointer;">
                    Đăng xuất
                </button>
            </div>
        </div>
        """, unsafe_allow_html=True)
    else:
        st.markdown("<p>Vui lòng đăng nhập để bắt đầu.</p>", unsafe_allow_html=True)

    with st.form(key="kpi_form", clear_on_submit=False):
        row = selected_row if selected_row is not None else {}
        kpi_name = row.get("Tên chỉ tiêu (KPI)", "")
        unit = row.get("Đơn vị tính", "")
        plan = row.get("Kế hoạch", "")
        actual = row.get("Thực hiện", "")
        weight = row.get("Trọng số", "")
        method = row.get("Phương pháp đo kết quả", "")
        month = row.get("Tháng", "")
        year = row.get("Năm", "")

        st.markdown(f"**Chỉ tiêu:** <span style='color: teal; font-weight: bold;'>{kpi_name}</span>", unsafe_allow_html=True)
        st.markdown(f"**Đơn vị:** <span style='color: darkgreen;'>{unit}</span>", unsafe_allow_html=True)

        col1, col2 = st.columns(2)
        with col1:
            st.session_state["plan_txt"] = st.text_input("Kế hoạch", value=plan)
        with col2:
            st.session_state["actual_txt"] = st.text_input("Thực hiện", value=actual)

        score = compute_score_with_method(row)
        score_display = f"{score:,.2f}" if isinstance(score, (int,float)) else "N/A"
        st.markdown(f"**Trọng số:** {weight} | **Điểm KPI (tạm tính):** <span style='color: red; font-weight: bold;'>{score_display}</span>", unsafe_allow_html=True)
        
        col3, col4, col5 = st.columns(3)
        with col3:
            st.text_input("Phương pháp đo kết quả", value=method, key="method_input")
        with col4:
            st.text_input("Tháng", value=month, key="month_input")
        with col5:
            st.text_input("Năm", value=year, key="year_input")

        # Nút hành động
        col_submit, col_clear = st.columns([1,1])
        with col_submit:
            if st.form_submit_button("✅ Cập nhật"):
                if st.session_state.get("_selected_idx") is not None:
                    # Cập nhật DataFrame trong cache
                    df_cache = st.session_state["_csv_cache"]
                    idx = st.session_state["_selected_idx"]
                    
                    if "Điểm KPI" in df_cache.columns and score is not None:
                        df_cache.loc[idx, "Điểm KPI"] = score

                    df_cache.loc[idx, "Kế hoạch"] = parse_vn_number(st.session_state["plan_txt"])
                    df_cache.loc[idx, "Thực hiện"] = parse_vn_number(st.session_state["actual_txt"])
                    df_cache.loc[idx, "Trọng số"] = parse_vn_number(weight)
                    df_cache.loc[idx, "Phương pháp đo kết quả"] = st.session_state["method_input"]
                    df_cache.loc[idx, "Tháng"] = st.session_state["month_input"]
                    df_cache.loc[idx, "Năm"] = st.session_state["year_input"]
                    st.session_state["_csv_cache"] = df_cache
                    toast("Đã cập nhật dữ liệu vào bảng!")
                else:
                    toast("Vui lòng chọn một dòng để cập nhật.", icon="⚠️")
        
        with col_clear:
            if st.form_submit_button("❌ Hủy"):
                st.session_state.pop("_selected_idx", None)
                st.rerun()

def display_login_form():
    with st.form(key="login_form"):
        st.text_input("Tên tài khoản (USE)", key="username_input")
        st.text_input("Mật khẩu", type="password", key="password_input")
        if st.form_submit_button("Đăng nhập"):
            users_df = load_users_df()
            success, msg = login(users_df, st.session_state["username_input"], st.session_state["password_input"])
            if success:
                toast(msg, icon="✅")
                st.rerun()
            else:
                toast(msg, icon="❌")

def get_df_and_selected_row():
    # Tải dữ liệu chính
    df = st.session_state.get("_csv_cache")
    if df is None:
        df = load_kpi_data()
        st.session_state["_csv_cache"] = df.copy()
    else: # Dữ liệu đã có trong cache, kiểm tra cập nhật
        sig = df.to_json(orient='split', compression='infer')
        if sig != st.session_state.get("_csv_loaded_sig"):
            st.session_state["_csv_loaded_sig"] = sig
            st.session_state.pop("_selected_idx", None) # reset selected row

    # Hiển thị form và bảng dữ liệu
    selected_row = None
    if st.session_state.get("_selected_idx") is not None:
        try:
            selected_row = df.loc[st.session_state["_selected_idx"]]
        except KeyError:
            st.session_state.pop("_selected_idx", None)

    return df, selected_row

# ------------------- MAIN APP -------------------
def main():
    if not st.session_state.get("_is_logged_in"):
        display_login_form()
    else:
        df, selected_row = get_df_and_selected_row()
        
        # Phần giao diện chính
        display_sticky_form(selected_row)

        st.subheader("Bảng dữ liệu KPI")

        # Xử lý bảng dữ liệu
        df_show = df.copy()
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
                st.session_state["plan_txt"] = df_edit.loc[new_sel, "Kế hoạch"]
                st.session_state["actual_txt"] = df_edit.loc[new_sel, "Thực hiện"]
                st.session_state["method_input"] = df_edit.loc[new_sel, "Phương pháp đo kết quả"]
                st.session_state["month_input"] = df_edit.loc[new_sel, "Tháng"]
                st.session_state["year_input"] = df_edit.loc[new_sel, "Năm"]
                st.session_state.pop("_csv_loaded_sig", None)
            st.rerun()

        # Nút hành động
        st.subheader("Hành động")
        col_refresh, col_save, col_export = st.columns(3)
        with col_refresh:
            if st.button("🔄 Tải lại dữ liệu"):
                st.session_state.pop("df_kpi_cache", None)
                st.session_state.pop("_csv_cache", None)
                st.session_state.pop("_selected_idx", None)
                st.rerun()
        with col_save:
            if st.button("💾 Lưu vào Google Sheet"):
                save_to_gsheet(df, st.session_state.get("kpi_sheet_name"))
        with col_export:
            csv_data = df.to_csv(index=False).encode("utf-8")
            st.download_button(
                label="📥 Tải xuống CSV",
                data=csv_data,
                file_name="kpi_data.csv",
                mime="text/csv",
            )
            
        # CSV UPLOADER
        st.subheader("⬇️ Nhập CSV vào KPI")
        uploaded = st.file_uploader("Chọn file CSV", type=["csv"], label_visibility="collapsed")
        if uploaded:
            try:
                df_csv = pd.read_csv(uploaded)
                st.dataframe(df_csv, use_container_width=True)
                st.session_state["_csv_cache"] = df_csv
                toast("Đã nhập dữ liệu từ CSV.", icon="✅")
            except Exception as e:
                st.error(f"Không đọc được CSV: {e}")

if __name__ == "__main__":
    main()
