# -*- coding: utf-8 -*-
"""
KPI Login Core (robust USE loader)
- Hardcodes default Google Sheet ID from anh Long.
- Finds worksheet by name ~= 'USE' (case-insensitive) OR by headers.
- Gives clear diagnostics for admins.
"""
import re
import streamlit as st
import pandas as pd
import gspread
from google.oauth2.service_account import Credentials

# ========= CONFIG =========
GOOGLE_SHEET_ID_DEFAULT = "1nXFKJrn8oHwQgUzv5QYihoazYRhhS1PeN-xyo7Er2iM"
ADMIN_ACCOUNTS = {r"pctn\\admin", r"npc\\longph"}

def is_admin(username: str) -> bool:
    return bool(username) and username.strip().lower() in ADMIN_ACCOUNTS

def toast(msg, icon="ℹ️"):
    try:
        st.toast(msg, icon=icon)
    except Exception:
        pass

def extract_sheet_id(text: str) -> str:
    if not text:
        return ""
    text = text.strip()
    m = re.search(r"/d/([a-zA-Z0-9-_]+)", text)
    return m.group(1) if m else text

def get_gs_client():
    try:
        svc = dict(st.secrets["gdrive_service_account"])
        if "private_key" in svc:
            # normalize newlines for Streamlit Cloud
            svc["private_key"] = svc["private_key"].replace("\\r\\n", "\\n").replace("\\r", "\\n").replace("\\\\n", "\\n")
        scopes = ["https://www.googleapis.com/auth/spreadsheets", "https://www.googleapis.com/auth/drive"]
        creds = Credentials.from_service_account_info(svc, scopes=scopes)
        return gspread.authorize(creds)
    except Exception as e:
        st.session_state["_gs_error"] = f"SECRETS_ERROR: {e}"
        return None

def open_spreadsheet(sid_or_url: str):
    sid = extract_sheet_id(sid_or_url or GOOGLE_SHEET_ID_DEFAULT) or GOOGLE_SHEET_ID_DEFAULT
    client = st.session_state.get("_gs_client") or get_gs_client()
    st.session_state["_gs_client"] = client
    if client is None:
        raise RuntimeError("no_client")
    return client.open_by_key(sid)

def df_from_ws(ws) -> pd.DataFrame:
    records = ws.get_all_records(expected_headers=ws.row_values(1))
    return pd.DataFrame(records)

# ---- Robust worksheet finder ----
def _normalize(s: str) -> str:
    return re.sub(r"\\s+", "", (s or "").strip().lower())

def find_use_worksheet(sh):
    # 1) by name similar to 'use'
    for ws in sh.worksheets():
        name = _normalize(ws.title)
        if name in {"use", "users"} or name.endswith("use"):
            return ws
    # 2) by headers
    for ws in sh.worksheets():
        try:
            headers = [h.strip() for h in ws.row_values(1)]
        except Exception:
            continue
        hdr = set(headers)
        need = [
            {"USE (mã đăng nhập)", "Mật khẩu mặc định"},
            {r"Tài khoản (USE\\username)", "Mật khẩu mặc định"},
            {r"Tài khoản (USE\username)", "Mật khẩu mặc định"},
            {"Tài khoản (USE/username)", "Mật khẩu mặc định"},
        ]
        for n in need:
            if n.issubset(hdr):
                return ws
    raise gspread.exceptions.WorksheetNotFound("NO_USE_TAB: Không tìm thấy tab có cột USE/mật khẩu.")

def normalize_columns(df: pd.DataFrame) -> pd.DataFrame:
    if df is None or df.empty:
        return df
    alias = {
        "USE (mã đăng nhập)": [
            "USE (mã đăng nhập)",
            r"Tài khoản (USE\\username)",
            r"Tài khoản (USE\username)",
            "Tài khoản (USE/username)",
            "Tài khoản",
            "Username",
        ],
        "Mật khẩu mặc định": [
            "Mật khẩu mặc định", "Password mặc định", "Password", "Mật khẩu", "Mat khau mac dinh"
        ],
    }
    cols_lower = {c.strip().lower(): c for c in df.columns}
    rename = {}
    for std, cands in alias.items():
        if std in df.columns:
            continue
        for c in cands:
            k = c.strip().lower()
            if k in cols_lower:
                rename[cols_lower[k]] = std
                break
    if rename:
        df = df.rename(columns=rename)
    return df

def load_users(spreadsheet_id_or_url: str = "") -> pd.DataFrame:
    sid = extract_sheet_id(spreadsheet_id_or_url) or GOOGLE_SHEET_ID_DEFAULT
    client = get_gs_client()
    if client is None:
        st.session_state["_gs_error"] = st.session_state.get("_gs_error", "SECRETS_ERROR")
        return pd.DataFrame()
    try:
        sh = client.open_by_key(sid)
        ws = find_use_worksheet(sh)
        df = df_from_ws(ws)
        return df
    except gspread.exceptions.APIError as e:
        st.session_state["_gs_error"] = f"API_ERROR: {e}"
        return pd.DataFrame()
    except gspread.exceptions.WorksheetNotFound as e:
        st.session_state["_gs_error"] = str(e)
        return pd.DataFrame()
    except Exception as e:
        st.session_state["_gs_error"] = f"OPEN_ERROR: {e}"
        return pd.DataFrame()

def check_credentials(df: pd.DataFrame, use_input: str, pwd_input: str) -> bool:
    if df is None or df.empty:
        st.error("Chưa tải được danh sách người dùng (USE).")
        if "_user" in st.session_state and is_admin(st.session_state["_user"]):
            st.caption(f"Chi tiết: {st.session_state.get('_gs_error','(không có)')}")
        return False
    df = normalize_columns(df)
    col_use = next((c for c in df.columns if c.strip().lower() in [
        "tài khoản (use\\username)".lower(),
        "tài khoản (use/username)".lower(),
        "use (mã đăng nhập)".lower(),
        "tài khoản".lower(), "username".lower()
    ]), None)
    col_pw = next((c for c in df.columns if c.strip().lower() in [
        "mật khẩu mặc định".lower(), "password mặc định".lower(), "password".lower(), "mật khẩu".lower()
    ]), None)
    if not col_use or not col_pw:
        st.error("Thiếu cột USE hoặc Mật khẩu trong bảng USE.")
        return False
    u = (use_input or "").strip()
    p = (pwd_input or "").strip()
    row = df.loc[df[col_use].astype(str).str.strip() == u]
    if row.empty or str(row.iloc[0][col_pw]).strip() != p:
        st.error("USE hoặc mật khẩu không đúng")
        return False
    return True

# ===== UI: Login only (for verification) =====
st.set_page_config(layout="wide")
st.title("🔒 Kiểm tra kết nối USE")

with st.sidebar:
    st.header("Đăng nhập")
    use_input = st.text_input("USE (vd: PCTN\\KVDHA)")
    pwd_input = st.text_input("Mật khẩu", type="password")
    login_clicked = st.button("Đăng nhập", use_container_width=True, type="primary")

if login_clicked:
    df_users = load_users(st.session_state.get("spreadsheet_id", ""))
    if check_credentials(df_users, use_input, pwd_input):
        st.success(f"Đăng nhập thành công: {use_input}")
        st.session_state["_user"] = use_input

# Show diagnosis for admin even before login for debugging
if is_admin(use_input):
    st.caption(f"(Diag) GOOGLE_SHEET_ID_DEFAULT = {GOOGLE_SHEET_ID_DEFAULT}")
    st.caption(f"(Diag) _gs_error = {st.session_state.get('_gs_error','(none)')}")
