# -*- coding: utf-8 -*-
"""
KPI App (bản đã bổ sung mặc định Google Sheet ID)
- Dùng GOOGLE_SHEET_ID mặc định nếu admin chưa cấu hình.
- Thông báo lỗi chi tiết hơn khi không đọc được tab USE.
"""
import re
import io
from datetime import datetime
import streamlit as st
import pandas as pd
import gspread
from google.oauth2.service_account import Credentials

GOOGLE_SHEET_ID_DEFAULT = "1hpDfYwueLWC2UwaH9e9qbx4XNvOf-2S4"

st.set_page_config(page_title="KPI - Đội quản lý Điện lực khu vực Định Hóa", layout="wide")
APP_TITLE = "📊 KPI - Đội quản lý Điện lực khu vực Định Hóa"

ADMIN_ACCOUNTS = {r"pctn\admin", r"npc\longph"}

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
            svc["private_key"] = svc["private_key"].replace("\n", "\n").replace("\r\n","\n").replace("\r","\n")
            svc["private_key"] = svc["private_key"].replace("\\n","\n")
        scopes = ["https://www.googleapis.com/auth/spreadsheets","https://www.googleapis.com/auth/drive"]
        creds = Credentials.from_service_account_info(svc, scopes=scopes)
        return gspread.authorize(creds)
    except Exception as e:
        st.session_state["_gs_error"] = f"SECRETS_ERROR: {e}"
        return None

def open_spreadsheet(sid_or_url: str):
    sid = extract_sheet_id(sid_or_url or GOOGLE_SHEET_ID_DEFAULT)
    if not sid:
        raise ValueError("missing_sid")
    client = st.session_state.get("_gs_client") or get_gs_client()
    st.session_state["_gs_client"] = client
    if client is None:
        raise RuntimeError("no_client")
    try:
        return client.open_by_key(sid)
    except Exception as e:
        st.session_state["_gs_error"] = f"OPEN_ERROR: {e}"
        raise

def df_from_ws(ws) -> pd.DataFrame:
    records = ws.get_all_records(expected_headers=ws.row_values(1))
    return pd.DataFrame(records)

def normalize_columns(df: pd.DataFrame) -> pd.DataFrame:
    if df is None or df.empty:
        return df
    alias = {
        "USE (mã đăng nhập)": ["USE (mã đăng nhập)", r"Tài khoản (USE\\\username)", r"Tài khoản (USE\\\username)",
                               "Tài khoản (USE/username)", "Tài khoản", "Username"],
        "Mật khẩu mặc định": ["Mật khẩu mặc định","Password mặc định","Password","Mật khẩu","Mat khau mac dinh"],
    }
    cols_lower = {c.strip().lower(): c for c in df.columns}
    rename_map = {}
    for std, cands in alias.items():
        if std in df.columns: continue
        for c in cands:
            key = c.strip().lower()
            if key in cols_lower:
                rename_map[cols_lower[key]] = std
                break
    if rename_map:
        df = df.rename(columns=rename_map)
    return df

def load_users(spreadsheet_id_or_url: str = "") -> pd.DataFrame:
    # Ưu tiên Google Sheets (dùng ID mặc định nếu chưa cấu hình)
    sid_final = extract_sheet_id(spreadsheet_id_or_url) or GOOGLE_SHEET_ID_DEFAULT
    client = get_gs_client()
    if client is not None and sid_final:
        try:
            sh = client.open_by_key(sid_final)
            try:
                ws = sh.worksheet("USE")
            except Exception:
                ws = None
                for w in sh.worksheets():
                    hdr = [h.strip() for h in w.row_values(1)]
                    if (("USE (mã đăng nhập)" in hdr) or (r"Tài khoản (USE\\\username)" in hdr) or ("Tài khoản (USE/username)" in hdr) or ("Tài khoản" in hdr) or ("Username" in hdr)) and ("Mật khẩu mặc định" in hdr or "Password" in hdr or "Mật khẩu" in hdr):
                        ws = w; break
                if ws is None:
                    st.session_state["_gs_error"] = "NO_USE_TAB"
                    raise RuntimeError("NO_USE_TAB")
            df = df_from_ws(ws)
            return df
        except Exception as e:
            st.session_state["_gs_error"] = st.session_state.get("_gs_error", f"READ_ERROR: {e}")
            # fallthrough to local
    # Fallback: USE.xlsx
    try:
        df_local = pd.read_excel("USE.xlsx", sheet_name="USE")
        return df_local
    except Exception as e:
        st.session_state["_gs_error"] = st.session_state.get("_gs_error", f"LOCAL_READ_ERROR: {e}")
        return pd.DataFrame()

# =============== ĐĂNG NHẬP (BẮT BUỘC) ===============
with st.sidebar:
    st.header("🔒 Đăng nhập")
    use_input = st.text_input("USE (vd: PCTN\KVDHA)")
    pwd_input = st.text_input("Mật khẩu", type="password")
    colA, colB = st.columns(2)
    with colA:
        login_clicked = st.button("Đăng nhập", use_container_width=True, type="primary")
    with colB:
        forgot_clicked = st.button("Quên mật khẩu", use_container_width=True)

    if "_user" in st.session_state and is_admin(st.session_state["_user"]):
        st.markdown("---")
        st.header("⚙️ Quản trị (Admin)")
        sid_val = st.text_input("Google Sheet ID/URL", value=st.session_state.get("spreadsheet_id",""))
        st.session_state["spreadsheet_id"] = sid_val

def check_credentials(df: pd.DataFrame, use_input: str, pwd_input: str) -> bool:
    if df is None or df.empty:
        st.error("Chưa tải được danh sách người dùng (USE).")
        # Hiển thị gợi ý nguyên nhân cho Admin (nếu có)
        if "_user" in st.session_state and is_admin(st.session_state["_user"]):
            st.caption(f"Chi tiết: {st.session_state.get('_gs_error','(không có)')}")
        return False
    df = normalize_columns(df)
    # Xác định cột
    col_use = next((c for c in df.columns if c.strip().lower() in [
        "tài khoản (use\\username)".lower(),
        "tài khoản (use/username)".lower(),
        "use (mã đăng nhập)".lower(),
        "tài khoản".lower(), "username".lower()
    ]), None)
    col_pw = next((c for c in df.columns if c.strip().lower() in [
        "mật khẩu mặc định".lower(), "password mặc định".lower(),"password".lower(),"mật khẩu".lower()
    ]), None)
    if not col_use or not col_pw:
        st.error("Thiếu cột USE hoặc Mật khẩu trong bảng USE.")
        return False
    u = (use_input or "").strip(); p = (pwd_input or "").strip()
    row = df.loc[df[col_use].astype(str).str.strip() == u]
    if row.empty or str(row.iloc[0][col_pw]).strip() != p:
        st.error("USE hoặc mật khẩu không đúng")
        return False
    return True

if login_clicked:
    df_users = load_users(spreadsheet_id_or_url=st.session_state.get("spreadsheet_id",""))
    if check_credentials(df_users, use_input, pwd_input):
        st.session_state["_user"] = use_input
        toast(f"Đăng nhập thành công: {use_input}", "✅")

if forgot_clicked:
    u = (use_input or "").strip()
    if not u:
        toast("Nhập USE trước khi bấm 'Quên mật khẩu'.", "❗")
    else:
        toast(f"Đã gửi yêu cầu cấp lại mật khẩu cho {u} tới quản trị.", "✅")

st.title(APP_TITLE)

# Chưa đăng nhập -> dừng hẳn
if "_user" not in st.session_state:
    st.stop()

st.success(f"Đã đăng nhập: **{st.session_state['_user']}**")
st.write("Google Sheet đang dùng:", st.session_state.get("spreadsheet_id", GOOGLE_SHEET_ID_DEFAULT) or GOOGLE_SHEET_ID_DEFAULT)
st.caption("Admin có thể thay đổi ID/URL ở sidebar sau khi đăng nhập (nếu có quyền).")
