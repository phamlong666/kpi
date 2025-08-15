# -*- coding: utf-8 -*-
"""
KPI App – Định Hóa (bản đã fix theo yêu cầu)
1) BẮT BUỘC ĐĂNG NHẬP trước khi vào giao diện làm việc (gating cứng).
2) Có nút "Đăng xuất".
3) Có nút "Quên mật khẩu": tạo mật khẩu tạm và cập nhật trực tiếp vào tab USE.
4) Có mục "Thay đổi mật khẩu": kiểm tra mật khẩu cũ, cập nhật mật khẩu mới vào tab USE.
   (Yêu cầu service account có quyền Editor trên Google Sheet.)

- Đọc người dùng từ tab 'USE' của Google Sheet (hoặc fallback file USE.xlsx để ĐĂNG NHẬP CHỈ ĐỌC).
- ID sheet mặc định: GOOGLE_SHEET_ID_DEFAULT (admin có thể đổi trong sidebar sau khi đăng nhập).
"""
import re
import io
from datetime import datetime
import random
import string
import streamlit as st
import pandas as pd
import gspread
from google.oauth2.service_account import Credentials

# ========== CẤU HÌNH CHUNG ==========
st.set_page_config(page_title="KPI – Định Hóa", layout="wide")
APP_TITLE = "📊 KPI – Đội quản lý Điện lực khu vực Định Hóa"
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
    if not text: return ""
    text = text.strip()
    m = re.search(r"/d/([a-zA-Z0-9-_]+)", text)
    return m.group(1) if m else text

def get_gs_client():
    """Khởi tạo client gspread từ st.secrets (nếu có)."""
    try:
        svc = dict(st.secrets["gdrive_service_account"])
        if "private_key" in svc:
            svc["private_key"] = (
                svc["private_key"]
                .replace("\\r\\n", "\\n")
                .replace("\\r", "\\n")
                .replace("\\\\n", "\\n")
            )
        scopes = ["https://www.googleapis.com/auth/spreadsheets",
                  "https://www.googleapis.com/auth/drive"]
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

# ---- alias cột ----
ALIAS = {
    "USE (mã đăng nhập)": [
        "USE (mã đăng nhập)",
        r"Tài khoản (USE\\username)",  # phải escape \\
        "Tài khoản (USE/username)",
        "Tài khoản", "Username",
    ],
    "Mật khẩu mặc định": [
        "Mật khẩu mặc định","Password mặc định","Password","Mật khẩu","Mat khau mac dinh"
    ],
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
    return df

# ---- tìm worksheet USE & vị trí cột để ghi cập nhật ----
def find_use_ws_and_cols(sh):
    """Trả về (ws, idx_col_use, idx_col_pwd, headers). Chỉ tìm trong Google Sheet (không áp dụng cho USE.xlsx)."""
    # Ưu tiên tên tab 'USE'
    try:
        ws = sh.worksheet("USE")
    except Exception:
        ws = None
        for w in sh.worksheets():
            try:
                headers = [h.strip() for h in w.row_values(1)]
            except Exception:
                continue
            if (("USE (mã đăng nhập)" in headers) or ("Tài khoản (USE\\username)" in headers) or ("Tài khoản" in headers) or ("Username" in headers)) \
               and ("Mật khẩu mặc định" in headers or "Password" in headers or "Mật khẩu" in headers):
                ws = w; break
        if ws is None:
            raise gspread.exceptions.WorksheetNotFound("NO_USE_TAB")
    headers = [h.strip() for h in ws.row_values(1)]
    # xác định cột
    def find_idx(names):
        for name in names:
            if name in headers: return headers.index(name)+1
        return None
    idx_use = find_idx(["USE (mã đăng nhập)", "Tài khoản (USE\\username)", "Tài khoản", "Username"])
    idx_pwd = find_idx(["Mật khẩu mặc định","Password","Mật khẩu"])
    if not idx_use or not idx_pwd:
        raise RuntimeError("MISSING_USE_OR_PASS_COL")
    return ws, idx_use, idx_pwd, headers

# ---- tải users để đăng nhập ----
def load_users(spreadsheet_id_or_url: str = "") -> pd.DataFrame:
    sid = extract_sheet_id(spreadsheet_id_or_url) or GOOGLE_SHEET_ID_DEFAULT
    client = get_gs_client()
    if client is not None and sid:
        try:
            sh = client.open_by_key(sid)
            ws, _, _, _ = find_use_ws_and_cols(sh)
            return df_from_ws(ws)
        except Exception as e:
            st.session_state["_gs_error"] = f"OPEN_ERROR: {e}"
            # fallthrough
    # Fallback đọc file cục bộ để cho phép đăng nhập cơ bản (KHÔNG ghi được)
    try:
        return pd.read_excel("USE.xlsx", sheet_name="USE")
    except Exception:
        return pd.DataFrame()

def check_credentials(df: pd.DataFrame, use_input: str, pwd_input: str) -> bool:
    if df is None or df.empty:
        st.error("Chưa tải được danh sách người dùng (USE).")
        return False
    df = normalize_columns(df)
    col_use = next((c for c in df.columns if c.strip().lower() in [
        "tài khoản (use\\username)".lower(), "tài khoản".lower(), "username".lower(), "use (mã đăng nhập)".lower()
    ]), None)
    col_pw = next((c for c in df.columns if c.strip().lower() in [
        "mật khẩu mặc định".lower(),"password mặc định".lower(),"password".lower(),"mật khẩu".lower()
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

# ---- cập nhật mật khẩu trên Google Sheet ----
def update_password_on_sheet(user_use: str, new_password: str, spreadsheet_id_or_url: str = "") -> bool:
    """Trả True nếu cập nhật thành công trên Google Sheet."""
    try:
        sh = open_spreadsheet(spreadsheet_id_or_url or GOOGLE_SHEET_ID_DEFAULT)
        ws, idx_use, idx_pwd, headers = find_use_ws_and_cols(sh)
        # Tải tất cả records để xác định dòng
        recs = ws.get_all_records(expected_headers=ws.row_values(1))
        df = pd.DataFrame(recs)
        df = normalize_columns(df)
        # Xác định tên cột chuẩn
        col_use = next((c for c in df.columns if c.strip().lower() in [
            "tài khoản (use\\username)".lower(),"tài khoản".lower(),"username".lower(),"use (mã đăng nhập)".lower()
        ]), None)
        if not col_use:
            raise RuntimeError("MISSING_USE_COL")
        # Tìm dòng (cộng 2 vì header ở row 1)
        mask = df[col_use].astype(str).str.strip() == str(user_use).strip()
        if not mask.any():
            return False
        row_idx = mask.idxmax()  # index trong df
        row_number = int(df.index.get_loc(row_idx)) + 2  # +2: header + base-1
        # Update cell mật khẩu
        ws.update_cell(row_number, idx_pwd, new_password)
        return True
    except Exception as e:
        st.session_state["_pwd_error"] = str(e)
        return False

def generate_temp_password(n=8) -> str:
    chars = string.ascii_letters + string.digits
    return "".join(random.choice(chars) for _ in range(n))

# ========== SIDEBAR: ĐĂNG NHẬP / QUÊN MK / ADMIN ==========
with st.sidebar:
    st.header("🔒 Đăng nhập")
    use_input = st.text_input("USE (vd: PCTN\\KVDHA)")
    pwd_input = st.text_input("Mật khẩu", type="password")
    c1, c2, c3 = st.columns([1,1,1])
    with c1:
        login_clicked = st.button("Đăng nhập", use_container_width=True, type="primary")
    with c2:
        logout_clicked = st.button("Đăng xuất", use_container_width=True)
    with c3:
        forgot_clicked = st.button("Quên mật khẩu", use_container_width=True)

    # Chỉ hiển thị khu quản trị sau khi ĐĂNG NHẬP và là ADMIN
    if "_user" in st.session_state and is_admin(st.session_state["_user"]):
        st.markdown("---")
        st.header("⚙️ Quản trị (Admin)")
        sid_val = st.text_input("Google Sheet ID/URL", value=st.session_state.get("spreadsheet_id",""))
        st.session_state["spreadsheet_id"] = sid_val
        kpi_sheet_name = st.text_input("Tên sheet KPI", value=st.session_state.get("kpi_sheet_name","KPI"))
        st.session_state["kpi_sheet_name"] = kpi_sheet_name

        with st.expander("🔐 Thay đổi mật khẩu (Admin hoặc chính chủ)"):
            target_use = st.text_input("USE cần đổi", value=st.session_state.get("_user",""))
            old_pw = st.text_input("Mật khẩu cũ (để an toàn)", type="password")
            new_pw = st.text_input("Mật khẩu mới", type="password")
            new_pw2 = st.text_input("Nhập lại mật khẩu mới", type="password")
            change_clicked = st.button("Cập nhật mật khẩu", type="primary", use_container_width=True)

            if change_clicked:
                # Kiểm tra đúng mật khẩu cũ nếu là chính chủ; Admin có thể bỏ qua
                ok_to_change = False
                df_users = load_users(st.session_state.get("spreadsheet_id",""))
                if is_admin(st.session_state.get("_user","")) and target_use:
                    ok_to_change = True
                else:
                    # chính chủ
                    if check_credentials(df_users, target_use, old_pw):
                        ok_to_change = True
                if not ok_to_change:
                    st.error("Không hợp lệ: sai mật khẩu cũ hoặc thiếu thông tin.")
                else:
                    if not new_pw or new_pw != new_pw2:
                        st.error("Mật khẩu mới không khớp.")
                    else:
                        if update_password_on_sheet(target_use, new_pw, st.session_state.get("spreadsheet_id","")):
                            toast("Đã cập nhật mật khẩu mới.", "✅")
                        else:
                            st.error("Cập nhật thất bại. Kiểm tra quyền Editor cho service account.")

# Hành vi nút đăng nhập/đăng xuất/ quên mật khẩu
if login_clicked:
    df_users = load_users(st.session_state.get("spreadsheet_id",""))
    if check_credentials(df_users, use_input, pwd_input):
        st.session_state["_user"] = use_input
        toast(f"Đăng nhập thành công: {use_input}", "✅")

if logout_clicked:
    st.session_state.pop("_user", None)
    toast("Đã đăng xuất.", "✅")

if forgot_clicked:
    u = (use_input or "").strip()
    if not u:
        toast("Nhập USE trước khi bấm 'Quên mật khẩu'.", "❗")
    else:
        temp_pw = generate_temp_password(8)
        if update_password_on_sheet(u, temp_pw, st.session_state.get("spreadsheet_id","")):
            st.info("Đã cấp mật khẩu tạm. Vui lòng đăng nhập lại và đổi mật khẩu ngay trong mục Quản trị.")
            toast(f"Mật khẩu tạm cho {u}: {temp_pw}", "✅")
        else:
            st.error("Không cập nhật được mật khẩu tạm. Vui lòng liên hệ quản trị.")

# ========== GATING CỨNG: CHƯA ĐĂNG NHẬP -> DỪNG APP ==========
st.title(APP_TITLE)
if "_user" not in st.session_state:
    st.stop()  # Không hiển thị BẤT CỨ giao diện nghiệp vụ nào bên dưới

# ========== (Ví dụ) GIAO DIỆN NGHIỆP VỤ SAU KHI ĐĂNG NHẬP ==========
# Anh có thể giữ phần KPI đầy đủ ở đây (bảng KPI, nhập CSV, ...).
# Để gọn bản fix theo yêu cầu đăng nhập/mật khẩu, em tạm để placeholder:
st.success(f"Đang đăng nhập: **{st.session_state['_user']}**")
st.caption("Các tab KPI sẽ hiển thị tại đây (đã được gate sau đăng nhập).")
