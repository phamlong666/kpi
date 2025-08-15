# -*- coding: utf-8 -*-
"""
KPI App – Định Hóa (FULL)
- Bắt buộc đăng nhập (gating cứng).
- Đăng xuất, Quên mật khẩu (gửi email đến phamlong666@gmail.com + reset trên Sheet), Thay đổi mật khẩu.
- KPI Tabs: Bảng KPI, Nhập CSV vào KPI, Quản trị.
"""

import re
import io
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from datetime import datetime
import random
import string
import streamlit as st
import pandas as pd
import gspread
from google.oauth2.service_account import Credentials

# ================= CẤU HÌNH =================
st.set_page_config(page_title="KPI – Định Hóa", layout="wide")
APP_TITLE = "📊 KPI – Đội quản lý Điện lực khu vực Định Hóa"
GOOGLE_SHEET_ID_DEFAULT = "1nXFKJrn8oHwQgUzv5QYihoazYRhhS1PeN-xyo7Er2iM"
ADMIN_ACCOUNTS = {r"pctn\\admin", r"npc\\longph"}
FORGOT_TARGET_EMAIL = "phamlong666@gmail.com"  # gửi cố định như yêu cầu

# ================= TIỆN ÍCH CHUNG =================
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
        r"Tài khoản (USE\\username)",
        "Tài khoản (USE/username)",
        "Tài khoản", "Username",
    ],
    "Mật khẩu mặc định": [
        "Mật khẩu mặc định","Password mặc định","Password","Mật khẩu","Mat khau mac dinh"
    ],
    # KPI
    "Tên chỉ tiêu (KPI)": ["Tên chỉ tiêu (KPI)","Tên KPI","Chỉ tiêu"],
    "Đơn vị tính": ["Đơn vị tính","Unit"],
    "Kế hoạch": ["Kế hoạch","Plan","Target"],
    "Thực hiện": ["Thực hiện","Thực hiện (tháng)","Actual (month)"],
    "Trọng số": ["Trọng số","Weight"],
    "Bộ phận/người phụ trách": ["Bộ phận/người phụ trách","Phụ trách"],
    "Tháng": ["Tháng","Month"],
    "Năm": ["Năm","Year"],
    "Điểm KPI": ["Điểm KPI","Score"],
    "Ghi chú": ["Ghi chú","Notes"],
    "Tên đơn vị": ["Tên đơn vị","Đơn vị"],
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
    """Trả về (ws, idx_col_use, idx_col_pwd, headers)."""
    # Ưu tiên tab 'USE'
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
    def find_idx(names):
        for name in names:
            if name in headers: return headers.index(name)+1
        return None
    idx_use = find_idx(["USE (mã đăng nhập)", "Tài khoản (USE\\username)", "Tài khoản", "Username"])
    idx_pwd = find_idx(["Mật khẩu mặc định","Password","Mật khẩu"])
    if not idx_use or not idx_pwd:
        raise RuntimeError("MISSING_USE_OR_PASS_COL")
    return ws, idx_use, idx_pwd, headers

# ---- load users để login ----
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
    # Fallback đọc file cục bộ
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
    try:
        sh = open_spreadsheet(spreadsheet_id_or_url or GOOGLE_SHEET_ID_DEFAULT)
        ws, idx_use, idx_pwd, headers = find_use_ws_and_cols(sh)
        # Tìm dòng cần update
        values = ws.col_values(idx_use)
        row_number = None
        for i, v in enumerate(values, start=1):
            if i == 1:  # header
                continue
            if str(v).strip() == str(user_use).strip():
                row_number = i
                break
        if not row_number:
            return False
        ws.update_cell(row_number, idx_pwd, new_password)
        return True
    except Exception as e:
        st.session_state["_pwd_error"] = str(e)
        return False

def generate_temp_password(n=8) -> str:
    chars = string.ascii_letters + string.digits
    return "".join(random.choice(chars) for _ in range(n))

# ---- gửi email báo mật khẩu tạm ----
def send_email_temp_password(target_email: str, use_name: str, temp_pw: str) -> bool:
    try:
        user = st.secrets["email"]["EMAIL_USER"]
        pwd  = st.secrets["email"]["EMAIL_PASS"]
    except Exception:
        # Không có cấu hình email -> coi như gửi "giả lập"
        toast(f"(Giả lập) Đã gửi mật khẩu tạm cho {use_name} đến {target_email}: {temp_pw}", "✅")
        return True

    try:
        msg = MIMEMultipart()
        msg["Subject"] = f"[KPI Định Hóa] Mật khẩu tạm cho {use_name}"
        msg["From"] = user
        msg["To"] = target_email
        body = f"""Chào anh/chị,

Hệ thống KPI đã tạo mật khẩu tạm cho tài khoản: {use_name}
Mật khẩu tạm: {temp_pw}

Vui lòng đăng nhập và đổi mật khẩu ngay trong mục Quản trị.
Trân trọng."""
        msg.attach(MIMEText(body, "plain", "utf-8"))

        with smtplib.SMTP_SSL("smtp.gmail.com", 465) as server:
            server.login(user, pwd)
            server.sendmail(user, [target_email], msg.as_string())
        toast("Đã gửi email mật khẩu tạm.", "✅")
        return True
    except Exception as e:
        st.error(f"Không gửi được email: {e}")
        return False

# ================= KPI CORE =================
KPI_COLS = ["Tên chỉ tiêu (KPI)","Đơn vị tính","Kế hoạch","Thực hiện","Trọng số","Bộ phận/người phụ trách","Tháng","Năm","Điểm KPI","Ghi chú","Tên đơn vị"]

def safe_float(x):
    try:
        s = str(x).replace(",",".")
        return float(s)
    except Exception:
        return None

def compute_score(row):
    plan = safe_float(row.get("Kế hoạch"))
    actual = safe_float(row.get("Thực hiện") or row.get("Thực hiện (tháng)"))
    weight = safe_float(row.get("Trọng số")) or 0.0
    if plan in (None,0) or actual is None: return None
    ratio = max(min(actual/plan, 2.0), 0.0)
    w = weight/100.0 if weight and weight>1 else (weight or 0.0)
    return round(ratio*10*w, 2)

def read_kpi_from_sheet(sh, sheet_name: str):
    try:
        ws = sh.worksheet(sheet_name)
    except Exception:
        # tìm tab phù hợp
        ws = None
        for w in sh.worksheets():
            hdr = [h.strip() for h in w.row_values(1)]
            if ("Kế hoạch" in hdr) and ("Thực hiện" in hdr or "Thực hiện (tháng)" in hdr):
                ws = w; break
        if ws is None: return pd.DataFrame()
    df = df_from_ws(ws)
    df = normalize_columns(df)
    if "Thực hiện (tháng)" in df.columns and "Thực hiện" not in df.columns:
        df = df.rename(columns={"Thực hiện (tháng)":"Thực hiện"})
    if "Điểm KPI" not in df.columns:
        df["Điểm KPI"] = df.apply(compute_score, axis=1)
    return df

def write_kpi_to_sheet(sh, sheet_name: str, df: pd.DataFrame):
    df = df.copy()
    df = normalize_columns(df)
    if "Thực hiện (tháng)" in df.columns and "Thực hiện" not in df.columns:
        df = df.rename(columns={"Thực hiện (tháng)":"Thực hiện"})
    if "Điểm KPI" not in df.columns:
        df["Điểm KPI"] = df.apply(compute_score, axis=1)
    cols = [c for c in KPI_COLS if c in df.columns] + [c for c in df.columns if c not in KPI_COLS]
    data = [cols] + df[cols].fillna("").astype(str).values.tolist()
    try:
        try:
            ws = sh.worksheet(sheet_name)
            ws.clear()
        except Exception:
            ws = sh.add_worksheet(title=sheet_name, rows=len(data)+10, cols=max(12,len(cols)))
        ws.update(data, value_input_option="USER_ENTERED")
        return True
    except Exception as e:
        st.error(f"Lưu KPI thất bại: {e}")
        return False

# ================= SIDEBAR: ĐĂNG NHẬP / QUÊN MK / ĐĂNG XUẤT / ADMIN =================
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
            old_pw = st.text_input("Mật khẩu cũ (đối với chính chủ)", type="password")
            new_pw = st.text_input("Mật khẩu mới", type="password")
            new_pw2 = st.text_input("Nhập lại mật khẩu mới", type="password")
            change_clicked = st.button("Cập nhật mật khẩu", type="primary", use_container_width=True)

            if change_clicked:
                ok_to_change = False
                df_users = load_users(st.session_state.get("spreadsheet_id",""))
                if is_admin(st.session_state.get("_user","")) and target_use:
                    ok_to_change = True
                else:
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

# Hành vi nút Đăng nhập / Đăng xuất / Quên mật khẩu
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
        ok_sheet = update_password_on_sheet(u, temp_pw, st.session_state.get("spreadsheet_id",""))
        ok_mail = send_email_temp_password(FORGOT_TARGET_EMAIL, u, temp_pw)
        if ok_sheet and ok_mail:
            st.info("Đã cấp mật khẩu tạm và gửi vào email quản trị. Vui lòng đăng nhập lại và đổi mật khẩu ngay.")
        elif ok_mail:
            st.warning("Đã gửi email mật khẩu tạm nhưng chưa cập nhật được trên sheet (kiểm tra quyền Editor).")
        else:
            st.error("Không thực hiện được yêu cầu quên mật khẩu.")

# ================= GATING CỨNG =================
st.title(APP_TITLE)
if "_user" not in st.session_state:
    st.stop()

# ================= KPI TABS =================
def get_sheet_and_name():
    sid_cfg = st.session_state.get("spreadsheet_id","") or GOOGLE_SHEET_ID_DEFAULT
    sheet_name = st.session_state.get("kpi_sheet_name","KPI")
    sh = open_spreadsheet(sid_cfg)
    return sh, sheet_name

tab1, tab2, tab3 = st.tabs(["📋 Bảng KPI","⬆️ Nhập CSV vào KPI","⚙️ Quản trị"])

with tab1:
    st.subheader("Bảng KPI")
    try:
        sh, sheet_name = get_sheet_and_name()
        df_kpi = read_kpi_from_sheet(sh, sheet_name)
    except Exception as e:
        st.error(f"Không đọc được KPI: {e}")
        df_kpi = pd.DataFrame()

    if not df_kpi.empty:
        months = ["Tất cả"] + sorted(df_kpi.get("Tháng", pd.Series(dtype=str)).dropna().astype(str).unique().tolist())
        years  = ["Tất cả"] + sorted(df_kpi.get("Năm", pd.Series(dtype=str)).dropna().astype(str).unique().tolist())
        colf1, colf2, colf3 = st.columns([1,1,2])
        with colf1:
            m = st.selectbox("Tháng", options=months, index=0)
        with colf2:
            y = st.selectbox("Năm", options=years, index=0)
        if m!="Tất cả" and "Tháng" in df_kpi.columns: df_kpi = df_kpi[df_kpi["Tháng"].astype(str)==str(m)]
        if y!="Tất cả" and "Năm" in df_kpi.columns:   df_kpi = df_kpi[df_kpi["Năm"].astype(str)==str(y)]

        if "Tên đơn vị" in df_kpi.columns:
            units = ["Tất cả"] + sorted(df_kpi["Tên đơn vị"].dropna().astype(str).unique().tolist())
            unit = st.selectbox("Đơn vị", options=units, index=0)
            if unit!="Tất cả": df_kpi = df_kpi[df_kpi["Tên đơn vị"].astype(str)==unit]

        if "Điểm KPI" in df_kpi.columns:
            if st.checkbox("Sắp xếp theo Điểm KPI (giảm dần)", True):
                df_kpi = df_kpi.sort_values(by="Điểm KPI", ascending=False)

        st.dataframe(df_kpi, use_container_width=True, hide_index=True)

        # Xuất Excel
        buf = io.BytesIO()
        with pd.ExcelWriter(buf, engine="xlsxwriter") as writer:
            df_kpi.to_excel(writer, sheet_name="KPI", index=False)
        st.download_button("⬇️ Tải Excel", data=buf.getvalue(), file_name="KPI_export.xlsx", mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")
    else:
        st.info("Chưa có dữ liệu KPI hoặc Admin chưa cấu hình sheet.")

with tab2:
    st.subheader("Nhập CSV vào KPI")
    st.caption("CSV gợi ý các cột: 'Tên chỉ tiêu (KPI)', 'Đơn vị tính', 'Kế hoạch', 'Thực hiện', 'Trọng số', 'Bộ phận/người phụ trách', 'Tháng', 'Năm', 'Ghi chú', 'Tên đơn vị'.")
    up = st.file_uploader("Tải file CSV", type=["csv"])
    if up is not None:
        try:
            df_csv = pd.read_csv(up)
        except Exception:
            up.seek(0)
            df_csv = pd.read_csv(up, encoding="utf-8-sig")
        df_csv = normalize_columns(df_csv)
        if "Thực hiện (tháng)" in df_csv.columns and "Thực hiện" not in df_csv.columns:
            df_csv = df_csv.rename(columns={"Thực hiện (tháng)":"Thực hiện"})
        if "Điểm KPI" not in df_csv.columns:
            df_csv["Điểm KPI"] = df_csv.apply(compute_score, axis=1)
        st.dataframe(df_csv, use_container_width=True, hide_index=True)

        colA,colB = st.columns(2)
        with colA:
            save_clicked = st.button("💾 Ghi vào sheet KPI", use_container_width=True, type="primary")
        if save_clicked:
            try:
                sh, sheet_name = get_sheet_and_name()
                ok = write_kpi_to_sheet(sh, sheet_name, df_csv)
                if ok: toast("Đã ghi dữ liệu CSV vào sheet KPI.", "✅")
            except Exception as e:
                st.error(f"Lưu thất bại: {e}")

with tab3:
    st.subheader("Thông tin")
    st.write("Người dùng:", st.session_state.get("_user"))
    st.write("Vai trò:", "Admin" if is_admin(st.session_state.get("_user","")) else "User")
    st.write("Google Sheet:", st.session_state.get("spreadsheet_id","(mặc định)") or GOOGLE_SHEET_ID_DEFAULT)
    st.write("Tên sheet KPI:", st.session_state.get("kpi_sheet_name","KPI"))
