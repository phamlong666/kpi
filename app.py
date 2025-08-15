# -*- coding: utf-8 -*-
"""
KPI App - Định Hóa (Login + KPI Suite)
- Đăng nhập từ Google Sheet tab USE (hoặc fallback USE.xlsx).
- Sau khi đăng nhập: các tab KPI (Bảng KPI, Nhập CSV, Quản trị).
- Tự động nhận dạng cột tương đương (alias) theo file "app - Copy.py".
- Ghi/đọc KPI tại worksheet "KPI" (có thể đổi trong sidebar Admin).
"""
import re
from datetime import datetime
import io
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
    try:
        svc = dict(st.secrets["gdrive_service_account"])
        if "private_key" in svc:
            svc["private_key"] = (
                svc["private_key"]
                .replace("\\r\\n", "\\n")
                .replace("\\r", "\\n")
                .replace("\\\\n", "\\n")
            )
        scopes = ["https://www.googleapis.com/auth/spreadsheets","https://www.googleapis.com/auth/drive"]
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

def _normalize_name(s: str) -> str:
    return re.sub(r"\\s+", " ", (s or "").strip())

# ========== ALIAS CỘT ==========
ALIAS = {
    "USE (mã đăng nhập)": [
        "USE (mã đăng nhập)",
        r"Tài khoản (USE\\username)",  # đã escape
        "Tài khoản (USE/username)",
        "Tài khoản", "Username",
    ],
    "Mật khẩu mặc định": [
        "Mật khẩu mặc định","Password mặc định","Password","Mật khẩu","Mat khau mac dinh"
    ],
    # KPI
    "Tên chỉ tiêu (KPI)": ["Tên chỉ tiêu (KPI)","Ten chi tieu (KPI)","Tên KPI","Ten KPI","Chỉ tiêu","Chi tieu"],
    "Đơn vị tính": ["Đơn vị tính","Don vi tinh","Unit"],
    "Kế hoạch": ["Kế hoạch","Ke hoach","Plan","Target"],
    "Thực hiện": ["Thực hiện","Thuc hien","Actual","Thực hiện (tháng)"],
    "Trọng số": ["Trọng số","Trong so","Weight"],
    "Bộ phận/người phụ trách": ["Bộ phận/người phụ trách","Bo phan/nguoi phu trach","Phụ trách","Nguoi phu trach"],
    "Tháng": ["Tháng","Thang","Month"],
    "Năm": ["Năm","Nam","Year"],
    "Điểm KPI": ["Điểm KPI","Diem KPI","Score","Diem"],
    "Ghi chú": ["Ghi chú","Ghi chu","Notes"],
    "Tên đơn vị": ["Tên đơn vị","Don vi","Ten don vi","Đơn vị"],
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

# ========== LOAD USERS ==========
def load_users(spreadsheet_id_or_url: str = "") -> pd.DataFrame:
    sid = extract_sheet_id(spreadsheet_id_or_url) or GOOGLE_SHEET_ID_DEFAULT
    client = get_gs_client()
    if client is None:
        st.session_state["_gs_error"] = st.session_state.get("_gs_error","SECRETS_ERROR")
        return pd.DataFrame()
    try:
        sh = client.open_by_key(sid)
        # ưu tiên tab 'USE'; nếu không có, quét theo headers
        try:
            ws = sh.worksheet("USE")
        except Exception:
            ws = None
            for w in sh.worksheets():
                hdr = [h.strip() for h in w.row_values(1)]
                if (("USE (mã đăng nhập)" in hdr) or ("Tài khoản (USE\\username)" in hdr) or ("Tài khoản" in hdr) or ("Username" in hdr)) \
                   and ("Mật khẩu mặc định" in hdr or "Password" in hdr or "Mật khẩu" in hdr):
                    ws = w; break
            if ws is None:
                st.session_state["_gs_error"] = "NO_USE_TAB"
                return pd.DataFrame()
        return df_from_ws(ws)
    except Exception as e:
        st.session_state["_gs_error"] = f"OPEN_ERROR: {e}"
        return pd.DataFrame()

def check_credentials(df: pd.DataFrame, use_input: str, pwd_input: str) -> bool:
    if df is None or df.empty:
        st.error("Chưa tải được danh sách người dùng (USE).")
        return False
    df = normalize_columns(df)
    # xác định cột
    col_use = next((c for c in df.columns if c.strip().lower() in [
        "tài khoản (use\\username)".lower(),
        "tài khoản".lower(),"username".lower(),"use (mã đăng nhập)".lower()
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

# ========== KPI CORE ==========
KPI_COLS = ["Tên chỉ tiêu (KPI)","Đơn vị tính","Kế hoạch","Thực hiện","Trọng số","Bộ phận/người phụ trách","Tháng","Năm","Điểm KPI","Ghi chú","Tên đơn vị"]

def safe_float(x):
    try:
        s = str(x).replace(",",".")
        return float(s)
    except Exception:
        return None

def compute_score(row):
    plan = safe_float(row.get("Kế hoạch"))
    actual = safe_float(row.get("Thực hiện"))
    weight = safe_float(row.get("Trọng số")) or 0.0
    if plan in (None,0) or actual is None: return None
    ratio = max(min(actual/plan, 2.0), 0.0)
    w = weight/100.0 if weight and weight>1 else (weight or 0.0)
    return round(ratio*10*w, 2)

def read_kpi_from_sheet(sh, sheet_name: str):
    try:
        ws = sh.worksheet(sheet_name)
    except Exception:
        # tự tìm tab nào có đủ cột KPI tối thiểu
        ws = None
        for w in sh.worksheets():
            hdr = [h.strip() for h in w.row_values(1)]
            if ("Tên chỉ tiêu (KPI)" in hdr or "Kế hoạch" in hdr) and ("Thực hiện" in hdr or "Thực hiện (tháng)" in hdr):
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
    if "Điểm KPI" not in df.columns:
        df["Điểm KPI"] = df.apply(compute_score, axis=1)
    # bảo đảm thứ tự cột
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
        toast(f"Lưu KPI thất bại: {e}", "❌")
        return False

# ========== SIDEBAR (LOGIN + ADMIN) ==========
with st.sidebar:
    st.header("🔒 Đăng nhập")
    use_input = st.text_input("USE (vd: PCTN\\KVDHA)")
    pwd_input = st.text_input("Mật khẩu", type="password")
    colA,colB = st.columns(2)
    with colA:
        login_clicked = st.button("Đăng nhập", use_container_width=True, type="primary")
    with colB:
        forgot_clicked = st.button("Quên mật khẩu", use_container_width=True)

    if "_user" in st.session_state and is_admin(st.session_state["_user"]):
        st.markdown("---")
        st.header("⚙️ Quản trị (Admin)")
        sid_val = st.text_input("Google Sheet ID/URL", value=st.session_state.get("spreadsheet_id",""))
        st.session_state["spreadsheet_id"] = sid_val
        kpi_sheet_name = st.text_input("Tên sheet KPI", value=st.session_state.get("kpi_sheet_name","KPI"))
        st.session_state["kpi_sheet_name"] = kpi_sheet_name

if login_clicked:
    df_users = load_users(st.session_state.get("spreadsheet_id",""))
    if check_credentials(df_users, use_input, pwd_input):
        st.session_state["_user"] = use_input
        toast(f"Đăng nhập thành công: {use_input}", "✅")

if forgot_clicked:
    u = (use_input or "").strip()
    if not u:
        toast("Nhập USE trước khi bấm 'Quên mật khẩu'.", "❗")
    else:
        toast(f"Đã gửi yêu cầu cấp lại mật khẩu cho {u}", "✅")

st.title(APP_TITLE)

if "_user" not in st.session_state:
    st.stop()

# ========== MAIN TABS ==========
tab1, tab2, tab3 = st.tabs(["📋 Bảng KPI","⬆️ Nhập CSV vào KPI","⚙️ Quản trị"])

def get_sheet_and_name():
    sid_cfg = st.session_state.get("spreadsheet_id","") or GOOGLE_SHEET_ID_DEFAULT
    sheet_name = st.session_state.get("kpi_sheet_name","KPI")
    sh = open_spreadsheet(sid_cfg)
    return sh, sheet_name

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
        if m!="Tất cả": df_kpi = df_kpi[df_kpi["Tháng"].astype(str)==str(m)]
        if y!="Tất cả": df_kpi = df_kpi[df_kpi["Năm"].astype(str)==str(y)]

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
        # Chuẩn tên cột "Thực hiện (tháng)" → "Thực hiện"
        if "Thực hiện (tháng)" in df_csv.columns and "Thực hiện" not in df_csv.columns:
            df_csv = df_csv.rename(columns={"Thực hiện (tháng)":"Thực hiện"})
        if "Điểm KPI" not in df_csv.columns:
            df_csv["Điểm KPI"] = df_csv.apply(compute_score, axis=1)
        st.dataframe(df_csv, use_container_width=True, hide_index=True)

        colA,colB = st.columns(2)
        with colA:
            save_clicked = st.button("💾 Ghi vào sheet KPI", use_container_width=True, type="primary")
        with colB:
            st.write("")

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
