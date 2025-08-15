# -*- coding: utf-8 -*-
import re
import io
import math
from datetime import datetime
import streamlit as st
import pandas as pd
import gspread
from google.oauth2.service_account import Credentials
import matplotlib.pyplot as plt

# ==================== CẤU HÌNH ====================
st.set_page_config(page_title="KPI - Đội quản lý Điện lực khu vực Định Hóa", layout="wide")
APP_TITLE = "📊 KPI - Đội quản lý Điện lực khu vực Định Hóa"

# ==================== TIỆN ÍCH ====================
def extract_sheet_id(text: str) -> str:
    if not text:
        return ""
    text = text.strip()
    m = re.search(r"/d/([a-zA-Z0-9-_]+)", text)
    return m.group(1) if m else text

ALIAS = {
    "USE (mã đăng nhập)": [
        "USE (mã đăng nhập)",
        r"Tài khoản (USE\\username)",
        r"Tài khoản (USE\username)",
        "Tài khoản (USE/username)",
        "Tài khoản","Username","Tài khoản USE",
    ],
    "Mật khẩu mặc định": [
        "Mật khẩu mặc định","Password mặc định","Mat khau mac dinh","Password","Mật khẩu",
    ],
    "Tên đơn vị": ["Tên đơn vị","Đơn vị","Don vi","Ten don vi","Đơn vị/Phòng ban"],
    "Chỉ tiêu": ["Chỉ tiêu","KPI","Chi tieu","Tên KPI"],
    "Kế hoạch": ["Kế hoạch","Plan","Target","Ke hoach"],
    "Thực hiện (tháng)": ["Thực hiện (tháng)","Thực hiện tháng","Thuc hien (thang)","Actual (month)"],
    "Thực hiện (lũy kế)": ["Thực hiện (lũy kế)","Thuc hien (luy ke)","Actual (YTD)","Lũy kế"],
    "Đơn vị tính": ["Đơn vị tính","Don vi tinh","Unit"],
    "Trọng số": ["Trọng số","Trong so","Weight"],
    "Ghi chú": ["Ghi chú","Ghi chu","Notes"],
    "Tháng": ["Tháng","Thang","Month"],
    "Năm": ["Năm","Nam","Year"],
}

def normalize_columns(df: pd.DataFrame) -> pd.DataFrame:
    if df is None or df.empty:
        return df
    cols_lower = {c.strip().lower(): c for c in df.columns}
    rename_map = {}
    for std, cands in ALIAS.items():
        if std in df.columns:
            continue
        for c in cands:
            key = c.strip().lower()
            if key in cols_lower:
                rename_map[cols_lower[key]] = std
                break
    if rename_map:
        df = df.rename(columns=rename_map)
    return df

def df_from_ws(ws) -> pd.DataFrame:
    records = ws.get_all_records(expected_headers=ws.row_values(1))
    return pd.DataFrame(records)

def get_gs_client():
    try:
        svc = dict(st.secrets["gdrive_service_account"])
        if "private_key" in svc:
            svc["private_key"] = svc["private_key"].replace("\\n", "\n")
        scopes = ["https://www.googleapis.com/auth/spreadsheets","https://www.googleapis.com/auth/drive"]
        creds = Credentials.from_service_account_info(svc, scopes=scopes)
        return gspread.authorize(creds)
    except Exception as e:
        st.session_state["_gs_error"] = str(e)
        return None

def open_spreadsheet(sid_or_url: str):
    sid = extract_sheet_id(sid_or_url)
    if not sid:
        raise ValueError("missing_sid")
    client = st.session_state.get("_gs_client") or get_gs_client()
    st.session_state["_gs_client"] = client
    if client is None:
        raise RuntimeError("no_client")
    return client.open_by_key(sid)

def find_use_worksheet(sh):
    try:
        return sh.worksheet("USE")
    except Exception:
        pass
    for ws in sh.worksheets():
        try:
            headers = [h.strip() for h in ws.row_values(1)]
            hdr = set(headers)
            need = [
                {"USE (mã đăng nhập)","Mật khẩu mặc định"},
                {r"Tài khoản (USE\\username)","Mật khẩu mặc định"},
                {r"Tài khoản (USE\username)","Mật khẩu mặc định"},
            ]
            if any(n.issubset(hdr) for n in need):
                return ws
        except Exception:
            continue
    raise gspread.exceptions.WorksheetNotFound("no_use")

def get_ws_by_name_or_guess(sh, prefer):
    for name in prefer:
        try:
            return sh.worksheet(name)
        except Exception:
            continue
    return None

def safe_float(x):
    try:
        s = str(x).strip().replace(",", ".")
        return float(s)
    except Exception:
        return None

def compute_kpi_score(row):
    plan = safe_float(row.get("Kế hoạch"))
    actual = safe_float(row.get("Thực hiện (tháng)"))
    weight = safe_float(row.get("Trọng số")) or 0.0
    if not plan or not actual:
        return None
    ratio = max(min(actual/plan, 2.0), 0.0)
    score10 = ratio * 10.0
    w = weight/100.0 if weight > 1 else weight
    return round(score10 * w, 2)

def prepare_kpi_df(df_raw: pd.DataFrame):
    if df_raw is None or df_raw.empty:
        return df_raw, []
    df = normalize_columns(df_raw.copy())
    if "Điểm" not in df.columns:
        df["Điểm"] = df.apply(compute_kpi_score, axis=1)
    cols = [c for c in ["Tên đơn vị","Chỉ tiêu","Đơn vị tính","Kế hoạch","Thực hiện (tháng)",
                        "Thực hiện (lũy kế)","Trọng số","Điểm","Tháng","Năm","Ghi chú"] if c in df.columns]
    return df, cols

def filter_by_time(df: pd.DataFrame, month_val, year_val):
    if df is None or df.empty:
        return df
    if (year_val not in (None,"","Tất cả")) and "Năm" in df.columns:
        df = df[df["Năm"].astype(str) == str(year_val)]
    if (month_val not in (None,"Tất cả")) and "Tháng" in df.columns:
        df = df[df["Tháng"].astype(str) == str(month_val)]
    return df

# ==================== SIDEBAR ====================
with st.sidebar:
    st.header("🔗 Kết nối dữ liệu")
    sid_input = st.text_input(
        "ID bảng tính",
        value=st.session_state.get("spreadsheet_id",""),
        placeholder="Dán URL hoặc ID Google Sheet (/d/<ID>/edit)",
        help="Có thể dán cả URL; hệ thống sẽ tự rút ID."
    )
    st.session_state["spreadsheet_id"] = sid_input
    st.text_input("Tên sheet KPI", key="kpi_sheet_name", value=st.session_state.get("kpi_sheet_name","KPI"))

    st.markdown("---")
    st.header("🔒 Đăng nhập")
    username = st.text_input("Tài khoản (USE\\username)", value=st.session_state.get("_username",""))
    password = st.text_input("Mật khẩu", type="password", value=st.session_state.get("_password",""))

    colA, colB = st.columns(2)
    with colA:
        login_clicked = st.button("Đăng nhập", use_container_width=True, type="primary")
    with colB:
        logout_clicked = st.button("Đăng xuất", use_container_width=True)

    st.markdown("---")
    sync_clicked = st.button("🌿 Đồng bộ Users từ sheet USE", use_container_width=True)

    st.markdown("---")
    st.subheader("Bộ lọc thời gian")
    months = ["Tất cả"] + [str(i) for i in range(1,13)]
    month_choice = st.selectbox("Tháng", options=months, index=0)
    year_choice = st.text_input("Năm", value=str(datetime.now().year))

# ==================== ĐĂNG NHẬP & ĐỒNG BỘ ====================
def toast(msg, icon="ℹ️"):
    try:
        st.toast(msg, icon=icon)
    except Exception:
        pass  # phòng trường hợp môi trường không hỗ trợ toast

login_msg = ""

def handle_login():
    global login_msg
    # Bắt buộc có SID trước khi login
    if not st.session_state.get("spreadsheet_id"):
        login_msg = "Vui lòng nhập ID/URL Google Sheet trước khi đăng nhập."
        toast(login_msg, "❗")
        return False
    try:
        sh = open_spreadsheet(st.session_state["spreadsheet_id"])
        ws = find_use_worksheet(sh)
        df = normalize_columns(df_from_ws(ws))
        for req in ["USE (mã đăng nhập)","Mật khẩu mặc định"]:
            if req not in df.columns:
                login_msg = f"Thiếu cột bắt buộc: {req}"
                toast(login_msg, "❗")
                return False
        u = (username or "").strip()
        p = (password or "").strip()
        row = df.loc[df["USE (mã đăng nhập)"].astype(str).str.strip() == u]
        if row.empty:
            login_msg = "Sai tài khoản hoặc chưa có trong danh sách."
            toast(login_msg, "❌")
            return False
        pass_ok = str(row["Mật khẩu mặc định"].iloc[0]).strip()
        if p and p == pass_ok:
            st.session_state["_user"] = u
            st.session_state["_username"] = u
            st.session_state["_password"] = p
            login_msg = "Đăng nhập thành công."
            toast(login_msg, "✅")
            return True
        else:
            login_msg = "Mật khẩu không đúng."
            toast(login_msg, "❌")
            return False
    except ValueError as ve:
        if str(ve) == "missing_sid":
            login_msg = "Vui lòng nhập ID/URL Google Sheet."
            toast(login_msg, "❗")
            return False
        login_msg = f"Lỗi đăng nhập."
        toast(login_msg, "❌")
        return False
    except Exception:
        login_msg = "Không thể đăng nhập. Kiểm tra quyền truy cập/ID."
        toast(login_msg, "❌")
        return False

def handle_logout():
    st.session_state.pop("_user", None)
    st.session_state["_password"] = ""
    st.session_state["_username"] = ""
    toast("Đã đăng xuất.", "✅")

def handle_sync_users():
    if "_user" not in st.session_state:
        toast("Vui lòng đăng nhập trước.", "❗")
        return
    try:
        sh = open_spreadsheet(st.session_state["spreadsheet_id"])
        ws = find_use_worksheet(sh)
        df = normalize_columns(df_from_ws(ws))
        toast(f"Đã đọc {len(df)} người dùng từ sheet USE.", "✅")
    except Exception:
        toast("Đồng bộ thất bại. Kiểm tra ID/quyền truy cập.", "❌")

if login_clicked:
    handle_login()
if logout_clicked:
    handle_logout()
if sync_clicked:
    handle_sync_users()

# ==================== MAIN ====================
st.title(APP_TITLE)

# Ẩn toàn bộ giao diện nghiệp vụ khi CHƯA đăng nhập
if "_user" not in st.session_state:
    st.caption("Vui lòng nhập ID/URL Google Sheet và đăng nhập để vào khu vực làm việc.")
    st.stop()

# --- Từ đây trở xuống chỉ hiển thị SAU khi đăng nhập ---

tab1, tab2, tab3 = st.tabs(["📋 Bảng KPI", "📈 Biểu đồ", "⚙️ Quản trị"])

def load_kpi_df():
    try:
        sh = open_spreadsheet(st.session_state["spreadsheet_id"])
        ws = get_ws_by_name_or_guess(sh, [st.session_state.get("kpi_sheet_name","KPI"), "KPI", "KPI_Data", "KPIs"])
        if ws is None:
            toast("Chưa tìm thấy sheet KPI. Kiểm tra tên sheet ở sidebar.", "❗")
            return pd.DataFrame(), []
        df = normalize_columns(df_from_ws(ws))
        df, cols = prepare_kpi_df(df)
        df = filter_by_time(df, month_choice, year_choice)
        return df, cols
    except Exception:
        toast("Lỗi khi đọc KPI. Kiểm tra ID/quyền truy cập.", "❌")
        return pd.DataFrame(), []

with tab1:
    st.subheader("Bảng KPI")
    df_kpi, show_cols = load_kpi_df()
    if not df_kpi.empty:
        if "Tên đơn vị" in df_kpi.columns:
            units = ["Tất cả"] + sorted(df_kpi["Tên đơn vị"].dropna().astype(str).unique().tolist())
            unit_sel = st.selectbox("Chọn đơn vị", options=units, index=0)
            if unit_sel != "Tất cả":
                df_kpi = df_kpi[df_kpi["Tên đơn vị"].astype(str) == unit_sel]
        sort_by_score = st.checkbox("Sắp xếp theo Điểm (giảm dần)", value=True)
        if sort_by_score and "Điểm" in df_kpi.columns:
            df_kpi = df_kpi.sort_values(by="Điểm", ascending=False)
        st.dataframe(df_kpi[show_cols] if show_cols else df_kpi, use_container_width=True, hide_index=True)
    else:
        st.caption("Chưa có dữ liệu KPI hoặc chưa cấu hình đúng tên sheet.")

with tab2:
    st.subheader("Biểu đồ KPI")
    df_kpi2, _ = load_kpi_df()
    if not df_kpi2.empty:
        c1, c2 = st.columns(2)
        with c1:
            field_y = st.selectbox("Trường giá trị", options=[c for c in ["Điểm","Thực hiện (tháng)","Thực hiện (lũy kế)","Kế hoạch"] if c in df_kpi2.columns])
        with c2:
            group_field = "Tên đơn vị" if "Tên đơn vị" in df_kpi2.columns else st.selectbox("Nhóm theo", options=[c for c in df_kpi2.columns if c not in ["Điểm"]])
        agg = df_kpi2.groupby(group_field, dropna=True)[field_y].sum().sort_values(ascending=False).head(20)
        fig, ax = plt.subplots()
        agg.plot(kind="bar", ax=ax)  # không chỉ định màu
        ax.set_ylabel(str(field_y)); ax.set_xlabel(str(group_field))
        ax.set_title(f"{field_y} theo {group_field}")
        st.pyplot(fig)
    else:
        st.caption("Chưa có dữ liệu để vẽ.")

with tab3:
    st.subheader("Quản trị / Kiểm tra kết nối")
    colq1, colq2 = st.columns(2)
    with colq1:
        if st.button("Kiểm tra kết nối Google Sheets", use_container_width=True):
            try:
                sh = open_spreadsheet(st.session_state["spreadsheet_id"])
                st.success(f"Kết nối OK: {sh.title}")
            except Exception:
                st.error("Lỗi kết nối. Kiểm tra ID/quyền truy cập.")
    with colq2:
        st.write("Tên sheet KPI hiện tại:", st.session_state.get("kpi_sheet_name","KPI"))
