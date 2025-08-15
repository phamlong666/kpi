from pathlib import Path

# Patch app.py to fix selectbox TypeError and remove the center caption.
code = r'''# -*- coding: utf-8 -*-
import re
import io
import json
import math
from datetime import datetime
import streamlit as st
import pandas as pd
import gspread
from google.oauth2.service_account import Credentials
import matplotlib.pyplot as plt

st.set_page_config(page_title="KPI - Đội quản lý Điện lực khu vực Định Hóa", layout="wide")
APP_TITLE = "📊 KPI - Đội quản lý Điện lực khu vực Định Hóa"

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
        "Tài khoản",
        "Username",
        "Tài khoản USE",
    ],
    "Mật khẩu mặc định": [
        "Mật khẩu mặc định",
        "Password mặc định",
        "Mat khau mac dinh",
        "Password",
        "Mật khẩu",
    ],
    "Tên đơn vị": [
        "Tên đơn vị","Ten don vi","Đơn vị","Don vi","Đơn vị/Phòng ban",
    ],
    "Chỉ tiêu": ["Chỉ tiêu","Chi tieu","KPI","Tên KPI"],
    "Kế hoạch": ["Kế hoạch","Ke hoach","Plan","Target"],
    "Thực hiện (tháng)": ["Thực hiện (tháng)","Thuc hien (thang)","Thực hiện tháng","Actual (month)"],
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
    for std_name, candidates in ALIAS.items():
        if std_name in df.columns:
            continue
        for c in candidates:
            key = c.strip().lower()
            if key in cols_lower:
                rename_map[cols_lower[key]] = std_name
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
        client = gspread.authorize(creds)
        return client
    except Exception as e:
        st.session_state["_gs_error"] = str(e)
        return None

def open_spreadsheet(spreadsheet_id_or_url: str):
    sid = extract_sheet_id(spreadsheet_id_or_url)
    if not sid:
        raise ValueError("Chưa nhập Spreadsheet ID/URL.")
    client = st.session_state.get("_gs_client")
    if client is None:
        client = get_gs_client()
        st.session_state["_gs_client"] = client
    if client is None:
        raise RuntimeError("Không thể khởi tạo kết nối Google Sheets.")
    return client.open_by_key(sid)

def find_use_worksheet(sh) -> gspread.Worksheet:
    try:
        return sh.worksheet("USE")
    except Exception:
        pass
    for ws in sh.worksheets():
        try:
            headers = [h.strip() for h in ws.row_values(1)]
            hdr_set = set(headers)
            need_any = [
                {"USE (mã đăng nhập)","Mật khẩu mặc định"},
                {r"Tài khoản (USE\\username)","Mật khẩu mặc định"},
                {r"Tài khoản (USE\username)","Mật khẩu mặc định"},
            ]
            for need in need_any:
                if need.issubset(hdr_set):
                    return ws
        except Exception:
            continue
    raise gspread.exceptions.WorksheetNotFound("Không tìm thấy sheet USE phù hợp.")

def get_ws_by_name_or_guess(sh, preferred_names):
    for name in preferred_names:
        try:
            return sh.worksheet(name)
        except Exception:
            continue
    return None

def safe_float(x):
    try:
        if x is None or (isinstance(x, float) and math.isnan(x)):
            return None
        s = str(x).replace(",", ".")
        return float(s)
    except Exception:
        return None

def compute_kpi_score(row):
    plan = safe_float(row.get("Kế hoạch"))
    actual = safe_float(row.get("Thực hiện (tháng)"))
    weight = safe_float(row.get("Trọng số")) or 0.0
    if plan is None or plan == 0 or actual is None:
        return None
    ratio = max(min(actual/plan, 2.0), 0.0)
    score10 = ratio * 10.0
    w = weight/100.0 if weight > 1.0 else weight
    return round(score10 * w, 2)

def prepare_kpi_df(df_raw: pd.DataFrame):
    if df_raw is None or df_raw.empty:
        return df_raw, []
    df = normalize_columns(df_raw.copy())
    if "Điểm" not in df.columns:
        df["Điểm"] = df.apply(compute_kpi_score, axis=1)
    columns_pref = [c for c in [
        "Tên đơn vị","Chỉ tiêu","Đơn vị tính","Kế hoạch","Thực hiện (tháng)",
        "Thực hiện (lũy kế)","Trọng số","Điểm","Tháng","Năm","Ghi chú"
    ] if c in df.columns]
    return df, columns_pref

def filter_by_time(df: pd.DataFrame, month_val, year_val):
    if df is None or df.empty:
        return df
    # year_val có thể là "Tất cả" hoặc số
    if (year_val not in (None, "", "Tất cả")) and "Năm" in df.columns:
        df = df[df["Năm"].astype(str) == str(year_val)]
    if (month_val not in (None, "Tất cả")) and "Tháng" in df.columns:
        df = df[df["Tháng"].astype(str) == str(month_val)]
    return df

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
    # Dùng chuỗi để tránh lỗi kiểu dữ liệu trong selectbox
    months = ["Tất cả"] + [str(i) for i in range(1,13)]
    month_choice = st.selectbox("Tháng", options=months, index=0)
    # Năm: cho phép bỏ trống hoặc nhập số
    year_choice = st.text_input("Năm", value=str(datetime.now().year))

login_msg = ""

def handle_login():
    global login_msg
    try:
        sh = open_spreadsheet(st.session_state["spreadsheet_id"])
        ws = find_use_worksheet(sh)
        df = df_from_ws(ws)
        df = normalize_columns(df)
        for req in ["USE (mã đăng nhập)","Mật khẩu mặc định"]:
            if req not in df.columns:
                raise ValueError(f"Thiếu cột bắt buộc: {req}")
        u = (username or "").strip()
        p = (password or "").strip()
        row = df.loc[df["USE (mã đăng nhập)"].astype(str).str.strip() == u]
        if row.empty:
            login_msg = "Sai tài khoản hoặc chưa có trong danh sách."
            return False
        pass_ok = str(row["Mật khẩu mặc định"].iloc[0]).strip()
        if p and p == pass_ok:
            st.session_state["_user"] = u
            st.session_state["_username"] = u
            st.session_state["_password"] = p
            login_msg = "Đăng nhập thành công."
            return True
        else:
            login_msg = "Mật khẩu không đúng."
            return False
    except Exception as e:
        login_msg = f"Lỗi đăng nhập: {e}"
        return False

def handle_logout():
    st.session_state.pop("_user", None)
    st.session_state["_password"] = ""
    st.session_state["_username"] = ""

def handle_sync_users():
    try:
        sh = open_spreadsheet(st.session_state["spreadsheet_id"])
        ws = find_use_worksheet(sh)
        df = df_from_ws(ws)
        df = normalize_columns(df)
        st.toast(f"Đã đọc {len(df)} người dùng từ sheet USE.", icon="✅")
    except Exception as e:
        st.toast(f"Đồng bộ thất bại: {e}", icon="❌")

if st.session_state.get("_first_run") is None:
    st.session_state["_first_run"] = False

if st.sidebar.button("Làm mới giao diện", use_container_width=True):
    st.rerun()

if st.session_state.get("_trigger_login") is None:
    st.session_state["_trigger_login"] = 0

if 'login_clicked' not in st.session_state:
    pass

# Buttons handling
if 'btn_login_handled' not in st.session_state:
    st.session_state['btn_login_handled'] = False

# Event handlers
if 'login_clicked_once' not in st.session_state:
    st.session_state['login_clicked_once'] = False

# Process buttons
if 'login_clicked_once' in st.session_state and st.session_state['login_clicked_once']:
    pass

# Actual events
if 'login_clicked_once' in st.session_state:
    pass

if 'login_clicked_once' in st.session_state and st.session_state['login_clicked_once']:
    pass

if st.session_state.get('dummy', False):
    pass

# Button events
if 'last_event' not in st.session_state:
    st.session_state['last_event'] = ''

# Actions
if login_clicked:
    handle_login()
if logout_clicked:
    handle_logout()
if sync_clicked:
    handle_sync_users()

st.title(APP_TITLE)

if "_user" in st.session_state:
    st.success(f"Đang đăng nhập: **{st.session_state['_user']}**")
elif login_msg:
    st.error(login_msg)

tab1, tab2, tab3 = st.tabs(["📋 Bảng KPI", "📈 Biểu đồ", "⚙️ Quản trị"])

def load_kpi_df():
    try:
        sh = open_spreadsheet(st.session_state["spreadsheet_id"])
        ws = get_ws_by_name_or_guess(sh, [st.session_state.get("kpi_sheet_name","KPI"), "KPI", "KPI_Data", "KPIs"])
        if ws is None:
            st.warning("Chưa tìm thấy sheet KPI. Hãy kiểm tra tên sheet ở sidebar.")
            return pd.DataFrame(), []
        df = df_from_ws(ws)
        df = normalize_columns(df)
        df, cols = prepare_kpi_df(df)
        df = filter_by_time(df, month_choice, year_choice)
        return df, cols
    except Exception as e:
        st.error(f"Lỗi khi đọc KPI: {e}")
        return pd.DataFrame(), []

with tab1:
    st.subheader("Bảng KPI")
    df_kpi, show_cols = load_kpi_df()
    if df_kpi is not None and not df_kpi.empty:
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
        st.info("Chưa có dữ liệu KPI hoặc chưa kết nối sheet.")

with tab2:
    st.subheader("Biểu đồ KPI")
    df_kpi2, show_cols2 = load_kpi_df()
    if df_kpi2 is not None and not df_kpi2.empty:
        col_plot1, col_plot2 = st.columns(2)
        with col_plot1:
            field_y = st.selectbox("Trường giá trị", options=[c for c in ["Điểm","Thực hiện (tháng)","Thực hiện (lũy kế)","Kế hoạch"] if c in df_kpi2.columns])
        with col_plot2:
            group_field = "Tên đơn vị" if "Tên đơn vị" in df_kpi2.columns else st.selectbox("Nhóm theo", options=[c for c in df_kpi2.columns if c not in ["Điểm"]])
        agg = df_kpi2.groupby(group_field, dropna=True)[field_y].sum().sort_values(ascending=False).head(20)
        fig, ax = plt.subplots()
        agg.plot(kind="bar", ax=ax)
        ax.set_ylabel(str(field_y))
        ax.set_xlabel(str(group_field))
        ax.set_title(f"{field_y} theo {group_field}")
        st.pyplot(fig)
    else:
        st.info("Chưa có dữ liệu để vẽ.")

with tab3:
    st.subheader("Quản trị / Kiểm tra kết nối")
    colq1, colq2 = st.columns(2)
    with colq1:
        if st.button("Kiểm tra kết nối Google Sheets", use_container_width=True):
            try:
                sh = open_spreadsheet(st.session_state["spreadsheet_id"])
                st.success(f"Kết nối OK: {sh.title}")
            except Exception as e:
                st.error(f"Lỗi: {e}")
    with colq2:
        st.write("Tên sheet KPI hiện tại:", st.session_state.get("kpi_sheet_name","KPI"))
'''
Path("/mnt/data/app.py").write_text(code, encoding="utf-8")
print("Patched full app.py written, size ~{:.1f} KB".format(Path('/mnt/data/app.py').stat().st_size/1024))
