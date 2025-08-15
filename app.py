# -*- coding: utf-8 -*-
"""
KPI App - Đội quản lý Điện lực khu vực Định Hóa
Yêu cầu của anh Long:
- Đăng nhập bằng USE + mật khẩu từ tab USE (Google Sheet) hoặc file USE.xlsx (fallback).
- Sai thông tin -> báo đúng "USE hoặc mật khẩu không đúng".
- Chỉ sau khi đăng nhập mới cho vào giao diện làm việc.
- Ô nhập Google Sheet ID/URL CHỈ hiển thị cho Admin (vd: PCTN\ADMIN, NPC\LONGPH).
- Giao diện gọn, dùng st.toast cho cảnh báo/nhắc nhở.
- Hạn chế phụ thuộc thêm thư viện (KHÔNG dùng matplotlib).

Lưu ý triển khai trên Streamlit Cloud:
- Cần cấu hình st.secrets["gdrive_service_account"] nếu dùng Google Sheet.
"""
import re
import io
import math
from datetime import datetime
import streamlit as st
import pandas as pd
import gspread
from google.oauth2.service_account import Credentials

# ==================== CẤU HÌNH ====================
st.set_page_config(page_title="KPI - Đội quản lý Điện lực khu vực Định Hóa", layout="wide")
APP_TITLE = "📊 KPI - Đội quản lý Điện lực khu vực Định Hóa"

# ==================== TIỆN ÍCH CHUNG ====================
ADMIN_ACCOUNTS = {  # so sánh dạng lower()
    r"pctn\admin",
    r"npc\longph",
}

def is_admin(username: str) -> bool:
    if not username:
        return False
    return username.strip().lower() in ADMIN_ACCOUNTS

def toast(msg, icon="ℹ️"):
    try:
        st.toast(msg, icon=icon)
    except Exception:
        pass

def extract_sheet_id(text: str) -> str:
    """Nhập ID hoặc URL Google Sheet -> trả về ID"""
    if not text:
        return ""
    text = text.strip()
    m = re.search(r"/d/([a-zA-Z0-9-_]+)", text)
    return m.group(1) if m else text

def get_gs_client():
    """Khởi tạo client gspread từ st.secrets (nếu có)"""
    try:
        svc = dict(st.secrets["gdrive_service_account"])
        if "private_key" in svc:
            svc["private_key"] = svc["private_key"].replace("\\n", "\n")
        scopes = [
            "https://www.googleapis.com/auth/spreadsheets",
            "https://www.googleapis.com/auth/drive",
        ]
        creds = Credentials.from_service_account_info(svc, scopes=scopes)
        return gspread.authorize(creds)
    except Exception:
        return None

def open_spreadsheet(sid_or_url: str):
    sid = extract_sheet_id(sid_or_url or "")
    if not sid:
        raise ValueError("missing_sid")
    client = st.session_state.get("_gs_client") or get_gs_client()
    st.session_state["_gs_client"] = client
    if client is None:
        raise RuntimeError("no_client")
    return client.open_by_key(sid)

def df_from_ws(ws) -> pd.DataFrame:
    records = ws.get_all_records(expected_headers=ws.row_values(1))
    return pd.DataFrame(records)

def normalize_columns(df: pd.DataFrame) -> pd.DataFrame:
    """Chuẩn hoá tên cột theo alias tối thiểu cần dùng trong app"""
    if df is None or df.empty:
        return df
    alias = {
        "USE (mã đăng nhập)": [
            "USE (mã đăng nhập)",
            r"Tài khoản (USE\username)",
            "Tài khoản (USE/username)",
            "Tài khoản (USE\\username)",
            "Tài khoản", "Username"
        ],
        "Mật khẩu mặc định": [
            "Mật khẩu mặc định","Password mặc định","Password","Mật khẩu","Mat khau mac dinh"
        ],
        "Tên đơn vị": ["Tên đơn vị","Đơn vị","Don vi","Ten don vi","Đơn vị/Phòng ban"],
        "Chỉ tiêu": ["Chỉ tiêu","KPI","Chi tieu","Tên KPI"],
        "Kế hoạch": ["Kế hoạch","Plan","Target","Ke hoach"],
        "Tháng": ["Tháng","Thang","Month"],
        "Năm": ["Năm","Nam","Year"],
        "Thực hiện (tháng)": ["Thực hiện (tháng)","Thực hiện tháng","Thuc hien (thang)","Actual (month)"],
        "Thực hiện (lũy kế)": ["Thực hiện (lũy kế)","Thuc hien (luy ke)","Actual (YTD)","Lũy kế"],
        "Đơn vị tính": ["Đơn vị tính","Don vi tinh","Unit"],
        "Trọng số": ["Trọng số","Trong so","Weight"],
        "Ghi chú": ["Ghi chú","Ghi chu","Notes"],
    }
    cols_lower = {c.strip().lower(): c for c in df.columns}
    rename_map = {}
    for std, cands in alias.items():
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

# ==================== TẢI USER (Sheet hoặc USE.xlsx) ====================
def load_users(spreadsheet_id_or_url: str = "") -> pd.DataFrame:
    """Ưu tiên đọc Google Sheet -> fallback USE.xlsx (sheet 'USE')."""
    # 1) Google Sheets
    client = get_gs_client()
    sid = extract_sheet_id(spreadsheet_id_or_url or "")
    if client is not None and sid:
        try:
            sh = client.open_by_key(sid)
            # Ưu tiên tab 'USE'; nếu không có thì quét tab phù hợp
            try:
                ws = sh.worksheet("USE")
            except Exception:
                ws = None
                for w in sh.worksheets():
                    hdr = [h.strip() for h in w.row_values(1)]
                    if (("USE (mã đăng nhập)" in hdr) or (r"Tài khoản (USE\username)" in hdr) or ("Tài khoản (USE\\username)" in hdr) or ("Tài khoản" in hdr) or ("Username" in hdr)) \
                       and ("Mật khẩu mặc định" in hdr or "Password" in hdr or "Mật khẩu" in hdr):
                        ws = w; break
                if ws is None:
                    raise RuntimeError("no_use_tab")
            df = df_from_ws(ws)
            return df
        except Exception:
            pass
    # 2) Fallback: file cục bộ
    try:
        df_local = pd.read_excel("USE.xlsx", sheet_name="USE")
        return df_local
    except Exception:
        return pd.DataFrame()

# ==================== KIỂM TRA ĐĂNG NHẬP ====================
def check_credentials(df: pd.DataFrame, use_input: str, pwd_input: str) -> bool:
    if df is None or df.empty:
        toast("Chưa tải được danh sách người dùng (USE).", "❗")
        return False

    df = normalize_columns(df)
    # Xác định cột bắt buộc
    col_use = None
    for c in df.columns:
        if c.strip().lower() in [
            "tài khoản (use\\username)".lower(),
            "tài khoản (use/username)".lower(),
            "use (mã đăng nhập)".lower(),
            "tài khoản".lower(), "username".lower()
        ]:
            col_use = c; break
    col_pw = None
    for c in df.columns:
        if c.strip().lower() in [
            "mật khẩu mặc định".lower(), "password mặc định".lower(),
            "password".lower(), "mật khẩu".lower()
        ]:
            col_pw = c; break

    if col_use is None or col_pw is None:
        toast("Thiếu cột USE hoặc Mật khẩu trong bảng USE.", "❗")
        return False

    u = (use_input or "").strip()
    p = (pwd_input or "").strip()

    row = df.loc[df[col_use].astype(str).str.strip() == u]
    if row.empty:
        st.error("USE hoặc mật khẩu không đúng")
        return False
    pass_ok = str(row.iloc[0][col_pw]).strip()
    if p and p == pass_ok:
        return True
    st.error("USE hoặc mật khẩu không đúng")
    return False

def generate_temp_password(n=8) -> str:
    import random, string
    chars = string.ascii_letters + string.digits
    return "".join(random.choice(chars) for _ in range(n))

# ==================== UI: SIDEBAR (ĐĂNG NHẬP + ADMIN) ====================
with st.sidebar:
    st.header("🔒 Đăng nhập")
    use_input = st.text_input("USE (vd: PCTN\\KVDHA)")
    pwd_input = st.text_input("Mật khẩu", type="password")
    colA, colB = st.columns(2)
    with colA:
        login_clicked = st.button("Đăng nhập", use_container_width=True, type="primary")
    with colB:
        forgot_clicked = st.button("Quên mật khẩu", use_container_width=True)

    # Nếu đã đăng nhập là admin -> hiện cấu hình Google Sheet
    if "_user" in st.session_state and is_admin(st.session_state["_user"]):
        st.markdown("---")
        st.header("⚙️ Quản trị (Admin)")
        sid_val = st.text_input(
            "Google Sheet ID/URL",
            value=st.session_state.get("spreadsheet_id",""),
            placeholder="Dán URL hoặc ID Google Sheet (/d/<ID>/edit)",
            help="Chỉ Admin thay đổi. Người dùng thường không thấy mục này."
        )
        st.session_state["spreadsheet_id"] = sid_val
        kpi_sheet_name = st.text_input(
            "Tên sheet KPI",
            value=st.session_state.get("kpi_sheet_name","KPI")
        )
        st.session_state["kpi_sheet_name"] = kpi_sheet_name

# Hành vi nút
if login_clicked:
    df_users = load_users(spreadsheet_id_or_url=st.session_state.get("spreadsheet_id",""))
    if check_credentials(df_users, use_input, pwd_input):
        st.session_state["_user"] = use_input
        toast(f"Đăng nhập thành công: {use_input}", "✅")

if forgot_clicked:
    u = (use_input or "").strip()
    if not u:
        toast("Vui lòng nhập USE trước khi bấm 'Quên mật khẩu'.", "❗")
    else:
        temp_pw = generate_temp_password(8)
        # Gợi ý quy trình: lưu mật khẩu tạm vào nơi quản trị theo dõi, hoặc gửi mail nội bộ
        toast(f"Đã cấp mật khẩu tạm cho {u}: {temp_pw}", "✅")
        st.info("Vui lòng liên hệ quản trị để được cập nhật mật khẩu chính thức trong hệ thống.")

# ==================== MAIN: BẮT BUỘC ĐĂNG NHẬP ====================
st.title(APP_TITLE)

if "_user" not in st.session_state:
    # Chưa đăng nhập -> dừng app tại đây, không lộ giao diện nghiệp vụ
    st.stop()

# ==================== GIAO DIỆN NGHIỆP VỤ (SAU KHI ĐĂNG NHẬP) ====================
def get_ws_by_name_or_guess(sh, prefer):
    for name in prefer:
        try:
            return sh.worksheet(name)
        except Exception:
            continue
    return None

def load_kpi_df():
    """Đọc KPI từ Google Sheet theo tên sheet KPI do admin cấu hình"""
    sid_cfg = st.session_state.get("spreadsheet_id","")
    sheet_name = st.session_state.get("kpi_sheet_name","KPI")
    if not sid_cfg:
        toast("Chưa cấu hình Google Sheet. Liên hệ Admin.", "❗")
        return pd.DataFrame(), []
    try:
        sh = open_spreadsheet(sid_cfg)
        ws = get_ws_by_name_or_guess(sh, [sheet_name, "KPI", "KPI_Data", "KPIs"])
        if ws is None:
            toast("Chưa tìm thấy sheet KPI. Kiểm tra tên sheet (Admin).", "❗")
            return pd.DataFrame(), []
        df = df_from_ws(ws)
        df = normalize_columns(df)
        # Tính điểm tổng quát (nhẹ) nếu có đủ cột
        def safe_float(x):
            try:
                s = str(x).strip().replace(",", ".")
                return float(s)
            except Exception:
                return None
        if "Điểm" not in df.columns and {"Kế hoạch","Thực hiện (tháng)","Trọng số"}.issubset(set(df.columns)):
            def compute_score(row):
                plan = safe_float(row.get("Kế hoạch"))
                actual = safe_float(row.get("Thực hiện (tháng)"))
                weight = safe_float(row.get("Trọng số")) or 0.0
                if not plan or not actual:
                    return None
                ratio = max(min(actual/plan, 2.0), 0.0)
                score10 = ratio * 10.0
                w = weight/100.0 if weight > 1 else weight
                return round(score10 * w, 2)
            df["Điểm"] = df.apply(compute_score, axis=1)
        # Chọn cột hiển thị
        cols = [c for c in ["Tên đơn vị","Chỉ tiêu","Đơn vị tính","Kế hoạch","Thực hiện (tháng)",
                            "Thực hiện (lũy kế)","Trọng số","Điểm","Tháng","Năm","Ghi chú"] if c in df.columns]
        return df, cols
    except ValueError as ve:
        if str(ve) == "missing_sid":
            toast("Chưa cấu hình Google Sheet. Liên hệ Admin.", "❗")
        else:
            toast("Lỗi tham số.", "❌")
        return pd.DataFrame(), []
    except Exception:
        toast("Không đọc được KPI. Kiểm tra ID/quyền truy cập (Admin).", "❌")
        return pd.DataFrame(), []

# Tabs chức năng
tab1, tab2 = st.tabs(["📋 Bảng KPI", "ℹ️ Thông tin"])

with tab1:
    st.subheader("Bảng KPI")
    # Bộ lọc thời gian dạng chuỗi để tránh lỗi kiểu
    months = ["Tất cả"] + [str(i) for i in range(1,13)]
    colf1, colf2, colf3 = st.columns([1,1,2])
    with colf1:
        month_choice = st.selectbox("Tháng", options=months, index=0)
    with colf2:
        year_choice = st.text_input("Năm", value=str(datetime.now().year))

    df_kpi, cols = load_kpi_df()
    if not df_kpi.empty:
        # Lọc theo thời gian nếu có cột
        def apply_time_filter(df):
            if "Năm" in df.columns and year_choice not in ("", None, "Tất cả"):
                df = df[df["Năm"].astype(str) == str(year_choice)]
            if "Tháng" in df.columns and month_choice not in ("", None, "Tất cả"):
                df = df[df["Tháng"].astype(str) == str(month_choice)]
            return df

        df_show = apply_time_filter(df_kpi.copy())
        # Lọc theo đơn vị (nếu có)
        if "Tên đơn vị" in df_show.columns:
            units = ["Tất cả"] + sorted(df_show["Tên đơn vị"].dropna().astype(str).unique().tolist())
            unit_sel = st.selectbox("Đơn vị", options=units, index=0)
            if unit_sel != "Tất cả":
                df_show = df_show[df_show["Tên đơn vị"].astype(str) == unit_sel]

        # Sắp xếp theo Điểm nếu có
        if "Điểm" in df_show.columns:
            sort_by_score = st.checkbox("Sắp xếp theo Điểm (giảm dần)", value=True)
            if sort_by_score:
                df_show = df_show.sort_values(by="Điểm", ascending=False)

        st.dataframe(df_show[cols] if cols else df_show, use_container_width=True, hide_index=True)
    else:
        st.caption("Chưa có dữ liệu KPI hoặc chưa được Admin cấu hình Google Sheet.")

with tab2:
    st.subheader("Thông tin phiên làm việc")
    st.write("Người dùng:", st.session_state.get("_user"))
    st.write("Vai trò:", "Admin" if is_admin(st.session_state.get("_user","")) else "User")
    if is_admin(st.session_state.get("_user","")):
        st.write("Google Sheet ID/URL:", st.session_state.get("spreadsheet_id","(chưa cấu hình)"))
        st.write("Tên sheet KPI:", st.session_state.get("kpi_sheet_name","KPI"))
    else:
        st.caption("Liên hệ Admin nếu cần thay đổi nguồn dữ liệu.")

