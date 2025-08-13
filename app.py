# -*- coding: utf-8 -*-
import streamlit as st
import pandas as pd
from datetime import datetime
from io import BytesIO
import base64
import json
import re

# ---- Cấu hình trang ----
st.set_page_config(
    page_title="KPI Scorer – Định Hóa (Full Suite)",
    layout="wide",
    initial_sidebar_state="expanded",
)

# ------------------------
# 1) TIỆN ÍCH & HÀM DÙNG CHUNG
# ------------------------

EXPECTED_KPI_COLS = [
    "Tên chỉ tiêu (KPI)",          # 1
    "Đơn vị tính",                 # 2
    "Kế hoạch",                    # 3
    "Thực hiện",                   # 4
    "Trọng số",                    # 5
    "Bộ phận/người phụ trách",     # 6
    "Tháng",                       # 7
    "Năm",                         # 8
    "Điểm KPI",                    # 9 = (Thực hiện / Kế hoạch) × Trọng số
]

def _safe_number(x, default=0.0):
    try:
        if x is None or x == "":
            return float(default)
        return float(x)
    except Exception:
        return float(default)

# --- Công thức mặc định (dạng tỷ lệ) ---
def compute_kpi_score(thuc_hien, ke_hoach, trong_so):
    ke_hoach = _safe_number(ke_hoach, 0.0)
    thuc_hien = _safe_number(thuc_hien, 0.0)
    trong_so = _safe_number(trong_so, 0.0)
    if ke_hoach == 0:
        return 0.0
    return round((thuc_hien / ke_hoach) * trong_so, 4)

# --- Công thức đặc thù: Dự báo tổng thương phẩm (±1.5%, trừ 0.04 mỗi 0.1%) ---
def _kpi_sai_so_du_bao_diem(sai_so_percent, trong_so):
    """
    - |sai số| ≤ 1.5%  => điểm = trọng số
    - Nếu vượt chuẩn: cứ 0.1% vượt → trừ 0.04 điểm, tối đa trừ 3 điểm
    - Không âm điểm
    """
    sai_so = abs(_safe_number(sai_so_percent, 0.0))
    ts = _safe_number(trong_so, 0.0)
    if sai_so <= 1.5:
        return ts
    vuot = sai_so - 1.5
    tru = (vuot / 0.1) * 0.04
    tru = min(tru, 3.0)
    return max(round(ts - tru, 4), 0.0)

# --- Nhận diện tên KPI dự báo ---
def _is_du_bao_tong_thuong_pham(ten_chi_tieu: str) -> bool:
    if not ten_chi_tieu:
        return False
    s = ten_chi_tieu.strip().lower()
    return "dự báo tổng thương phẩm" in s

# --- Tính điểm động cho bảng nhập tay (không có sai số % rõ ràng) ---
def compute_kpi_score_dynamic(ten_chi_tieu, thuc_hien, ke_hoach, trong_so):
    """
    - Nếu tên chứa 'Dự báo tổng thương phẩm' → coi 'Thực hiện' là sai số (%) theo tháng và áp công thức ±1.5%.
    - Ngược lại → công thức mặc định (Thực hiện/Kế hoạch)*Trọng số.
    """
    if _is_du_bao_tong_thuong_pham(ten_chi_tieu):
        return _kpi_sai_so_du_bao_diem(thuc_hien, trong_so)
    return compute_kpi_score(thuc_hien, ke_hoach, trong_so)

# --- Xuất DataFrame ra Excel bytes ---
def export_dataframe_to_excel(df: pd.DataFrame) -> bytes:
    buffer = BytesIO()
    with pd.ExcelWriter(buffer, engine="xlsxwriter") as writer:
        df.to_excel(writer, index=False, sheet_name="KPI")
        workbook  = writer.book
        worksheet = writer.sheets["KPI"]
        fmt = workbook.add_format({"text_wrap": True, "valign": "vcenter"})
        worksheet.set_column(0, len(df.columns)-1, 22, fmt)
    buffer.seek(0)
    return buffer.read()

# --- Đọc service account từ secrets (tuỳ chọn) ---
def read_service_account_from_secrets():
    try:
        conf = st.secrets["google_service_account"]
    except Exception:
        try:
            conf = st.secrets["gdrive_service_account"]
        except Exception as e:
            raise RuntimeError("Không tìm thấy google_service_account hoặc gdrive_service_account trong secrets.") from e

    conf = dict(conf)
    if "private_key" in conf and conf["private_key"]:
        conf["private_key"] = conf["private_key"].replace("\\n", "\n")
        return conf
    if "private_key_b64" in conf and conf["private_key_b64"]:
        import base64
        try:
            decoded = base64.b64decode(conf["private_key_b64"]).decode("utf-8")
            conf["private_key"] = decoded
            return conf
        except Exception as e:
            raise RuntimeError(f"Giải mã private_key_b64 lỗi: {e}")
    raise RuntimeError("Secrets thiếu private_key hoặc private_key_b64.")

# --- Thử kết nối gspread (không bắt buộc) ---
def get_gspread_client_if_possible():
    try:
        from oauth2client.service_account import ServiceAccountCredentials
        import gspread
    except Exception as e:
        return None, f"Thiếu thư viện gspread/oauth2client: {e}"

    try:
        sa = read_service_account_from_secrets()
        scope = [
            "https://spreadsheets.google.com/feeds",
            "https://www.googleapis.com/auth/spreadsheets",
            "https://www.googleapis.com/auth/drive",
            "https://www.googleapis.com/auth/drive.file",
        ]
        creds = ServiceAccountCredentials.from_json_keyfile_dict(sa, scope)
        client = gspread.authorize(creds)
        return client, "Đã sẵn sàng kết nối Google Sheets."
    except Exception as e:
        return None, str(e)

# --- Session state ---
def init_session_state():
    if "kpi_rows" not in st.session_state:
        st.session_state.kpi_rows = []
    if "connected" not in st.session_state:
        st.session_state.connected = False
    if "connect_msg" not in st.session_state:
        st.session_state.connect_msg = ""
    if "editing_index" not in st.session_state:
        st.session_state.editing_index = None
    for k, v in {
        'ten_kpi':'', 'dvt':'', 'ke_hoach':0.0, 'thuc_hien':0.0, 'trong_so':0.0,
        'bo_phan':'Tổ Kinh doanh tổng hợp', 'thang':datetime.now().month, 'nam':datetime.now().year
    }.items():
        st.session_state.setdefault(k, v)

init_session_state()

# ------------------------
# 2) GIAO DIỆN SIDEBAR
# ------------------------
with st.sidebar:
    st.header("🔗 Kết nối")
    spreadsheet_id = st.text_input(
        "Spreadsheet ID",
        help=(
            "Dán ID của Google Sheets. Ví dụ từ URL:\n"
            "https://docs.google.com/spreadsheets/d/1AbCdEfGh.../edit#gid=0\n"
            "=> Spreadsheet ID là phần giữa /d/ và /edit"
        ),
    )
    nhom_cham = st.selectbox("Nhóm chấm", [1,2,3,4,5], index=1)
    email_nhan_bao_cao = st.text_input("Email nhận báo cáo", "phamlong666@gmail.com")

    st.markdown("---")
    st.caption("Mắt Nâu vẫn cho phép **nhập KPI thủ công** kể cả khi chưa kết nối Google.")

# ------------------------
# 3) THỬ KẾT NỐI GOOGLE (KHÔNG DỪNG APP)
# ------------------------
connected = False
connect_msg = ""
if spreadsheet_id:
    client, msg = get_gspread_client_if_possible()
    if client is None:
        connect_msg = f"Không kết nối được Google Sheets: {msg}"
        st.warning(connect_msg)
    else:
        connected = True
        connect_msg = "Kết nối Google Sheets sẵn sàng."
        st.success(connect_msg)

st.session_state.connected = connected
st.session_state.connect_msg = connect_msg

# ------------------------
# 4) HEADER
# ------------------------
st.markdown("""
<div style="padding:14px 18px; border:1px solid #ececec; border-radius:12px; background:#fff9ef">
  <h1 style="margin:0">KPI Đội quản lý Điện lực khu vực Định Hóa</h1>
  <p style="margin:6px 0 0 0; color:#555">Nhập thủ công → Xuất Excel chuẩn (9 cột) + Nạp file mẫu 1 tháng để nhập TH và tính điểm</p>
</div>
""", unsafe_allow_html=True)

st.markdown("## KPI Scorer – Định Hóa (Full Suite)")

if connected:
    with st.expander("0) Đồng bộ dữ liệu Excel ⇄ Google Sheets (tùy chọn)", expanded=False):
        st.info("Khu vực này sẽ đồng bộ dữ liệu khi anh Long yêu cầu. (Đã sẵn sàng kết nối)")
    with st.expander("1) Nhập KPI từ Google Sheet (chuẩn hóa sang 9 cột)", expanded=False):
        st.info("Đang chuẩn bị, sẽ đọc dữ liệu nguồn rồi ánh xạ sang 9 cột chuẩn.")
    with st.expander("2) Báo cáo theo cá nhân/bộ phận (khi đã có dữ liệu trên Sheets)", expanded=False):
        st.info("Đang chuẩn bị, sẽ lọc/summarize theo nhân sự hoặc bộ phận.")

# ------------------------
# 5) NHẬP KPI THỦ CÔNG & XUẤT EXCEL (LUÔN HIỂN THỊ)
# ------------------------
st.markdown("---")
st.subheader("3) Nhập KPI thủ công & Xuất Excel (9 cột)")

with st.form("kpi_input_form", clear_on_submit=False):
    c1, c2, c3 = st.columns([1.2, 1.2, 1])
    with c1:
        ten_chi_tieu = st.text_input("1) Tên chỉ tiêu (KPI)")
        don_vi_tinh = st.text_input("2) Đơn vị tính")
        ke_hoach = st.number_input("3) Kế hoạch", min_value=0.0, step=0.1, format="%.4f")
    with c2:
        thuc_hien = st.number_input("4) Thực hiện", min_value=0.0, step=0.1, format="%.4f")
        trong_so = st.number_input("5) Trọng số", min_value=0.0, step=0.1, format="%.4f")
    bo_phan_list = [
        "Tổ Kế hoạch kỹ thuật",
        "Tổ Kinh doanh tổng hợp",
        "Tổ Quản lý tổng hợp 1",
        "Tổ Quản lý tổng hợp 2",
        "Tổ Trực vận hành",
        "Tổ Kiểm tra giám sát mua bán điện",
    ]
    bo_phan = st.selectbox("6) Bộ phận/người phụ trách", bo_phan_list)
    with c3:
        thang = st.selectbox("7) Tháng", list(range(1,13)), index=datetime.now().month-1)
        nam = st.number_input("8) Năm", min_value=2000, max_value=2100, value=datetime.now().year, step=1)
        diem_kpi_preview = compute_kpi_score_dynamic(ten_chi_tieu, thuc_hien, ke_hoach, trong_so)
        st.metric("9) Điểm KPI (xem trước)", diem_kpi_preview)

    submitted = st.form_submit_button("➕ Thêm vào bảng tạm")
    if submitted:
        row = {
            "Tên chỉ tiêu (KPI)": ten_chi_tieu.strip(),
            "Đơn vị tính": don_vi_tinh.strip(),
            "Kế hoạch": _safe_number(ke_hoach, 0.0),
            "Thực hiện": _safe_number(thuc_hien, 0.0),
            "Trọng số": _safe_number(trong_so, 0.0),
            "Bộ phận/người phụ trách": bo_phan.strip(),
            "Tháng": int(thang),
            "Năm": int(nam),
            "Điểm KPI": compute_kpi_score_dynamic(ten_chi_tieu, thuc_hien, ke_hoach, trong_so),
        }
        st.session_state.kpi_rows.append(row)
        st.success("Đã thêm 1 dòng KPI vào bảng tạm.")

# Hiển thị bảng tạm thời
df_manual = pd.DataFrame(st.session_state.kpi_rows, columns=EXPECTED_KPI_COLS) if st.session_state.kpi_rows else pd.DataFrame(columns=EXPECTED_KPI_COLS)
st.dataframe(df_manual, use_container_width=True, height=300)

# Các nút thao tác
cA, cB, cC, cD = st.columns([1,1,1,2])
with cA:
    if st.button("🗑️ Xóa dòng cuối"):
        if st.session_state.kpi_rows:
            st.session_state.kpi_rows.pop()
            st.warning("Đã xóa dòng cuối.")
        else:
            st.info("Không còn dòng để xóa.")
with cB:
    if st.button("🧹 Xóa tất cả"):
        st.session_state.kpi_rows = []
        st.warning("Đã xóa toàn bộ bảng tạm.")
with cC:
    if st.button("💾 Xuất Excel (tải xuống)"):
        if len(st.session_state.kpi_rows) == 0:
            st.error("Chưa có dữ liệu để xuất.")
        else:
            out_df = pd.DataFrame(st.session_state.kpi_rows, columns=EXPECTED_KPI_COLS)
            bin_data = export_dataframe_to_excel(out_df)
            file_name = f"KPI_Scorer_{datetime.now().strftime('%Y%m%d_%H%M%S')}.xlsx"
            st.download_button(
                label="⬇️ Tải file Excel",
                data=bin_data,
                file_name=file_name,
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            )
with cD:
    st.info(
        "📌 Gợi ý: Điểm KPI = (Thực hiện / Kế hoạch) × Trọng số.\n\n"
        "Nếu chưa kết nối Google, anh cứ nhập và tải Excel. Khi kết nối xong, em sẽ bổ sung nút **Đẩy lên Google Sheet** theo đúng format 9 cột."
    )

# ------------------------
# 6) NẠP FILE CHUẨN 1 THÁNG → NHẬP TH & AUTO-SCORE (HỖ TRỢ EXCEL & CSV)
# ------------------------
st.markdown("---")
st.subheader("4) Nạp file chuẩn 1 tháng → Nhập 'Thực hiện (tháng)' → Tự tính điểm cho 2 chỉ tiêu Dự báo")

TOTAL_FORECAST_REGEX = re.compile(r"dự\s*báo.*tổng\s*thương\s*phẩm(?!.*triệu)", re.IGNORECASE)
SEGMENT_FORECAST_REGEX = re.compile(r"dự\s*báo.*tổng\s*thương\s*phẩm.*(1\s*triệu|>\s*1\s*triệu|trên\s*1\s*triệu)", re.IGNORECASE)

@st.cache_data(show_spinner=False)
def load_template_from_bytes(b: bytes) -> pd.DataFrame:
    """Đọc Excel .xlsx (cần openpyxl) và trả về DataFrame đã chuẩn cột."""
    xls = pd.ExcelFile(BytesIO(b))
    if "KPI_Input" not in xls.sheet_names:
        raise ValueError("Không tìm thấy sheet 'KPI_Input' trong file.")
    df = pd.read_excel(xls, sheet_name="KPI_Input")
    required = [
        "STT", "Nhóm/Parent", "Tên chỉ tiêu (KPI)", "Phương pháp đo kết quả",
        "Đơn vị tính", "Bộ phận/người phụ trách", "Kế hoạch (tháng)",
        "Thực hiện (tháng)", "Trọng số", "Điểm KPI", "Tháng", "Năm"
    ]
    missing = [c for c in required if c not in df.columns]
    if missing:
        raise ValueError(f"Thiếu cột bắt buộc: {missing}")
    return df[required].copy()

# Quy tắc tính điểm cho file 1 tháng (tính sai số từ KH & TH)
def _forecast_point_from_plan_actual(plan, actual, max_point: float = 3.0, threshold=1.5):
    try:
        plan = float(plan); actual = float(actual)
    except Exception:
        return None
    if plan == 0:
        return 0.0
    error_pct = (actual - plan) / plan * 100.0
    abs_err = abs(error_pct)
    if abs_err <= threshold:
        return max_point
    steps = (abs_err - threshold) / 0.1
    deduction = steps * 0.04
    return max(0.0, round(max_point - deduction, 4))


def autoscore_row_onemonth(row: pd.Series) -> float:
    name = str(row.get("Tên chỉ tiêu (KPI)", ""))
    method = str(row.get("Phương pháp đo kết quả", ""))
    plan = row.get("Kế hoạch (tháng)")
    actual = row.get("Thực hiện (tháng)")
    try:
        float(plan); float(actual)
    except Exception:
        return row.get("Điểm KPI", None)
    if TOTAL_FORECAST_REGEX.search(name) or TOTAL_FORECAST_REGEX.search(method):
        return _forecast_point_from_plan_actual(plan, actual)
    if SEGMENT_FORECAST_REGEX.search(name) or SEGMENT_FORECAST_REGEX.search(method):
        return _forecast_point_from_plan_actual(plan, actual)
    return row.get("Điểm KPI", None)


def autoscore_dataframe_onemonth(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return df
    out = df.copy()
    out["Điểm KPI"] = out.apply(autoscore_row_onemonth, axis=1)
    return out

# Chọn nguồn file (có hỗ trợ CSV khi thiếu openpyxl)
default_excel_path = "/mnt/data/KPI_OneMonth_Template.xlsx"
default_csv_hint = "Nếu môi trường thiếu openpyxl, dùng CSV: KPI_Input_template.csv"
mode = st.radio(
    "Nguồn file 1 tháng",
    ["Dùng đường dẫn hệ thống (Excel .xlsx)", "Tải Excel (.xlsx)", "Tải CSV (.csv)"],
    horizontal=True
)

file_bytes = None
df1 = pd.DataFrame()
if mode == "Dùng đường dẫn hệ thống (Excel .xlsx)":
    path = st.text_input("Path Excel", value=default_excel_path)
    if st.button("📂 Đọc Excel từ path"):
        try:
            with open(path, "rb") as f:
                file_bytes = f.read()
            df1 = load_template_from_bytes(file_bytes)
        except Exception as e:
            st.error(f"Lỗi khi đọc Excel (.xlsx): {e}")
            st.info(default_csv_hint)
elif mode == "Tải Excel (.xlsx)":
    up = st.file_uploader("Tải file Excel KPI_OneMonth_Template.xlsx", type=["xlsx"])
    if up is not None:
        try:
            file_bytes = up.read()
            df1 = load_template_from_bytes(file_bytes)
        except Exception as e:
            st.error(f"Lỗi khi đọc Excel (.xlsx): {e}")
            st.info(default_csv_hint)
else:  # CSV mode
    upc = st.file_uploader("Tải file CSV (KPI_Input_template.csv)", type=["csv"])
    if upc is not None:
        try:
            df1 = pd.read_csv(upc)
        except Exception as e:
            st.error(f"Lỗi khi đọc CSV: {e}")

# Guard: nếu chưa đọc được dữ liệu hợp lệ thì dừng ở đây
if df1 is None or df1.empty:
    st.info("⚠️ Chưa có dữ liệu hợp lệ. Vui lòng chọn 1 trong 3 cách: nhập path Excel, tải Excel, hoặc tải CSV.")
else:
    # Kiểm tra cột bắt buộc
    required = [
        "STT", "Nhóm/Parent", "Tên chỉ tiêu (KPI)", "Phương pháp đo kết quả",
        "Đơn vị tính", "Bộ phận/người phụ trách", "Kế hoạch (tháng)",
        "Thực hiện (tháng)", "Trọng số", "Điểm KPI", "Tháng", "Năm"
    ]
    missing = [c for c in required if c not in df1.columns]
    if missing:
        st.error(f"Thiếu cột bắt buộc: {missing}")
        st.write("Các cột hiện có:", list(df1.columns))
        st.stop()

    # Chọn tháng/năm để lọc
    colM, colY = st.columns(2)
    with colM:
        month_default = int(df1["Tháng"].iloc[0]) if "Tháng" in df1.columns and len(df1)>0 else 7
        chosen_month = st.number_input("Tháng", min_value=1, max_value=12, value=month_default, step=1)
    with colY:
        year_default = int(df1["Năm"].iloc[0]) if "Năm" in df1.columns and len(df1)>0 else datetime.now().year
        chosen_year = st.number_input("Năm", min_value=2000, max_value=2100, value=year_default, step=1)

    base = df1[(df1["Tháng"].astype(int) == int(chosen_month)) & (df1["Năm"].astype(int) == int(chosen_year))].copy()

    with st.expander("🔎 Tìm nhanh theo 'Phương pháp đo kết quả' / Tên KPI / Bộ phận"):
        q = st.text_input("Từ khóa", value="")
        col1, col2 = st.columns(2)
        with col1:
            departments = [x for x in sorted(base["Bộ phận/người phụ trách"].dropna().astype(str).unique().tolist()) if x]
            dept = st.multiselect("Bộ phận", departments, default=[])
        with col2:
            units = [x for x in sorted(base["Đơn vị tính"].dropna().astype(str).unique().tolist()) if x]
            unit = st.multiselect("Đơn vị tính", units, default=[])

        mask = pd.Series([True] * len(base))
        if q:
            qlow = q.lower()
            mask &= base.apply(lambda r: qlow in str(r["Phương pháp đo kết quả"]).lower() \
                                           or qlow in str(r["Tên chỉ tiêu (KPI)"]).lower() \
                                           or qlow in str(r["Bộ phận/người phụ trách"]).lower(), axis=1)
        if dept:
            mask &= base["Bộ phận/người phụ trách"].astype(str).isin(dept)
        if unit:
            mask &= base["Đơn vị tính"].astype(str).isin(unit)
        base = base[mask].copy()

    st.markdown("**Nhập cột 'Thực hiện (tháng)' để tính điểm (2 chỉ tiêu Dự báo):**")
    edited = st.data_editor(
        base,
        use_container_width=True,
        hide_index=True,
        column_config={
            "Thực hiện (tháng)": st.column_config.NumberColumn(format="%f"),
            "Trọng số": st.column_config.NumberColumn(format="%f"),
            "Điểm KPI": st.column_config.NumberColumn(format="%f", disabled=True),
        },
        num_rows="fixed",
    )

    scored = autoscore_dataframe_onemonth(edited)

    st.markdown("**Kết quả sau tính điểm:**")
    st.dataframe(scored, use_container_width=True, hide_index=True)

    colL, colR = st.columns([1,1])
    with colL:
        if st.button("💾 Xuất Excel (.xlsx) – bảng 1 tháng"):
            output = BytesIO()
            with pd.ExcelWriter(output, engine="xlsxwriter") as writer:
                scored.to_excel(writer, index=False, sheet_name="KPI_Input")
                wb = writer.book
                ws = writer.sheets["KPI_Input"]
                fmt_header = wb.add_format({"bold": True, "bg_color": "#E2F0D9", "border": 1})
                fmt_cell = wb.add_format({"border": 1})
                ws.set_row(0, 22, fmt_header)
                for i, _ in enumerate(scored.columns):
                    ws.set_column(i, i, 22, fmt_cell)
            st.download_button(
                label="Tải về KPI_Input",
                data=output.getvalue(),
                file_name=f"KPI_Input_{int(chosen_year)}_{int(chosen_month):02d}.xlsx",
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            )
    with colR:
        st.caption("Sẽ bổ sung xuất PDF & tổng hợp theo Bộ phận ở bản sau.")

# ------------------------
# Footer
# ------------------------
st.caption("© BrownEyes – KPI Scorer (Full Suite: nhập tay + file 1 tháng + auto-score 2 KPI Dự báo).")
