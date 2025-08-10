
# -*- coding: utf-8 -*-
import streamlit as st
import pandas as pd
from datetime import datetime
from io import BytesIO
import base64
import json

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

def compute_kpi_score(thuc_hien, ke_hoach, trong_so):
    ke_hoach = _safe_number(ke_hoach, 0.0)
    thuc_hien = _safe_number(thuc_hien, 0.0)
    trong_so = _safe_number(trong_so, 0.0)
    if ke_hoach == 0:
        return 0.0
    return round((thuc_hien / ke_hoach) * trong_so, 4)


def _kpi_sai_so_du_bao_diem(sai_so_percent, trong_so):
    """
    Công thức đặc thù cho các KPI 'Dự báo tổng thương phẩm ...':
    - Chuẩn: |sai số| ≤ 1.5%  => điểm = trọng số
    - Nếu vượt chuẩn: cứ 0.1% vượt → trừ 0.04 điểm, tối đa trừ 3 điểm
    - Không âm điểm
    Tham số:
      - sai_so_percent: nhập theo %, ví dụ 1.6 nghĩa là 1.6%
      - trong_so: điểm tối đa của chỉ tiêu
    """
    sai_so = abs(_safe_number(sai_so_percent, 0.0))
    ts = _safe_number(trong_so, 0.0)
    if sai_so <= 1.5:
        return ts
    vuot = sai_so - 1.5
    tru = (vuot / 0.1) * 0.04
    tru = min(tru, 3.0)
    return max(round(ts - tru, 4), 0.0)

def _is_du_bao_tong_thuong_pham(ten_chi_tieu: str) -> bool:
    if not ten_chi_tieu:
        return False
    s = ten_chi_tieu.strip().lower()
    return "dự báo tổng thương phẩm" in s

def compute_kpi_score_dynamic(ten_chi_tieu, thuc_hien, ke_hoach, trong_so):
    """
    Tự chọn công thức tính điểm theo tên chỉ tiêu:
    - Nếu tên chứa 'Dự báo tổng thương phẩm' (kể cả nhóm KH >1 triệu kWh/năm) → dùng công thức sai số ±1.5%
    - Ngược lại → công thức mặc định (Thực hiện/Kế hoạch)*Trọng số
    Ghi chú: với công thức sai số, trường 'Thực hiện' là giá trị sai số (%) theo tháng.
    """
    if _is_du_bao_tong_thuong_pham(ten_chi_tieu):
        return _kpi_sai_so_du_bao_diem(thuc_hien, trong_so)
    return compute_kpi_score(thuc_hien, ke_hoach, trong_so)
def export_dataframe_to_excel(df: pd.DataFrame) -> bytes:
    # Xuất ra file Excel trong bộ nhớ
    buffer = BytesIO()
    with pd.ExcelWriter(buffer, engine="xlsxwriter") as writer:
        df.to_excel(writer, index=False, sheet_name="KPI")
        # Định dạng nhẹ
        workbook  = writer.book
        worksheet = writer.sheets["KPI"]
        fmt = workbook.add_format({"text_wrap": True, "valign": "vcenter"})
        worksheet.set_column(0, len(df.columns)-1, 22, fmt)
    buffer.seek(0)
    return buffer.read()

def read_service_account_from_secrets():
    """
    Đọc khóa dịch vụ từ secrets. Hỗ trợ 2 kiểu:
    - private_key: chuỗi PEM đầy đủ (có xuống dòng)
    - private_key_b64: chuỗi PEM đã mã hóa base64
    Trả về dict thông tin tài khoản nếu có đủ, ngược lại ném lỗi.
    """
    try:
        conf = st.secrets["google_service_account"]
    except Exception:
        # Cho tương thích các cuộc trò chuyện cũ
        try:
            conf = st.secrets["gdrive_service_account"]
        except Exception as e:
            raise RuntimeError("Không tìm thấy google_service_account hoặc gdrive_service_account trong secrets.") from e

    conf = dict(conf)
    if "private_key" in conf and conf["private_key"]:
        # Cho phép dạng có \\n
        conf["private_key"] = conf["private_key"].replace("\\n", "\n")
        return conf

    if "private_key_b64" in conf and conf["private_key_b64"]:
        try:
            decoded = base64.b64decode(conf["private_key_b64"]).decode("utf-8")
            conf["private_key"] = decoded
            return conf
        except Exception as e:
            raise RuntimeError(f"Giải mã private_key_b64 lỗi: {e}")

    raise RuntimeError("Secrets thiếu private_key hoặc private_key_b64.")

def get_gspread_client_if_possible():
    """
    Thử khởi tạo gspread client nếu có đầy đủ secrets.
    Trả về (client, message). Nếu không thành công, trả về (None, cảnh báo).
    """
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

def init_session_state():
    if "kpi_rows" not in st.session_state:
        st.session_state.kpi_rows = []  # danh sách dict
    if "connected" not in st.session_state:
        st.session_state.connected = False
    if "connect_msg" not in st.session_state:
        st.session_state.connect_msg = ""
    if "editing_index" not in st.session_state:
        st.session_state.editing_index = None
    # form fields
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
    st.caption("Mắt Nâu sẽ vẫn cho phép **nhập KPI thủ công** kể cả khi chưa kết nối được Google.")

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
        # Không bắt buộc phải mở Sheet ở đây; chỉ đánh dấu 'đã sẵn sàng'
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
  <p style="margin:6px 0 0 0; color:#555">Nhập thủ công → Xuất Excel chuẩn (9 cột)</p>
</div>
""", unsafe_allow_html=True)

st.markdown("## KPI Scorer – Định Hóa (Full Suite)")

# Nếu chưa kết nối được, CHỈ ẩn các tính năng 0)–2) mà cần Google; vẫn hiện mục 3)
if connected:
    with st.expander("0) Đồng bộ dữ liệu Excel ⇄ Google Sheets (tùy chọn)", expanded=False):
        st.info("Khu vực này sẽ đồng bộ dữ liệu khi anh Long yêu cầu. (Đang bật cờ kết nối thành công)")

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
        # Điểm KPI tự tính & hiển thị
        diem_kpi_preview = compute_kpi_score_dynamic(st.session_state.get('ten_kpi', ''), st.session_state.thuc_hien, st.session_state.ke_hoach, st.session_state.trong_so)
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
            "Điểm KPI": compute_kpi_score_dynamic(st.session_state.ten_kpi, st.session_state.thuc_hien, st.session_state.ke_hoach, st.session_state.trong_so),
        }
        st.session_state.kpi_rows.append(row)
        st.success("Đã thêm 1 dòng KPI vào bảng tạm.")

# Hiển thị bảng tạm thời
df = pd.DataFrame(st.session_state.kpi_rows, columns=EXPECTED_KPI_COLS) if st.session_state.kpi_rows else pd.DataFrame(columns=EXPECTED_KPI_COLS)
st.dataframe(df, use_container_width=True, height=300)

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

# Gợi ý & trợ giúp
with cD:
    st.info(
        "📌 Gợi ý: Điểm KPI = (Thực hiện / Kế hoạch) × Trọng số.\n\n"
        "Nếu chưa kết nối Google, anh cứ nhập và tải Excel. Khi kết nối xong, em sẽ "
        "bổ sung nút **Đẩy lên Google Sheet** theo đúng format 9 cột."
    )

# Footer
st.caption("© BrownEyes – KPI Scorer (bản không dừng app khi lỗi kết nối).")
