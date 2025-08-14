# -*- coding: utf-8 -*-
import streamlit as st
import pandas as pd
from datetime import datetime
from io import BytesIO
import base64
import os
import unicodedata

# =============================
# CẤU HÌNH TRANG
# =============================
st.set_page_config(
    page_title="KPI Scorer – Định Hóa (Full Suite)",
    page_icon="⚡",
    layout="wide",
    initial_sidebar_state="expanded",
)

# =============================
# HẰNG SỐ & HÀM DÙNG CHUNG
# =============================
EXPECTED_KPI_COLS = [
    "Tên chỉ tiêu (KPI)",          # 1
    "Đơn vị tính",                 # 2
    "Kế hoạch",                    # 3
    "Thực hiện",                   # 4
    "Trọng số",                    # 5
    "Bộ phận/người phụ trách",     # 6
    "Tháng",                       # 7
    "Năm",                         # 8
    "Điểm KPI",                    # 9 = (Thực hiện/Kế hoạch)×Trọng số (trừ KPI dự báo)
]


def _safe_number(x, default=0.0):
    try:
        if x is None or x == "":
            return float(default)
        return float(x)
    except Exception:
        return float(default)


# --- CÔNG THỨC CHUNG ---

def compute_kpi_score(thuc_hien, ke_hoach, trong_so):
    ke_hoach = _safe_number(ke_hoach, 0.0)
    thuc_hien = _safe_number(thuc_hien, 0.0)
    trong_so = _safe_number(trong_so, 0.0)
    if ke_hoach == 0:
        return 0.0
    return round((thuc_hien / ke_hoach) * trong_so, 4)


# --- KPI DỰ BÁO TỔNG THƯƠNG PHẨM (±1.5%; vượt 0.1% trừ 0.04; trần 3đ) ---

def _kpi_sai_so_du_bao_diem(sai_so_percent, trong_so):
    sai_so = abs(_safe_number(sai_so_percent, 0.0))
    ts = min(_safe_number(trong_so, 0.0), 3.0)  # trần 3 điểm
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
    # Nếu là KPI dự báo → thuc_hien coi là sai số (%) theo tháng
    if _is_du_bao_tong_thuong_pham(ten_chi_tieu):
        return _kpi_sai_so_du_bao_diem(thuc_hien, trong_so)
    return compute_kpi_score(thuc_hien, ke_hoach, trong_so)


def export_dataframe_to_excel(df: pd.DataFrame) -> bytes:
    buffer = BytesIO()
    with pd.ExcelWriter(buffer, engine="xlsxwriter") as writer:
        df.to_excel(writer, index=False, sheet_name="KPI")
        workbook = writer.book
        worksheet = writer.sheets["KPI"]
        fmt = workbook.add_format({"text_wrap": True, "valign": "vcenter"})
        worksheet.set_column(0, len(df.columns)-1, 22, fmt)
    buffer.seek(0)
    return buffer.read()


# =============================
# LOGO TRÒN (đa nguồn + mặc định GitHub của anh Long)
# =============================

def _detect_logo_bytes():
    """Ưu tiên:
    1) secrets['ui']['logo_url'] hoặc secrets['logo_url']
    2) /mnt/data/logo.png
    3) ./assets/logo.png hoặc ./.streamlit/logo.png
    4) ENV LOGO_URL
    5) DEFAULT_LOGO_URL (GitHub của anh Long)
    """
    DEFAULT_LOGO_URL = "https://raw.githubusercontent.com/phamlong666/kpi/main/logo_hinh_tron.png"
    try:
        ui = st.secrets.get("ui", {})
        logo_url = ui.get("logo_url") or st.secrets.get("logo_url")
        if logo_url:
            return f'<img class="floating-logo" src="{logo_url}" />', "secrets.logo_url"
    except Exception:
        pass
    for p in ["/mnt/data/logo.png", "./assets/logo.png", "./.streamlit/logo.png"]:
        if os.path.exists(p):
            try:
                with open(p, "rb") as f:
                    b64 = base64.b64encode(f.read()).decode()
                return f'<img class="floating-logo" src="data:image/png;base64,{b64}" />', p
            except Exception:
                pass
    env_logo = os.getenv("LOGO_URL")
    if env_logo:
        return f'<img class="floating-logo" src="{env_logo}" />', "env.LOGO_URL"
    if DEFAULT_LOGO_URL:
        return f'<img class="floating-logo" src="{DEFAULT_LOGO_URL}" />', "DEFAULT_LOGO_URL"
    return '<div class="floating-logo">⚡</div>', "fallback"


def _inject_ui_enhancements():
    logo_tag, _ = _detect_logo_bytes()
    css = """
    <style>
    .title-card{padding:14px 18px;border:1px solid #ececec;border-radius:12px;background:#fff;box-shadow:0 2px 8px rgba(0,0,0,0.04)}
    .title-card h1{margin:0;font-size:28px;line-height:1.25;font-weight:800;color:#0B5ED7;display:flex;align-items:center;gap:10px}
    .title-card .title-icon{font-size:26px;background:#0B5ED7;color:#fff;width:36px;height:36px;border-radius:50%;display:inline-flex;align-items:center;justify-content:center;box-shadow:0 2px 6px rgba(11,94,215,.35)}
    .title-card .subtitle{margin:6px 0 0 0;color:#444}
    .section-title{font-size:24px;font-weight:800;margin:6px 0 12px 0;color:#222}
    [data-testid="stDataFrame"] *,[data-testid="stDataEditor"] *,[data-testid="stDataEditorGrid"] *{font-size:20px !important}
    html, body, [data-testid="stAppViewContainer"] *{font-size:20px}
    .stTextInput>div>div>input,.stNumberInput input{font-size:19px !important}
    .stButton>button{font-size:18px !important}
    .floating-logo{position:fixed;right:16px;top:86px;width:76px;height:76px;border-radius:50%;box-shadow:0 6px 16px rgba(0,0,0,0.15);z-index:99999;background:#ffffffee;backdrop-filter:blur(4px);display:inline-block;object-fit:cover;text-align:center;line-height:76px;font-size:38px;animation:pop .6s ease-out;pointer-events:none}
    @keyframes pop{0%{transform:scale(.6);opacity:.2}100%{transform:scale(1);opacity:1}}
    </style>
    """
    st.markdown(css, unsafe_allow_html=True)
    st.markdown(logo_tag, unsafe_allow_html=True)


# =============================
# GOOGLE SHEETS (tùy chọn) & SESSION STATE
# =============================

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
        # FIX: thay '\n' bằng xuống dòng thật (handle cả dạng đã bị escape)
        pk = str(conf["private_key"])
        conf["private_key"] = pk.replace("\\n", "\n")
        return conf
    if "private_key_b64" in conf and conf["private_key_b64"]:
        import base64
        decoded = base64.b64decode(conf["private_key_b64"]).decode("utf-8")
        conf["private_key"] = decoded.replace("\\n", "\n")
        return conf
    raise RuntimeError("Secrets thiếu private_key hoặc private_key_b64.")


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


def init_session_state():
    # Bảng tạm cho luồng CSV + form
    st.session_state.setdefault("kpi_rows", [])
    # State form thủ công
    defaults = {
        'ten_kpi':'', 'dvt':'', 'ke_hoach':0.0, 'thuc_hien':0.0, 'trong_so':0.0,
        'bo_phan':'Tổ Kinh doanh tổng hợp', 'thang':datetime.now().month, 'nam':datetime.now().year
    }
    for k, v in defaults.items():
        st.session_state.setdefault(k, v)
    # Google flags
    st.session_state.setdefault("connected", False)
    st.session_state.setdefault("connect_msg", "")
    # ✅ LƯU TRẠNG THÁI TÍCH CHỌN HÀNG TRONG BẢNG TẠM (để không bị mất khi rerun)
    # key: __row_id → bool
    st.session_state.setdefault("temp_selected", {})


# =============================
# SIDEBAR & HEADER
# =============================
init_session_state()

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

_inject_ui_enhancements()
st.markdown(
    """
<div class="title-card">
  <h1><span class="title-icon">⚡</span><span class="title-text">KPI Đội quản lý Điện lực khu vực Định Hóa</span></h1>
  <p class="subtitle">Luồng chuẩn: Upload CSV → thêm vào Bảng tạm → chọn dòng → tự nạp lên Form nhập → tính điểm ngay.</p>
</div>
""",
    unsafe_allow_html=True,
)

# =============================
# 3) NHẬP THỦ CÔNG KPI & XUẤT EXCEL (9 CỘT)
#    + NẠP CSV VÀO BẢNG TẠM & CHỌN DÒNG ĐỂ NẠP LÊN FORM
# =============================
st.markdown("---")
st.markdown('<h2 class="section-title">3) Nhập thủ công KPI & Xuất Excel (9 cột)</h2>', unsafe_allow_html=True)

# ---- 3.a) UPLOAD CSV → ĐỔ VÀO BẢNG TẠM ----
with st.expander("⬆️ Nạp CSV vào 'Bảng tạm'", expanded=False):
    up_csv = st.file_uploader(
        "Tải file .csv (các cột gợi ý: 'Tên chỉ tiêu (KPI)', 'Đơn vị tính', 'Kế hoạch (tháng)', 'Trọng số', 'Bộ phận/người phụ trách', 'Tháng', 'Năm', 'Thực hiện (tháng)')",
        type=["csv"],
        key="csv_to_temp",
    )
    if up_csv is not None:
        try:
            df_csv = pd.read_csv(up_csv)
            # Ánh xạ linh hoạt → 9 cột chuẩn của bảng tạm
            def _map_row(r):
                name = r.get('Tên chỉ tiêu (KPI)', r.get('Ten KPI', ''))
                dvt = r.get('Đơn vị tính', r.get('Don vi tinh', ''))
                plan = r.get('Kế hoạch (tháng)', r.get('Kế hoạch', r.get('Ke hoach', 0)))
                actual = r.get('Thực hiện (tháng)', r.get('Thực hiện', r.get('Thuc hien', 0)))
                weight = r.get('Trọng số', r.get('Trong so', 0))
                dept = r.get('Bộ phận/người phụ trách', r.get('Bo phan', ''))
                month = r.get('Tháng', datetime.now().month)
                year = r.get('Năm', datetime.now().year)
                score = compute_kpi_score_dynamic(name, actual, plan, weight)
                return {
                    "Tên chỉ tiêu (KPI)": str(name or "").strip(),
                    "Đơn vị tính": str(dvt or "").strip(),
                    "Kế hoạch": _safe_number(plan, 0),
                    "Thực hiện": _safe_number(actual, 0),
                    "Trọng số": _safe_number(weight, 0),
                    "Bộ phận/người phụ trách": str(dept or "").strip(),
                    "Tháng": int(_safe_number(month, datetime.now().month)),
                    "Năm": int(_safe_number(year, datetime.now().year)),
                    "Điểm KPI": score,
                }
            added = [_map_row(r) for _, r in df_csv.fillna("").iterrows()]
            st.session_state.kpi_rows.extend(added)
            st.success(f"Đã thêm {len(added)} dòng vào Bảng tạm.")
        except Exception as e:
            st.error(f"Không đọc được CSV: {e}")

# ---- 3.b) FORM THỦ CÔNG (gắn với session_state để nạp từ bảng tạm) ----
with st.form("kpi_input_form", clear_on_submit=False):
    c1, c2, c3 = st.columns([1.2, 1.2, 1])
    with c1:
        st.text_input("1) Tên chỉ tiêu (KPI)", key='ten_kpi')
        st.text_input("2) Đơn vị tính", key='dvt')
        st.number_input("3) Kế hoạch", min_value=0.0, step=0.1, format="%.4f", key='ke_hoach')
    with c2:
        st.number_input("4) Thực hiện", min_value=0.0, step=0.1, format="%.4f", key='thuc_hien')
        st.number_input("5) Trọng số", min_value=0.0, step=0.1, format="%.4f", key='trong_so')
    bo_phan_list = [
        "Tổ Kế hoạch kỹ thuật",
        "Tổ Kinh doanh tổng hợp",
        "Tổ Quản lý tổng hợp 1",
        "Tổ Quản lý tổng hợp 2",
        "Tổ Trực vận hành",
        "Tổ Kiểm tra giám sát mua bán điện",
    ]
    st.selectbox("6) Bộ phận/người phụ trách", bo_phan_list, index=bo_phan_list.index(st.session_state.get('bo_phan', bo_phan_list[1])) if st.session_state.get('bo_phan', None) in bo_phan_list else 1, key='bo_phan')
    with c3:
        st.selectbox("7) Tháng", list(range(1,13)), index=(st.session_state.get('thang', datetime.now().month)-1), key='thang')
        st.number_input("8) Năm", min_value=2000, max_value=2100, value=st.session_state.get('nam', datetime.now().year), step=1, key='nam')
        st.metric("9) Điểm KPI (xem trước)", compute_kpi_score_dynamic(st.session_state['ten_kpi'], st.session_state['thuc_hien'], st.session_state['ke_hoach'], st.session_state['trong_so']))

    if st.form_submit_button("➕ Thêm vào bảng tạm"):
        row = {
            "Tên chỉ tiêu (KPI)": st.session_state['ten_kpi'].strip(),
            "Đơn vị tính": st.session_state['dvt'].strip(),
            "Kế hoạch": _safe_number(st.session_state['ke_hoach'], 0.0),
            "Thực hiện": _safe_number(st.session_state['thuc_hien'], 0.0),
            "Trọng số": _safe_number(st.session_state['trong_so'], 0.0),
            "Bộ phận/người phụ trách": st.session_state['bo_phan'].strip(),
            "Tháng": int(st.session_state['thang']),
            "Năm": int(st.session_state['nam']),
            "Điểm KPI": compute_kpi_score_dynamic(st.session_state['ten_kpi'], st.session_state['thuc_hien'], st.session_state['ke_hoach'], st.session_state['trong_so']),
        }
        st.session_state.kpi_rows.append(row)
        st.success("Đã thêm 1 dòng KPI vào bảng tạm.")

# ---- 3.c) BẢNG TẠM: CHỌN DÒNG → NẠP LÊN FORM & XUẤT EXCEL ----
# Tạo DataFrame từ bảng tạm
df_manual = pd.DataFrame(st.session_state.kpi_rows, columns=EXPECTED_KPI_COLS) if st.session_state.kpi_rows else pd.DataFrame(columns=EXPECTED_KPI_COLS)

st.markdown("**Bảng tạm (tick cột *Chọn* rồi nhấn ▶ Nạp dòng đã chọn lên Form):**")

# ✅ Tạo __row_id ổn định để lưu trạng thái chọn
if not df_manual.empty:
    df_manual["__row_id"] = (
        df_manual["Tên chỉ tiêu (KPI)"].astype(str).fillna("") + "|" +
        df_manual["Đơn vị tính"].astype(str).fillna("") + "|" +
        df_manual["Bộ phận/người phụ trách"].astype(str).fillna("") + "|" +
        df_manual["Tháng"].astype(str).fillna("") + "|" +
        df_manual["Năm"].astype(str).fillna("")
    )
else:
    df_manual["__row_id"] = []

# ✅ Dựng cột "Chọn" từ state, để tick không bị mất khi app rerun
sel_map = st.session_state.get("temp_selected", {})
chons = [bool(sel_map.get(i, False)) for i in df_manual["__row_id"].tolist()]

# Tạo DataFrame hiển thị, sử dụng list of booleans cho cột 'Chọn'
df_display = df_manual.copy()
df_display.insert(0, "Chọn", chons)

# Cấu hình cột: chỉ cho phép tick "Chọn", các cột còn lại khóa lại
colcfg = {
    "Chọn": st.column_config.CheckboxColumn(
        "Chọn",
        help="Đánh dấu một dòng để nạp lên Form",
    ),
    "Tên chỉ tiêu (KPI)": st.column_config.TextColumn(disabled=True),
    "Đơn vị tính": st.column_config.TextColumn(disabled=True),
    "Kế hoạch": st.column_config.NumberColumn(disabled=True),
    "Thực hiện": st.column_config.NumberColumn(disabled=True),
    "Trọng số": st.column_config.NumberColumn(disabled=True),
    "Bộ phận/người phụ trách": st.column_config.TextColumn(disabled=True),
    "Tháng": st.column_config.NumberColumn(disabled=True),
    "Năm": st.column_config.NumberColumn(disabled=True),
    "Điểm KPI": st.column_config.NumberColumn(format="%.4f", disabled=True),
}

edited_temp = st.data_editor(
    df_display,
    key="temp_table_editor",
    use_container_width=True,
    hide_index=True,
    column_config=colcfg,
    num_rows="fixed",
)

# ✅ SỬA LỖI: CẬP NHẬT LẠI state lựa chọn từ kết quả edited_temp một cách cẩn thận
if not edited_temp.empty:
    try:
        # Lấy danh sách ID hiện tại
        current_ids = edited_temp["__row_id"].tolist()
        # Tạo một dictionary mới để lưu trạng thái
        new_sel_map = {}
        for _id, _chon in zip(current_ids, edited_temp["Chọn"].tolist()):
            new_sel_map[_id] = bool(_chon)
        # Thay thế toàn bộ state cũ bằng state mới
        st.session_state.temp_selected = new_sel_map
    except Exception:
        # Trong trường hợp có lỗi, giữ lại trạng thái cũ
        pass

colSel1, colSel2, colSel3 = st.columns([1,1,2])
with colSel1:
    if st.button("▶ Nạp dòng đã chọn lên Form"):
        # Lấy danh sách id đã chọn từ state (bền vững)
        selected_ids = [k for k, v in st.session_state.temp_selected.items() if v]
        if not selected_ids:
            st.warning("Chưa chọn dòng nào (tick vào cột 'Chọn').")
        else:
            # Ưu tiên dòng cuối cùng vừa tick
            sel_id = selected_ids[-1]
            r = df_manual[df_manual["__row_id"] == sel_id].iloc[0]
            # Gán lên form
            st.session_state['ten_kpi'] = str(r["Tên chỉ tiêu (KPI)"])
            st.session_state['dvt'] = str(r["Đơn vị tính"]) or ""
            st.session_state['ke_hoach'] = float(_safe_number(r["Kế hoạch"], 0))
            st.session_state['thuc_hien'] = float(_safe_number(r["Thực hiện"], 0))
            st.session_state['trong_so'] = float(_safe_number(r["Trọng số"], 0))
            st.session_state['bo_phan'] = str(r["Bộ phận/người phụ trách"]) or "Tổ Kinh doanh tổng hợp"
            st.session_state['thang'] = int(_safe_number(r["Tháng"], datetime.now().month))
            st.session_state['nam'] = int(_safe_number(r["Năm"], datetime.now().year))
            st.success("Đã nạp dòng đã chọn lên Form. Anh chỉnh 'Thực hiện' để ra điểm KPI.")
with colSel2:
    if st.button("🗑️ Xóa dòng tick chọn"):
        selected_ids = [k for k, v in st.session_state.temp_selected.items() if v]
        if not selected_ids:
            st.info("Chưa tick chọn dòng nào.")
        else:
            # Xóa khỏi bảng tạm theo __row_id
            keep_mask = ~df_manual["__row_id"].isin(selected_ids)
            st.session_state.kpi_rows = df_manual[keep_mask].drop(columns=["__row_id"]).to_dict(orient="records")
            # Xóa trạng thái chọn tương ứng
            for k in selected_ids:
                st.session_state.temp_selected.pop(k, None)
            st.success(f"Đã xóa {len(selected_ids)} dòng khỏi Bảng tạm.")
with colSel3:
    if st.button("💾 Xuất Excel (Bảng tạm)"):
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

# =============================
# 4) NẠP FILE CHUẨN 1 THÁNG → AUTO-SCORE TRỰC TIẾP TRONG LƯỚI (TÙY CHỌN)
# =============================
# Phần này giữ lại khả năng nạp file KPI_Input (Excel/CSV) và tính điểm tự động ngay trong bảng,
# giúp anh xử lý nhanh một tháng dữ liệu độc lập (không đụng đến bảng tạm ở trên).

def _norm_text(s: str) -> str:
    if not isinstance(s, str):
        s = str(s or "")
    s = unicodedata.normalize("NFKD", s)
    s = "".join(ch for ch in s if not unicodedata.combining(ch)).lower()
    s = " ".join(s.split())
    return s

@st.cache_data(show_spinner=False)
def _load_kpi_input_from_xlsx(b: bytes) -> pd.DataFrame:
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


def _autoscore_row_onemonth(row: pd.Series) -> float:
    name = row.get("Tên chỉ tiêu (KPI)", "")
    method = row.get("Phương pháp đo kết quả", "")
    plan = row.get("Kế hoạch (tháng)")
    actual = row.get("Thực hiện (tháng)")
    try:
        plan = float(plan); actual = float(actual)
    except Exception:
        return row.get("Điểm KPI", None)
    txt = _norm_text(f"{name} {method}")
    if "du bao tong thuong pham" in txt:
        ts = row.get("Trọng số", 3)
        return _kpi_sai_so_du_bao_diem((actual - plan) / plan * 100.0, ts)
    ts = row.get("Trọng số", 0)
    return compute_kpi_score(actual, plan, ts)


def _autoscore_dataframe_onemonth(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return df
    out = df.copy()
    out["Điểm KPI"] = out.apply(_autoscore_row_onemonth, axis=1)
    return out

st.markdown("---")
st.markdown('<h2 class="section-title">4) Nạp file chuẩn 1 tháng → Nhập "Thực hiện (tháng)" → Tự tính điểm</h2>', unsafe_allow_html=True)

mode = st.radio(
    "Nguồn file 1 tháng",
    ["Tải Excel (.xlsx)", "Tải CSV (.csv)"],
    horizontal=True,
)

mon_df = pd.DataFrame()
if mode == "Tải Excel (.xlsx)":
    up = st.file_uploader("Tải file Excel KPI_Input.xlsx (sheet KPI_Input)", type=["xlsx"], key="one_xlsx")
    if up is not None:
        try:
            mon_df = _load_kpi_input_from_xlsx(up.read())
        except Exception as e:
            st.error(f"Lỗi Excel: {e}")
elif mode == "Tải CSV (.csv)":
    upc = st.file_uploader("Tải file CSV (cấu trúc như KPI_Input)", type=["csv"], key="one_csv")
    if upc is not None:
        try:
            mon_df = pd.read_csv(upc)
        except Exception as e:
            st.error(f"Lỗi CSV: {e}")

if mon_df is None or mon_df.empty:
    st.info("⚠️ Chưa có dữ liệu hợp lệ cho mục 1 tháng.")
else:
    # Chuẩn hóa kiểu
    for _col in ["Kế hoạch (tháng)", "Thực hiện (tháng)", "Trọng số", "Điểm KPI", "Tháng", "Năm"]:
        if _col in mon_df.columns:
            mon_df[_col] = pd.to_numeric(mon_df[_col], errors="coerce")

    # Chọn tháng/năm
    colM, colY = st.columns(2)
    with colM:
        month_default = int(mon_df["Tháng"].dropna().astype(int).iloc[0]) if "Tháng" in mon_df.columns and len(mon_df)>0 else datetime.now().month
        chosen_month = st.number_input("Tháng", min_value=1, max_value=12, value=month_default, step=1)
    with colY:
        year_default = int(mon_df["Năm"].dropna().astype(int).iloc[0]) if "Năm" in mon_df.columns and len(mon_df)>0 else datetime.now().year
        chosen_year = st.number_input("Năm", min_value=2000, max_value=2100, value=year_default, step=1)

    base = mon_df[(mon_df["Tháng"].astype(int) == int(chosen_month)) & (mon_df["Năm"].astype(int) == int(chosen_year))].copy()

    with st.expander("🔎 Tìm nhanh theo Phương pháp/Tên KPI/Bộ phận"):
        q = st.text_input("Từ khóa", value="")
        col1, col2 = st.columns(2)
        with col1:
            departments = [x for x in sorted(base["Bộ phận/người phụ trách"].dropna().astype(str).unique().tolist()) if x]
            dept = st.multiselect("Bộ phận", departments, default=[])
        with col2:
            units = [x for x in sorted(base["Đơn vị tính"].dropna().astype(str).unique().tolist()) if x]
            unit = st.multiselect("Đơn vị tính", units, default=[])
        mask = pd.Series([True]*len(base))
        if q:
            qlow = q.lower()
            mask &= base.apply(lambda r: qlow in str(r["Phương pháp đo kết quả"]).lower()
                                           or qlow in str(r["Tên chỉ tiêu (KPI)"]).lower()
                                           or qlow in str(r["Bộ phận/người phụ trách"]).lower(), axis=1)
        if dept:
            mask &= base["Bộ phận/người phụ trách"].astype(str).isin(dept)
        if unit:
            mask &= base["Đơn vị tính"].astype(str).isin(unit)
        base = base[mask].copy()

    # Hiển thị lưới cho phép nhập và tính
    scored = _autoscore_dataframe_onemonth(base)
    edited = st.data_editor(
        scored,
        key=f"editor_onemonth_{chosen_year}_{chosen_month}",
        use_container_width=True,
        hide_index=True,
        column_config={
            "Thực hiện (tháng)": st.column_config.NumberColumn(format="%f"),
            "Trọng số": st.column_config.NumberColumn(format="%f"),
            "Điểm KPI": st.column_config.NumberColumn(format="%f", disabled=True),
        },
    )

    # Xuất Excel
    colL, colR = st.columns([1,1])
    with colL:
        if st.button("💾 Xuất Excel (.xlsx) – bảng 1 tháng"):
            output = BytesIO()
            with pd.ExcelWriter(output, engine="xlsxwriter") as writer:
                edited.to_excel(writer, index=False, sheet_name="KPI_Input")
                wb = writer.book
                ws = writer.sheets["KPI_Input"]
                fmt_header = wb.add_format({"bold": True, "bg_color": "#E2F0D9", "border": 1})
                fmt_cell = wb.add_format({"border": 1})
                ws.set_row(0, 22, fmt_header)
                for i, _ in enumerate(edited.columns):
                    ws.set_column(i, i, 22, fmt_cell)
            st.download_button(
                label="Tải về KPI_Input",
                data=output.getvalue(),
                file_name=f"KPI_Input_{int(chosen_year)}_{int(chosen_month):02d}.xlsx",
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            )
    with colR:
        st.caption("Bảng trên đã hiển thị điểm KPI trực tiếp – gọn giao diện.")

# =============================
# FOOTER
# =============================
st.caption("© BrownEyes – KPI Scorer (CSV → Bảng tạm → Nạp Form + Module 1 tháng).")
