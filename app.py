from datetime import d
from io import B
import base64
import os
import pandas
import pandas as pd
import streamlit
import streamlit as st
import unicodedata

# -*- coding: utf-8 -*-
 as st
 as pd
atetime
ytesIO

# =============================
# CẤU HÌNH TRANG
# =============================
st.set_page_config(
    page_title="KPI Scorer – Định Hóa (Full Suite)
",
    page_icon="📊",
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
    for p in ["/mnt/data/logo.png", "./assets/logo.png", "./.streamlit/logo.png"]:
        if os.path.exists(p):
    env_logo = os.getenv("LOGO_URL")
    if env_logo:
        return f'<img class="floating-logo" src="{env_logo}" />', "env.LOGO_URL"
    if DEFAULT_LOGO_URL:
        return f'<img class="floating-logo" src="{DEFAULT_LOGO_URL}" />', "DEFAULT_LOGO_URL"
    return '<div class="floating-logo">📊</div>', "fallback"


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
    # ✅ LƯU TRẠNG THÁI BẢNG TẠM VỚI DF
    # Nếu DataFrame chưa tồn tại, tạo mới. Nếu đã tồn tại, dùng lại.
    if "temp_kpi_df" not in st.session_state:
        st.session_state.temp_kpi_df = pd.DataFrame(columns=["Chọn"] + EXPECTED_KPI_COLS)


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
# ---- 3.a) UPLOAD CSV → ĐỔ VÀO BẢNG TẠM ----
with st.expander("Nạp CSV vào 'Bảng tạm'", expanded=False):
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
            added_rows = [_map_row(r) for _, r in df_csv.fillna("").iterrows()]
            added_df = pd.DataFrame(added_rows, columns=EXPECTED_KPI_COLS)
            added_df.insert(0, "Chọn", False)
            st.session_state.temp_kpi_df = pd.concat([st.session_state.temp_kpi_df, added_df], ignore_index=True)
            st.success(f"Đã thêm {len(added_df)} dòng vào Bảng tạm.")
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
        new_row_data = {
            "Chọn": False,
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
        new_row_df = pd.DataFrame([new_row_data], columns=["Chọn"] + EXPECTED_KPI_COLS)
        st.session_state.temp_kpi_df = pd.concat([st.session_state.temp_kpi_df, new_row_df], ignore_index=True)
        st.success("Đã thêm 1 dòng KPI vào bảng tạm.")
df_tmp = st.session_state.get("temp_kpi_df", pd.DataFrame()).copy()
if df_tmp.empty:
    st.info("Bảng tạm chưa có dữ liệu.")
else:
    # Đảm bảo cột 'Chọn' tồn tại & là bool
    if "Chọn" not in df_tmp.columns:
        df_tmp.insert(0, "Chọn", False)
    def _to_bool(v):
        if isinstance(v, bool): return v
        if v is None: return False
        if isinstance(v, (int,float)): 
            try: return bool(int(v))
            except: return False
        if isinstance(v, str): 
            return v.strip().lower() in ("true","1","x","yes","y","checked")
        return False
    df_tmp["Chọn"] = df_tmp["Chọn"].map(_to_bool).fillna(False)

    # Lọc nhanh theo trạng thái chọn
    show_selected_only = st.toggle("🔎 Chỉ hiển thị các dòng đã chọn", value=False, key="kpi_oneclick_filter")
    view_df = df_tmp[df_tmp["Chọn"]] if show_selected_only else df_tmp

    # Hiển thị bảng đọc-only, thêm cột trạng thái ✅/⬜ để nhìn sướng mắt
    view_df_display = view_df.copy()
    view_df_display.insert(0, "✓", view_df_display["Chọn"].map(lambda x: "✅" if x else "⬜"))
    st.dataframe(
        view_df_display.drop(columns=["Chọn"], errors="ignore"),
        hide_index=True, use_container_width=True
    )

    # Checkbox độc lập từng dòng (CHẮC CHẮN tick được)
    with st.expander("🧩 Chọn dòng (One‑Click) — Không phụ thuộc Data Editor", expanded=not show_selected_only):
        for i, row in df_tmp.iterrows():
            label = f"#{i+1} – {row.get('Tên chỉ tiêu (KPI)', 'KPI')}"
            key = f"oc_sel_{i}"
            checked = bool(row["Chọn"])
            new_val = st.checkbox(label, key=key, value=checked)
            if new_val != checked:
                df_tmp.at[i, "Chọn"] = bool(new_val)

    # Ghi lại vào session
    st.session_state.temp_kpi_df = df_tmp
st.markdown(
    """
    <style>
    .big-kpi-title { 
        font-size: 36px !important; 
        font-weight: 800; 
        letter-spacing: 0.2px;
        line-height: 1.2;
        margin: 6px 0 2px 0;
        color: #0F1E49;
        text-shadow: 0 0 1px rgba(0,0,0,0.04);
    }
    /* hiệu ứng hover nhẹ cho checkbox list */
    div[data-testid="stVerticalBlock"] label:hover { 
        filter: brightness(1.05);
        transform: translateX(2px);
        transition: all .15s ease-in-out;
    }
    /* Logo tròn sang BÊN TRÁI */
    .floating-logo { 
        position: fixed; 
        left: 14px; top: 12px; 
        z-index: 1000; 
        width: 56px; height: 56px; object-fit: contain;
        box-shadow: 0 3px 12px rgba(0,0,0,0.10);
        border-radius: 50%;
        background: white;
        padding: 4px;
    }
    </style>
    """, 
    unsafe_allow_html=True
)
