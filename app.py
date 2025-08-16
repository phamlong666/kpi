from pathlib import Path

app_code = r'''# -*- coding: utf-8 -*-
"""
KPI App – Định Hóa (v3, UI + CSV + Drive)
- Form nhập tay đặt LÊN TRÊN (kèm nút Áp dụng / Ghi CSV / Làm mới CSV / Xuất báo cáo / Lưu dữ liệu).
- Các nhóm ô cùng hàng: (Đơn vị tính – Bộ phận phụ trách – Kế hoạch),
  (Thực hiện – Trọng số – Điểm KPI), (Tháng – Năm).
- "Điểm KPI" tính theo "Phương pháp đo kết quả" nếu có; mặc định theo ratio (Actual/Plan)*10*Weight.
- "Làm mới bảng CSV" có xác nhận, tránh mất dữ liệu ngoài ý muốn.
- "Xuất báo cáo": tải Excel & PDF.
- "Lưu dữ liệu": tạo thư mục "Báo cáo KPI" trong thư mục con theo USE (KVDHA, KVDH, ...)
  dưới thư mục gốc App_KPI (có thể nhập ID), rồi lưu file "KPI_dd-mm-yy".
- Nhấn Enter để đăng nhập (st.form).
- BẮT BUỘC đăng nhập mới vào được nghiệp vụ.
"""
import re
import io
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
import random
import string
from datetime import datetime
import streamlit as st
import pandas as pd
import gspread
from google.oauth2.service_account import Credentials
import matplotlib.pyplot as plt

# ========== Thư viện Drive (có thể cần thêm vào requirements: google-api-python-client) ==========
try:
    from googleapiclient.discovery import build as gbuild
    from googleapiclient.http import MediaIoBaseUpload
except Exception:  # nếu thiếu thư viện, ta sẽ cảnh báo khi dùng
    gbuild = None
    MediaIoBaseUpload = None

# ================= CẤU HÌNH =================
st.set_page_config(page_title="KPI – Định Hóa", layout="wide")
APP_TITLE = "📊 KPI – Đội quản lý Điện lực khu vực Định Hóa"
GOOGLE_SHEET_ID_DEFAULT = "1nXFKJrn8oHwQgUzv5QYihoazYRhhS1PeN-xyo7Er2iM"
KPI_SHEET_DEFAULT = "KPI"
ADMIN_ACCOUNTS = {r"pctn\\admin", r"npc\\longph"}
FORGOT_TARGET_EMAIL = "phamlong666@gmail.com"

# Thư mục gốc App_KPI trên Drive (có thể thay bằng ID của anh)
APP_KPI_DRIVE_ROOT_ID_DEFAULT = "1rE3E8CuPViw8-VYWYZgeB4Mz9WEY3e7"

if "spreadsheet_id" not in st.session_state:
    st.session_state["spreadsheet_id"] = GOOGLE_SHEET_ID_DEFAULT
if "kpi_sheet_name" not in st.session_state:
    st.session_state["kpi_sheet_name"] = KPI_SHEET_DEFAULT
if "drive_root_id" not in st.session_state:
    st.session_state["drive_root_id"] = APP_KPI_DRIVE_ROOT_ID_DEFAULT

# ================= TIỆN ÍCH =================
def toast(msg, icon="ℹ️"):
    try:
        st.toast(msg, icon=icon)
    except Exception:
        pass

def is_admin(username: str) -> bool:
    return bool(username) and username.strip().lower() in ADMIN_ACCOUNTS

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
                svc["private_key"].replace("\\r\\n", "\\n")
                                  .replace("\\r", "\\n")
                                  .replace("\\\\n", "\\n")
            )
        scopes = ["https://www.googleapis.com/auth/spreadsheets",
                  "https://www.googleapis.com/auth/drive"]
        creds = Credentials.from_service_account_info(svc, scopes=scopes)
        return gspread.authorize(creds), creds
    except Exception as e:
        st.session_state["_gs_error"] = f"SECRETS_ERROR: {e}"
        return None, None

def open_spreadsheet(sid_or_url: str):
    sid = extract_sheet_id(sid_or_url or GOOGLE_SHEET_ID_DEFAULT) or GOOGLE_SHEET_ID_DEFAULT
    client, _ = st.session_state.get("_gs_pair", (None, None))
    if client is None:
        client, creds = get_gs_client()
        st.session_state["_gs_pair"] = (client, creds)
    if client is None:
        raise RuntimeError("no_client")
    return client.open_by_key(sid)

def df_from_ws(ws) -> pd.DataFrame:
    records = ws.get_all_records(expected_headers=ws.row_values(1))
    return pd.DataFrame(records)

# Chuẩn hoá tên cột
ALIAS = {
    "USE (mã đăng nhập)": [
        "USE (mã đăng nhập)", r"Tài khoản (USE\\username)", "Tài khoản (USE/username)",
        "Tài khoản", "Username", "USE", "User"
    ],
    "Mật khẩu mặc định": [
        "Mật khẩu mặc định","Password mặc định","Password","Mật khẩu","Mat khau mac dinh"
    ],
    # KPI
    "Tên chỉ tiêu (KPI)": ["Tên chỉ tiêu (KPI)","Tên KPI","Chỉ tiêu"],
    "Đơn vị tính": ["Đơn vị tính","Unit"],
    "Kế hoạch": ["Kế hoạch","Plan","Target", "Kế hoạch (tháng)"],
    "Thực hiện": ["Thực hiện","Thực hiện (tháng)","Actual (month)"],
    "Trọng số": ["Trọng số","Weight"],
    "Bộ phận/người phụ trách": ["Bộ phận/người phụ trách","Phụ trách"],
    "Tháng": ["Tháng","Month"],
    "Năm": ["Năm","Year"],
    "Điểm KPI": ["Điểm KPI","Score"],
    "Ghi chú": ["Ghi chú","Notes"],
    "Tên đơn vị": ["Tên đơn vị","Đơn vị"],
    "Phương pháp đo kết quả": ["Phương pháp đo kết quả","Cách tính","Công thức"],
    "Ngưỡng dưới": ["Ngưỡng dưới","Min"],
    "Ngưỡng trên": ["Ngưỡng trên","Max"],
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
    # Chuẩn: "Thực hiện (tháng)" -> "Thực hiện"; "Kế hoạch (tháng)" -> "Kế hoạch"
    if "Thực hiện (tháng)" in df.columns and "Thực hiện" not in df.columns:
        df = df.rename(columns={"Thực hiện (tháng)":"Thực hiện"})
    if "Kế hoạch (tháng)" in df.columns and "Kế hoạch" not in df.columns:
        df = df.rename(columns={"Kế hoạch (tháng)":"Kế hoạch"})
    return df

def find_use_ws_and_cols(sh):
    try:
        ws = sh.worksheet("USE")
    except Exception:
        ws = None
        for w in sh.worksheets():
            try:
                headers = [h.strip() for h in w.row_values(1)]
            except Exception:
                continue
            if (("USE (mã đăng nhập)" in headers) or ("Tài khoản (USE\\username)" in headers) or
                ("Tài khoản" in headers) or ("Username" in headers) or ("USE" in headers)) and \
               ("Mật khẩu mặc định" in headers or "Password" in headers or "Mật khẩu" in headers):
                ws = w; break
        if ws is None:
            raise gspread.exceptions.WorksheetNotFound("NO_USE_TAB")
    headers = [h.strip() for h in ws.row_values(1)]
    def find_idx(names):
        for name in names:
            if name in headers: return headers.index(name)+1
        return None
    idx_use = find_idx(["USE (mã đăng nhập)", "Tài khoản (USE\\username)", "Tài khoản", "Username", "USE"])
    idx_pwd = find_idx(["Mật khẩu mặc định","Password","Mật khẩu"])
    if not idx_use or not idx_pwd:
        raise RuntimeError("MISSING_USE_OR_PASS_COL")
    return ws, idx_use, idx_pwd, headers

def load_users(spreadsheet_id_or_url: str = "") -> pd.DataFrame:
    sid = extract_sheet_id(spreadsheet_id_or_url) or GOOGLE_SHEET_ID_DEFAULT
    client, _ = get_gs_client()
    if client is not None and sid:
        try:
            sh = client.open_by_key(sid)
            ws, _, _, _ = find_use_ws_and_cols(sh)
            return df_from_ws(ws)
        except Exception as e:
            st.session_state["_gs_error"] = f"OPEN_ERROR: {e}"
    return pd.DataFrame()

def check_credentials(df: pd.DataFrame, use_input: str, pwd_input: str) -> bool:
    if df is None or df.empty:
        st.error("Chưa tải được danh sách người dùng (USE).")
        return False
    df = normalize_columns(df)
    col_use = next((c for c in df.columns if c.strip().lower() in [
        "tài khoản (use\\username)".lower(), "tài khoản".lower(), "username".lower(), "use (mã đăng nhập)".lower(), "use"
    ]), None)
    col_pw = next((c for c in df.columns if c.strip().lower() in [
        "mật khẩu mặc định".lower(),"password mặc định".lower(),"password".lower(),"mật khẩu".lower()
    ]), None)
    if not col_use or not col_pw:
        st.error("Thiếu cột USE hoặc Mật khẩu trong bảng USE.")
        return False
    u = (use_input or "").strip(); p = (pwd_input or "").strip()
    row = df.loc[df[col_use].astype(str).str.strip().str.lower() == u.lower()]
    if row.empty or str(row.iloc[0][col_pw]).strip() != p:
        st.error("USE hoặc mật khẩu không đúng")
        return False
    return True

def compute_score_generic(plan, actual, weight):
    if plan in (None, 0) or actual is None: 
        return None
    ratio = max(min(actual/plan, 2.0), 0.0)
    w = weight/100.0 if weight and weight>1 else (weight or 0.0)
    return round(ratio*10*w, 2)

def parse_float(x):
    try: return float(str(x).replace(",", "."))
    except: return None

def compute_score_with_method(row):
    plan = parse_float(row.get("Kế hoạch"))
    actual = parse_float(row.get("Thực hiện"))
    weight = parse_float(row.get("Trọng số")) or 0.0
    method = str(row.get("Phương pháp đo kết quả") or "").strip().lower()

    if not method:
        return compute_score_generic(plan, actual, weight)
    if plan in (None, 0) or actual is None:
        return None

    w = weight/100.0 if weight and weight>1 else (weight or 0.0)

    # Các cách tính điển hình
    if any(k in method for k in ["tăng", ">=", "cao hơn tốt", "increase", "higher"]):
        ratio = max(min(actual/plan, 2.0), 0.0)
        return round(ratio*10*w, 2)

    if any(k in method for k in ["giảm", "<=", "thấp hơn tốt", "decrease", "lower"]):
        if actual <= plan: ratio = 1.0
        else: ratio = max(min(plan/actual, 2.0), 0.0)
        return round(ratio*10*w, 2)

    if any(k in method for k in ["đạt", "dat", "bool", "pass/fail"]):
        ok = actual >= plan
        return round((10.0 if ok else 0.0)*w, 2)

    if any(k in method for k in ["khoảng", "range", "trong khoảng"]):
        lo = parse_float(row.get("Ngưỡng dưới"))
        hi = parse_float(row.get("Ngưỡng trên"))
        if lo is None or hi is None:
            return compute_score_generic(plan, actual, weight)
        ok = (lo <= actual <= hi)
        return round((10.0 if ok else 0.0)*w, 2)

    # Mặc định
    return compute_score_generic(plan, actual, weight)

def generate_pdf_from_df(df: pd.DataFrame, title: str = "BÁO CÁO KPI") -> bytes:
    # Tạo PDF đơn giản bằng reportlab (cần gói reportlab trong requirements)
    try:
        from reportlab.lib.pagesizes import A4, landscape
        from reportlab.pdfgen import canvas
        from reportlab.lib import colors
        from reportlab.lib.units import cm
        from reportlab.platypus import SimpleDocTemplate, Table, TableStyle, Paragraph, Spacer
        from reportlab.lib.styles import getSampleStyleSheet

        buf = io.BytesIO()
        doc = SimpleDocTemplate(buf, pagesize=landscape(A4), rightMargin=20, leftMargin=20, topMargin=20, bottomMargin=20)
        styles = getSampleStyleSheet()
        story = [Paragraph(title, styles["Title"]), Spacer(1, 0.3*cm)]

        cols = list(df.columns)
        data = [cols] + df.fillna("").astype(str).values.tolist()
        t = Table(data, repeatRows=1)
        t.setStyle(TableStyle([
            ("BACKGROUND", (0,0), (-1,0), colors.lightgrey),
            ("GRID", (0,0), (-1,-1), 0.25, colors.grey),
            ("FONTSIZE", (0,0), (-1,-1), 8),
            ("ALIGN", (0,0), (-1,-1), "CENTER"),
        ]))
        story.append(t)
        doc.build(story)
        return buf.getvalue()
    except Exception as e:
        st.warning("Thiếu gói reportlab để xuất PDF. Vui lòng thêm 'reportlab' vào requirements.")
        return b""

# ================= Drive helpers =================
def get_drive_service():
    if gbuild is None:
        st.error("Thiếu thư viện 'google-api-python-client' để lưu lên Google Drive.")
        return None
    _client, creds = st.session_state.get("_gs_pair", (None, None))
    if creds is None:
        _, creds = get_gs_client()
        st.session_state["_gs_pair"] = (st.session_state.get("_gs_pair", (None,None))[0], creds)
    if creds is None:
        return None
    return gbuild("drive", "v3", credentials=creds)

def ensure_folder(service, parent_id: str, name: str) -> str:
    q = f"mimeType='application/vnd.google-apps.folder' and name='{name}' and '{parent_id}' in parents and trashed=false"
    res = service.files().list(q=q, spaces="drive", fields="files(id,name)").execute()
    items = res.get("files", [])
    if items:
        return items[0]["id"]
    file_metadata = {"name": name, "mimeType": "application/vnd.google-apps.folder", "parents": [parent_id]}
    folder = service.files().create(body=file_metadata, fields="id").execute()
    return folder["id"]

def upload_bytes(service, parent_id: str, filename: str, data: bytes, mime: str) -> str:
    media = MediaIoBaseUpload(io.BytesIO(data), mimetype=mime, resumable=False)
    file_metadata = {"name": filename, "parents": [parent_id]}
    f = service.files().create(body=file_metadata, media_body=media, fields="id").execute()
    return f["id"]

# ================= SIDEBAR (LOGIN/LOGOUT + Drive root) =================
with st.sidebar:
    st.header("🔒 Đăng nhập")

    if "_user" not in st.session_state:
        with st.form("login_form", clear_on_submit=False):
            use_input = st.text_input("USE (vd: PCTN\\KVDHA)", key="login_use")
            pwd_input = st.text_input("Mật khẩu", type="password", key="login_pwd")
            login_submit = st.form_submit_button("Đăng nhập", use_container_width=True)
        if login_submit:
            df_users = load_users(st.session_state.get("spreadsheet_id",""))
            if check_credentials(df_users, use_input, pwd_input):
                st.session_state["_user"] = use_input
                toast("Đăng nhập thành công.", "✅")
                st.rerun()
    else:
        st.success(f"Đang đăng nhập: **{st.session_state['_user']}**")
        st.text_input("ID thư mục gốc App_KPI (Drive)", key="drive_root_id")
        if st.button("Đăng xuất", use_container_width=True):
            st.session_state.pop("_user", None); toast("Đã đăng xuất.", "✅"); st.rerun()

# ================= GATING CHÍNH =================
st.title(APP_TITLE)
if "_user" not in st.session_state:
    st.info("Vui lòng đăng nhập để làm việc.")
    st.stop()

# ================= KPI CORE =================
KPI_COLS = ["Tên chỉ tiêu (KPI)","Đơn vị tính","Kế hoạch","Thực hiện","Trọng số",
            "Bộ phận/người phụ trách","Tháng","Năm","Phương pháp đo kết quả",
            "Ngưỡng dưới","Ngưỡng trên","Điểm KPI","Ghi chú","Tên đơn vị"]

def read_kpi_from_sheet(sh, sheet_name: str):
    try:
        ws = sh.worksheet(sheet_name)
    except Exception:
        return pd.DataFrame()
    df = normalize_columns(df_from_ws(ws))
    if "Điểm KPI" not in df.columns:
        df["Điểm KPI"] = df.apply(compute_score_with_method, axis=1)
    return df

def write_kpi_to_sheet(sh, sheet_name: str, df: pd.DataFrame):
    df = normalize_columns(df.copy())
    if "Điểm KPI" not in df.columns:
        df["Điểm KPI"] = df.apply(compute_score_with_method, axis=1)
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

def get_sheet_and_name():
    sid_cfg = st.session_state.get("spreadsheet_id","") or GOOGLE_SHEET_ID_DEFAULT
    sheet_name = st.session_state.get("kpi_sheet_name", KPI_SHEET_DEFAULT)
    sh = open_spreadsheet(sid_cfg)
    return sh, sheet_name

# ================= TAB CSV (kèm form trên) =================
st.subheader("⬆️ Nhập CSV vào KPI")

# Khu vực FORM NHẬP TAY nằm TRÊN
with st.container(border=True):
    st.markdown("#### ✍️ Biểu mẫu nhập tay (áp dụng cho dòng đang chọn hoặc thêm mới)")
    # Lấy danh sách cột chuẩn
    def _get_form_state():
        if "_csv_form" not in st.session_state:
            st.session_state["_csv_form"] = {
                "Tên chỉ tiêu (KPI)": "", "Đơn vị tính": "", "Kế hoạch": 0.0,
                "Thực hiện": 0.0, "Trọng số": 0.0, "Bộ phận/người phụ trách": "",
                "Tháng": str(datetime.now().month), "Năm": str(datetime.now().year),
                "Phương pháp đo kết quả": "Tăng tốt hơn", "Ngưỡng dưới":"", "Ngưỡng trên":"",
                "Ghi chú":"", "Tên đơn vị": ""
            }
        return st.session_state["_csv_form"]
    f = _get_form_state()

    c0 = st.columns([2,1,1,1])  # Tên chỉ tiêu dài hơn
    with c0[0]:
        f["Tên chỉ tiêu (KPI)"] = st.text_input("Tên chỉ tiêu (KPI)", value=f["Tên chỉ tiêu (KPI)"])
    with c0[1]:
        f["Đơn vị tính"] = st.text_input("Đơn vị tính", value=f["Đơn vị tính"])
    with c0[2]:
        f["Bộ phận/người phụ trách"] = st.text_input("Bộ phận/người phụ trách", value=f["Bộ phận/người phụ trách"])
    with c0[3]:
        f["Tên đơn vị"] = st.text_input("Tên đơn vị", value=f["Tên đơn vị"])

    c1 = st.columns(3)  # Cùng 1 hàng: Kế hoạch - Thực hiện - Trọng số
    with c1[0]:
        f["Kế hoạch"] = st.number_input("Kế hoạch", value=float(f.get("Kế hoạch") or 0.0))
    with c1[1]:
        f["Thực hiện"] = st.number_input("Thực hiện", value=float(f.get("Thực hiện") or 0.0))
    with c1[2]:
        f["Trọng số"] = st.number_input("Trọng số (%)", value=float(f.get("Trọng số") or 0.0))

    c2 = st.columns(3)  # Cùng 1 hàng: Phương pháp - Điểm KPI - Ghi chú
    with c2[0]:
        f["Phương pháp đo kết quả"] = st.selectbox("Phương pháp đo kết quả",
            options=["Tăng tốt hơn","Giảm tốt hơn","Đạt/Không đạt","Trong khoảng"],
            index=["Tăng tốt hơn","Giảm tốt hơn","Đạt/Không đạt","Trong khoảng"].index(f.get("Phương pháp đo kết quả","Tăng tốt hơn")) if f.get("Phương pháp đo kết quả") in ["Tăng tốt hơn","Giảm tốt hơn","Đạt/Không đạt","Trong khoảng"] else 0
        )
    with c2[1]:
        # Tính điểm động
        _tmp_row = {k:f.get(k) for k in f.keys()}
        _tmp_row["Điểm KPI"] = compute_score_with_method(_tmp_row)
        st.metric("Điểm KPI (tự tính)", _tmp_row["Điểm KPI"] if _tmp_row["Điểm KPI"] is not None else "—")
    with c2[2]:
        f["Ghi chú"] = st.text_input("Ghi chú", value=f["Ghi chú"])

    if f["Phương pháp đo kết quả"] == "Trong khoảng":
        c3 = st.columns(2)
        with c3[0]:
            f["Ngưỡng dưới"] = st.text_input("Ngưỡng dưới", value=str(f.get("Ngưỡng dưới") or ""))
        with c3[1]:
            f["Ngưỡng trên"] = st.text_input("Ngưỡng trên", value=str(f.get("Ngưỡng trên") or ""))

    c4 = st.columns(2)  # Cùng hàng: Tháng - Năm
    with c4[0]:
        f["Tháng"] = st.text_input("Tháng", value=str(f["Tháng"]))
    with c4[1]:
        f["Năm"] = st.text_input("Năm", value=str(f["Năm"]))

    c_btn = st.columns([1,1,1,1,2])
    apply_clicked = c_btn[0].button("Áp dụng vào bảng CSV tạm", type="primary", use_container_width=True)
    save_csv_clicked = c_btn[1].button("💾 Ghi CSV tạm vào sheet KPI", use_container_width=True)
    refresh_clicked = c_btn[2].button("🔁 Làm mới bảng CSV", use_container_width=True)
    export_clicked = c_btn[3].button("📤 Xuất báo cáo (Excel/PDF)", use_container_width=True)
    save_drive_clicked = c_btn[4].button("☁️ Lưu dữ liệu vào Google Drive", use_container_width=True)

# Tải CSV dưới FORM
up = st.file_uploader("Tải file CSV", type=["csv"])

# Khởi tạo cache CSV
if "_csv_cache" not in st.session_state:
    st.session_state["_csv_cache"] = pd.DataFrame(columns=KPI_COLS)

# Đọc CSV nếu có
if up is not None:
    try:
        df_csv = pd.read_csv(up)
    except Exception:
        up.seek(0)
        df_csv = pd.read_csv(up, encoding="utf-8-sig")
    df_csv = normalize_columns(df_csv)
    if "Điểm KPI" not in df_csv.columns:
        df_csv["Điểm KPI"] = df_csv.apply(compute_score_with_method, axis=1)
    st.session_state["_csv_cache"] = df_csv[KPI_COLS[:len(df_csv.columns)]] if all(c in df_csv.columns for c in KPI_COLS) else df_csv

# Bảng CSV có checkbox chọn dòng
df_show = st.session_state["_csv_cache"].copy()
if "✓ Chọn" not in df_show.columns:
    df_show.insert(0, "✓ Chọn", False)

st.write("Chọn tích một dòng để đưa dữ liệu lên biểu mẫu phía trên:")
df_edit = st.data_editor(
    df_show,
    use_container_width=True,
    hide_index=True,
    num_rows="dynamic",
    key="csv_editor"
)
st.session_state["_csv_cache"] = df_edit.drop(columns=["✓ Chọn"], errors="ignore")

# Khi chọn dòng → nạp lên form
selected_rows = df_edit[df_edit["✓ Chọn"] == True]
if not selected_rows.empty:
    row = selected_rows.iloc[0].drop(labels=["✓ Chọn"], errors="ignore").to_dict()
    for k in [c for c in KPI_COLS if c in row]:
        st.session_state["_csv_form"][k] = row.get(k, st.session_state["_csv_form"].get(k))

# XỬ LÝ NÚT: Áp dụng / Ghi CSV / Làm mới / Xuất / Lưu Drive
if apply_clicked:
    # cập nhật vào dòng đầu tiên được tích, nếu không có thì append
    base = st.session_state["_csv_cache"].copy()
    new_row = {c: st.session_state["_csv_form"].get(c, "") for c in KPI_COLS}
    # Tính điểm theo phương pháp
    new_row["Điểm KPI"] = compute_score_with_method(new_row)
    if not selected_rows.empty:
        idx = selected_rows.index[0]
        for k, v in new_row.items():
            base.loc[idx, k] = v
    else:
        base = pd.concat([base, pd.DataFrame([new_row])], ignore_index=True)
    st.session_state["_csv_cache"] = base
    toast("Đã áp dụng dữ liệu biểu mẫu vào CSV tạm.", "✅")
    st.rerun()

if save_csv_clicked:
    try:
        sh, sheet_name = get_sheet_and_name()
        ok = write_kpi_to_sheet(sh, sheet_name, st.session_state["_csv_cache"])
        if ok: toast("Đã ghi dữ liệu CSV vào sheet KPI.", "✅")
    except Exception as e:
        st.error(f"Lưu thất bại: {e}")

if refresh_clicked:
    # Xác nhận
    if "confirm_refresh" not in st.session_state:
        st.session_state["confirm_refresh"] = True
    else:
        st.session_state["confirm_refresh"] = not st.session_state["confirm_refresh"]

if st.session_state.get("confirm_refresh", False):
    with st.expander("❓ Bạn xác định làm mới dữ liệu chứ? (Sẽ mất những thay đổi chưa ghi)", expanded=True):
        colc = st.columns(2)
        if colc[0].button("Có, làm mới ngay", type="primary"):
            st.session_state["_csv_cache"] = pd.DataFrame(columns=KPI_COLS)
            st.session_state["confirm_refresh"] = False
            toast("Đã làm mới bảng CSV tạm.", "✅")
            st.rerun()
        if colc[1].button("Không, giữ nguyên dữ liệu"):
            st.session_state["confirm_refresh"] = False
            toast("Đã hủy làm mới.", "ℹ️")

if export_clicked:
    # Tạo Excel
    buf_xlsx = io.BytesIO()
    with pd.ExcelWriter(buf_xlsx, engine="xlsxwriter") as writer:
        st.session_state["_csv_cache"].to_excel(writer, index=False, sheet_name="KPI")
    st.download_button("⬇️ Tải Excel báo cáo", data=buf_xlsx.getvalue(), file_name="KPI_baocao.xlsx",
                       mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")
    # Tạo PDF
    pdf_bytes = generate_pdf_from_df(st.session_state["_csv_cache"], title="BÁO CÁO KPI")
    if pdf_bytes:
        st.download_button("⬇️ Tải PDF báo cáo", data=pdf_bytes, file_name="KPI_baocao.pdf", mime="application/pdf")

if save_drive_clicked:
    service = get_drive_service()
    if service is None:
        st.stop()
    try:
        # Xác định thư mục theo USE
        use_code = st.session_state["_user"].split("\\")[-1].upper()
        root_id = st.session_state.get("drive_root_id") or APP_KPI_DRIVE_ROOT_ID_DEFAULT
        folder_user = ensure_folder(service, root_id, use_code)
        folder_report = ensure_folder(service, folder_user, "Báo cáo KPI")

        ts = datetime.now().strftime("%d-%m-%y")
        fname_xlsx = f"KPI_{ts}.xlsx"
        fname_pdf  = f"KPI_{ts}.pdf"

        # Excel bytes
        buf_xlsx = io.BytesIO()
        with pd.ExcelWriter(buf_xlsx, engine="xlsxwriter") as writer:
            st.session_state["_csv_cache"].to_excel(writer, index=False, sheet_name="KPI")
        upload_bytes(service, folder_report, fname_xlsx, buf_xlsx.getvalue(),
                     "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")

        # PDF bytes
        pdf_bytes = generate_pdf_from_df(st.session_state["_csv_cache"], title=f"BÁO CÁO KPI – {use_code}")
        if pdf_bytes:
            upload_bytes(service, folder_report, fname_pdf, pdf_bytes, "application/pdf")

        toast(f"Đã lưu vào Google Drive /{use_code}/Báo cáo KPI/{fname_xlsx} & {fname_pdf}", "✅")
    except Exception as e:
        st.error(f"Lưu Google Drive thất bại: {e}")
'''

Path("/mnt/data/app.py").write_text(app_code, encoding="utf-8")
print("Wrote enhanced app.py (KB):", round(Path('/mnt/data/app.py').stat().st_size/1024,1))
