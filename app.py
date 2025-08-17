# -*- coding: utf-8 -*-
"""
KPI App – Định Hóa (v3.8)
- Form NHẬP TAY ở TRÊN, bảng CSV ở DƯỚI.
- Bảng CSV có cột checkbox “✓ Chọn”, chọn 1 dòng để nạp ngược lên form (ổn định sau rerun).
- Phương pháp “Sai số ≤ ±1,5%; mỗi 0,1% vượt trừ 0,02 (max 3)” trả về **điểm trừ 0→3** (KHÔNG phải 10 - trừ).
  + Trên form đổi nhãn sang “Điểm trừ (tự tính)” khi dùng phương pháp này.
- Định dạng số kiểu VN: 1.000.000,00 ở các ô Kế hoạch/Thực hiện (nhập số tự động format).
- Ghi Google Sheets (fallback tên sheet nếu để trống), Xuất Excel (fallback openpyxl/xlsxwriter/CSV), PDF (nếu có reportlab),
  Lưu vào Google Drive (khuyên dùng Shared Drive; có supportsAllDrives).

Yêu cầu: khai báo service account trong st.secrets["gdrive_service_account"].
"""

import re
import io
from datetime import datetime

import pandas as pd
import streamlit as st
import gspread
from google.oauth2.service_account import Credentials

# Google Drive API (tùy môi trường có/không)
try:
    from googleapiclient.discovery import build as gbuild
    from googleapiclient.http import MediaIoBaseUpload
    from googleapiclient.errors import HttpError
except Exception:  # môi trường không có google-api-python-client
    gbuild = None
    MediaIoBaseUpload = None
    HttpError = Exception

# ------------------- CẤU HÌNH CƠ BẢN -------------------
st.set_page_config(page_title="KPI – Định Hóa", layout="wide")
APP_TITLE = "📊 KPI – Đội quản lý Điện lực khu vực Định Hóa"

GOOGLE_SHEET_ID_DEFAULT = "1nXFKJrn8oHwQgUzv5QYihoazYRhhS1PeN-xyo7Er2iM"  # ví dụ
KPI_SHEET_DEFAULT       = "KPI"
APP_KPI_DRIVE_ROOT_ID_DEFAULT = "1rE3E8CuPViw8-VYWYZgeB4Mz9WEY3e7"            # id thư mục gốc App_KPI

defaults = {
    "spreadsheet_id": GOOGLE_SHEET_ID_DEFAULT,
    "kpi_sheet_name":  KPI_SHEET_DEFAULT,
    "drive_root_id":   APP_KPI_DRIVE_ROOT_ID_DEFAULT,
    "_report_folder_id": "",
    "_selected_idx": None,   # lưu dòng đang tick trong bảng CSV
}
for k, v in defaults.items():
    if k not in st.session_state:
        st.session_state[k] = v

# ------------------- TIỆN ÍCH CHUNG -------------------
def toast(msg, icon="ℹ️"):
    try: st.toast(msg, icon=icon)
    except Exception: pass

def extract_sheet_id(text: str) -> str:
    if not text: return ""
    m = re.search(r"/d/([a-zA-Z0-9-_]+)", text.strip())
    return m.group(1) if m else text.strip()

def extract_drive_folder_id(s: str) -> str:
    if not s: return ""
    m = re.search(r"/folders/([a-zA-Z0-9_-]+)", s.strip())
    return m.group(1) if m else s.strip()

def get_gs_clients():
    try:
        svc = dict(st.secrets["gdrive_service_account"])
        if "private_key" in svc:
            # sửa xuống dòng trong private_key (tránh lỗi)
            svc["private_key"] = (
                svc["private_key"]
                .replace("\\r\\n", "\\n")
                .replace("\\r", "\\n")
                .replace("\\\\n", "\\n")
            )
        scopes = [
            "https://www.googleapis.com/auth/spreadsheets",
            "https://www.googleapis.com/auth/drive",
        ]
        creds = Credentials.from_service_account_info(svc, scopes=scopes)
        gclient = gspread.authorize(creds)
        return gclient, creds
    except Exception as e:
        st.session_state["_gs_error"] = f"SECRETS_ERROR: {e}"
        return None, None

def open_spreadsheet(sid_or_url: str):
    sid = extract_sheet_id(sid_or_url or GOOGLE_SHEET_ID_DEFAULT) or GOOGLE_SHEET_ID_DEFAULT
    gclient, creds = st.session_state.get("_gs_pair", (None, None))
    if gclient is None:
        gclient, creds = get_gs_clients()
        st.session_state["_gs_pair"] = (gclient, creds)
    if gclient is None:
        raise RuntimeError("Chưa cấu hình service account trong st.secrets.")
    return gclient.open_by_key(sid)

def df_from_ws(ws) -> pd.DataFrame:
    records = ws.get_all_records(expected_headers=ws.row_values(1))
    return pd.DataFrame(records)

ALIAS = {
    "USE (mã đăng nhập)": ["USE (mã đăng nhập)", r"Tài khoản (USE\\username)", "Tài khoản (USE/username)", "Tài khoản", "Username", "USE", "User"],
    "Mật khẩu mặc định": ["Mật khẩu mặc định", "Password mặc định", "Password", "Mật khẩu"],
    "Tên chỉ tiêu (KPI)": ["Tên chỉ tiêu (KPI)", "Tên KPI", "Chỉ tiêu"],
    "Đơn vị tính": ["Đơn vị tính", "Unit"],
    "Kế hoạch": ["Kế hoạch", "Plan", "Target", "Kế hoạch (tháng)"],
    "Thực hiện": ["Thực hiện", "Thực hiện (tháng)", "Actual (month)"],
    "Trọng số": ["Trọng số", "Weight"],
    "Bộ phận/người phụ trách": ["Bộ phận/người phụ trách", "Phụ trách"],
    "Tháng": ["Tháng", "Month"],
    "Năm": ["Năm", "Year"],
    "Điểm KPI": ["Điểm KPI", "Score"],
    "Ghi chú": ["Ghi chú", "Notes"],
    "Tên đơn vị": ["Tên đơn vị", "Đơn vị"],
    "Phương pháp đo kết quả": ["Phương pháp đo kết quả", "Cách tính", "Công thức"],
    "Ngưỡng dưới": ["Ngưỡng dưới", "Min"],
    "Ngưỡng trên": ["Ngưỡng trên", "Max"],
}

def normalize_columns(df: pd.DataFrame) -> pd.DataFrame:
    if df is None or df.empty:
        return df
    cols_lower = {c.strip().lower(): c for c in df.columns}
    rename = {}
    for std, cands in ALIAS.items():
        if std in df.columns:
            continue
        for c in cands:
            key = c.strip().lower()
            if key in cols_lower:
                rename[cols_lower[key]] = std
                break
    if rename:
        df = df.rename(columns=rename)
    if "Thực hiện (tháng)" in df.columns and "Thực hiện" not in df.columns:
        df = df.rename(columns={"Thực hiện (tháng)": "Thực hiện"})
    if "Kế hoạch (tháng)" in df.columns and "Kế hoạch" not in df.columns:
        df = df.rename(columns={"Kế hoạch (tháng)": "Kế hoạch"})
    return df

def format_vn_number(x, decimals=2):
    try:
        f = float(x)
    except:
        return ""
    s = f"{f:,.{decimals}f}"
    return s.replace(",", "_").replace(".", ",").replace("_", ".")

def parse_vn_number(s):
    if s is None:
        return None
    txt = str(s).strip()
    if txt == "" or txt.lower() in ("none", "nan"):
        return None
    txt = txt.replace(".", "").replace(",", ".")
    try:
        return float(txt)
    except:
        return None

def parse_float(x):
    if isinstance(x, (int, float)):
        return float(x)
    return parse_vn_number(x)

def to_percent(val):
    v = parse_float(val)
    if v is None:
        return None
    return v * 100.0 if abs(v) <= 1.0 else v

def kpi_penalty_error_method(actual_err_pct, threshold_pct=1.5, step_pct=0.1, per_step_penalty=0.02, max_penalty=3.0):
    """Tính điểm trừ theo % sai số vượt ngưỡng (0 → 3)."""
    if actual_err_pct is None:
        return 0.0, None
    exceed = max(0.0, actual_err_pct - threshold_pct)
    steps = int(exceed // step_pct)
    penalty = min(max_penalty, steps * per_step_penalty)
    return penalty, max(0.0, 10.0 - penalty)

def compute_score_generic(plan, actual, weight):
    """Điểm KPI thông thường: (Actual/Plan) * 10 * trọng số."""
    if plan in (None, 0) or actual is None:
        return None
    ratio = max(min(actual / plan, 2.0), 0.0)
    w = weight / 100.0 if (weight and weight > 1) else (weight or 0.0)
    return round(ratio * 10 * w, 2)

def compute_score_with_method(row):
    """Trả về:
       - Nếu là phương pháp 'sai số ... trừ 0,02 (max 3)': TRẢ VỀ ĐIỂM TRỪ (0→3).
       - Các phương pháp khác: TRẢ VỀ điểm KPI đã nhân trọng số (0→10*w)."""
    # lấy từ form nếu có
    plan   = parse_vn_number(st.session_state.get("plan_txt",""))   if "plan_txt"   in st.session_state else None
    actual = parse_vn_number(st.session_state.get("actual_txt","")) if "actual_txt" in st.session_state else None
    if plan is None:   plan   = parse_float(row.get("Kế hoạch"))
    if actual is None: actual = parse_float(row.get("Thực hiện"))

    weight = parse_float(row.get("Trọng số")) or 0.0
    method = str(row.get("Phương pháp đo kết quả") or "").strip().lower()

    # --- PHƯƠNG PHÁP ĐIỂM TRỪ THEO SAI SỐ ---
    if ("sai số" in method or "sai so" in method) and ("0,02" in method or "0.02" in method):
        unit = str(row.get("Đơn vị tính") or "").lower()
        actual_err_pct = None
        if actual is not None:
            if actual <= 5 or ("%" in unit and actual <= 100):
                actual_err_pct = to_percent(actual)
            elif plan not in (None, 0):
                actual_err_pct = abs(actual - plan) / abs(plan) * 100.0

        threshold = 1.5
        m = re.search(r"(\d+)[\.,](\d+)", method)  # nếu viết 1,5 hay 1.5 trong mô tả
        if m:
            try:
                threshold = float(m.group(1) + "." + m.group(2))
            except:
                threshold = 1.5
        else:
            thr = parse_float(row.get("Ngưỡng trên"))
            if thr is not None:
                threshold = thr

        penalty, _ = kpi_penalty_error_method(actual_err_pct, threshold, 0.1, 0.02, 3.0)
        # TRẢ VỀ điểm trừ (0→3), KHÔNG nhân trọng số
        return round(penalty, 2)

    # --- CÁC PHƯƠNG PHÁP KHÁC ---
    if plan in (None, 0) or actual is None:
        return None
    w = weight / 100.0 if (weight and weight > 1) else (weight or 0.0)

    if any(k in method for k in ["tăng", ">=", "cao hơn tốt", "increase", "higher"]):
        return round(max(min(actual / plan, 2.0), 0.0) * 10 * w, 2)
    if any(k in method for k in ["giảm", "<=", "thấp hơn tốt", "decrease", "lower"]):
        ratio = 1.0 if actual <= plan else max(min(plan / actual, 2.0), 0.0)
        return round(ratio * 10 * w, 2)
    if any(k in method for k in ["đạt", "dat", "bool", "pass/fail"]):
        return round((10.0 if actual >= plan else 0.0) * w, 2)
    if any(k in method for k in ["khoảng", "range", "trong khoảng"]):
        lo = parse_float(row.get("Ngưỡng dưới"))
        hi = parse_float(row.get("Ngưỡng trên"))
        if lo is None or hi is None:
            return round(max(min(actual / plan, 2.0), 0.0) * 10 * w, 2)
        return round((10.0 if (lo <= actual <= hi) else 0.0) * w, 2)

    # mặc định
    return compute_score_generic(plan, actual, weight)

# ------------------- ĐĂNG NHẬP -------------------
def find_use_worksheet(sh):
    try:
        return sh.worksheet("USE")
    except Exception:
        for ws in sh.worksheets():
            try:
                headers = [h.strip() for h in ws.row_values(1)]
            except Exception:
                continue
            if (
                ("USE (mã đăng nhập)" in headers)
                or ("Tài khoản (USE\\username)" in headers)
                or ("Tài khoản" in headers)
                or ("Username" in headers)
                or ("USE" in headers)
            ) and ("Mật khẩu mặc định" in headers or "Password" in headers or "Mật khẩu" in headers):
                return ws
        raise gspread.exceptions.WorksheetNotFound("Không tìm thấy sheet USE.")

def load_users_df():
    sh = open_spreadsheet(st.session_state.get("spreadsheet_id", ""))
    ws = find_use_worksheet(sh)
    return normalize_columns(df_from_ws(ws))

def check_credentials(use_name: str, password: str) -> bool:
    df = load_users_df()
    if df.empty:
        return False
    col_use = next(
        (c for c in df.columns if c.strip().lower() in ["tài khoản (use\\username)", "tài khoản", "username", "use (mã đăng nhập)", "use"]),
        None,
    )
    col_pw = next(
        (c for c in df.columns if c.strip().lower() in ["mật khẩu mặc định", "password mặc định", "password", "mật khẩu"]),
        None,
    )
    if not col_use or not col_pw:
        return False
    u = (use_name or "").strip().lower()
    p = (password or "").strip()
    row = df.loc[df[col_use].astype(str).str.strip().str.lower() == u]
    return (not row.empty) and (str(row.iloc[0][col_pw]).strip() == p)

# ------------------- DRIVE HELPERS -------------------
def get_drive_service():
    if gbuild is None:
        st.warning("Thiếu thư viện 'google-api-python-client' để thao tác Google Drive.")
        return None
    gclient, creds = st.session_state.get("_gs_pair", (None, None))
    if creds is None:
        gclient, creds = get_gs_clients()
        st.session_state["_gs_pair"] = (gclient, creds)
    if creds is None:
        return None
    return gbuild("drive", "v3", credentials=creds)

def ensure_parent_ok(service, parent_id: str):
    try:
        service.files().get(fileId=parent_id, fields="id,name").execute()
    except HttpError as e:
        raise RuntimeError(
            f"Không truy cập được thư mục gốc (ID: {parent_id}). Kiểm tra ID/URL & quyền Editor cho service account."
        ) from e

def ensure_folder(service, parent_id: str, name: str) -> str:
    ensure_parent_ok(service, parent_id)
    q = (
        "mimeType='application/vnd.google-apps.folder' and "
        f"name='{name}' and '{parent_id}' in parents and trashed=false"
    )
    res = service.files().list(
        q=q, spaces="drive", supportsAllDrives=True, includeItemsFromAllDrives=True, fields="files(id,name)"
    ).execute()
    items = res.get("files", [])
    if items:
        return items[0]["id"]
    file_metadata = {"name": name, "mimeType": "application/vnd.google-apps.folder", "parents": [parent_id]}
    folder = service.files().create(body=file_metadata, fields="id", supportsAllDrives=True).execute()
    return folder["id"]

def find_file_in_folder(service, parent_id: str, name: str):
    q = (
        f"name='{name}' and '{parent_id}' in parents and "
        "mimeType!='application/vnd.google-apps.folder' and trashed=false"
    )
    res = service.files().list(
        q=q, spaces="drive", supportsAllDrives=True, includeItemsFromAllDrives=True, fields="files(id,name,mimeType)"
    ).execute()
    files = res.get("files", [])
    return files[0] if files else None

def upload_or_update(service, parent_id: str, filename: str, data: bytes, mime: str) -> str:
    media = MediaIoBaseUpload(io.BytesIO(data), mimetype=mime, resumable=False)
    existing = find_file_in_folder(service, parent_id, filename)
    if existing:
        f = service.files().update(
            fileId=existing["id"], media_body=media, supportsAllDrives=True, fields="id"
        ).execute()
        return f["id"]
    file_metadata = {"name": filename, "parents": [parent_id]}
    try:
        f = service.files().create(
            body=file_metadata, media_body=media, fields="id", supportsAllDrives=True
        ).execute()
        return f["id"]
    except HttpError as e:
        raise RuntimeError(
            "Service account không có quota để tạo file trong 'My Drive'. Hãy dùng Shared Drive, hoặc tạo sẵn file để app UPDATE."
        ) from e

# ------------------- HỖ TRỢ XUẤT EXCEL/PDF -------------------
def df_to_excel_bytes(df: pd.DataFrame) -> bytes:
    """Trả về bytes Excel (ưu tiên openpyxl, sau đó xlsxwriter). Fallback CSV."""
    # openpyxl
    try:
        buf = io.BytesIO()
        with pd.ExcelWriter(buf, engine="openpyxl") as writer:
            df.to_excel(writer, index=False, sheet_name="KPI")
        return buf.getvalue()
    except Exception:
        pass
    # xlsxwriter
    try:
        buf = io.BytesIO()
        with pd.ExcelWriter(buf, engine="xlsxwriter") as writer:
            df.to_excel(writer, index=False, sheet_name="KPI")
        return buf.getvalue()
    except Exception:
        # fallback CSV
        return df.to_csv(index=False).encode("utf-8")

def generate_pdf_from_df(df: pd.DataFrame, title="BÁO CÁO KPI") -> bytes:
    try:
        from reportlab.lib.pagesizes import A4, landscape
        from reportlab.platypus import SimpleDocTemplate, Table, TableStyle, Paragraph, Spacer
        from reportlab.lib import colors
        from reportlab.lib.units import cm
        from reportlab.lib.styles import getSampleStyleSheet

        buf = io.BytesIO()
        doc = SimpleDocTemplate(
            buf, pagesize=landscape(A4), rightMargin=20, leftMargin=20, topMargin=20, bottomMargin=20
        )
        styles = getSampleStyleSheet()
        story = [Paragraph(title, styles["Title"]), Spacer(1, 0.3 * cm)]
        cols = list(df.columns)
        data = [cols] + df.fillna("").astype(str).values.tolist()
        t = Table(data, repeatRows=1)
        t.setStyle(
            TableStyle(
                [
                    ("BACKGROUND", (0, 0), (-1, 0), colors.lightgrey),
                    ("GRID", (0, 0), (-1, -1), 0.25, colors.grey),
                    ("FONTSIZE", (0, 0), (-1, -1), 8),
                    ("ALIGN", (0, 0), (-1, -1), "CENTER"),
                ]
            )
        )
        story.append(t)
        doc.build(story)
        return buf.getvalue()
    except Exception:
        return b""

# ------------------- SIDEBAR: ĐĂNG NHẬP + CẤU HÌNH -------------------
with st.sidebar:
    st.header("🔒 Đăng nhập")
    if "_user" not in st.session_state:
        with st.form("login_form", clear_on_submit=False):
            use_input = st.text_input("USE (vd: PCTN\\KVDHA)")
            pwd_input = st.text_input("Mật khẩu", type="password")
            ok = st.form_submit_button("Đăng nhập", use_container_width=True)
        if ok:
            if check_credentials(use_input, pwd_input):
                st.session_state["_user"] = use_input.strip()
                toast("Đăng nhập thành công.", "✅")
                st.rerun()
            else:
                st.error("USE hoặc mật khẩu không đúng.")
    else:
        st.success(f"Đang đăng nhập: **{st.session_state['_user']}**")
        st.subheader("🧩 Kết nối Google Sheets")
        st.text_input("ID/URL Google Sheet", key="spreadsheet_id")
        st.text_input("Tên sheet KPI", key="kpi_sheet_name")

        st.subheader("📁 Thư mục lưu báo cáo (Drive)")
        st.text_input("ID/URL thư mục gốc App_KPI", key="drive_root_id")
        st.caption("Khuyến nghị: dùng Shared Drive để tránh lỗi quota 403.")

        if st.button("🔧 Chuẩn bị thư mục báo cáo", use_container_width=True):
            try:
                service = get_drive_service()
                if service is None:
                    st.stop()
                use_code = st.session_state["_user"].split("\\")[-1].upper()
                root_id  = extract_drive_folder_id(st.session_state.get("drive_root_id") or APP_KPI_DRIVE_ROOT_ID_DEFAULT)
                folder_user   = ensure_folder(service, root_id, use_code)
                folder_report = ensure_folder(service, folder_user, "Báo cáo KPI")
                st.session_state["_report_folder_id"] = folder_report
                toast(f"Đã sẵn sàng: /{use_code}/Báo cáo KPI", "✅")
            except Exception as e:
                st.error(f"Lỗi chuẩn bị thư mục: {e}")

        if st.button("Đăng xuất", use_container_width=True):
            st.session_state.pop("_user", None)
            st.session_state["_report_folder_id"] = ""
            toast("Đã đăng xuất.", "✅")
            st.rerun()

# ------------------- MAIN -------------------
st.title(APP_TITLE)
if "_user" not in st.session_state:
    st.info("Vui lòng đăng nhập để làm việc.")
    st.stop()

KPI_COLS = [
    "Tên chỉ tiêu (KPI)", "Đơn vị tính", "Kế hoạch", "Thực hiện", "Trọng số",
    "Bộ phận/người phụ trách", "Tháng", "Năm", "Phương pháp đo kết quả",
    "Ngưỡng dưới", "Ngưỡng trên", "Điểm KPI", "Ghi chú", "Tên đơn vị"
]

def get_sheet_and_name():
    sid_cfg = st.session_state.get("spreadsheet_id", "") or GOOGLE_SHEET_ID_DEFAULT
    # Quan trọng: nếu để trống → fallback "KPI"
    sheet_name = st.session_state.get("kpi_sheet_name") or KPI_SHEET_DEFAULT
    sh = open_spreadsheet(sid_cfg)
    return sh, sheet_name

def write_kpi_to_sheet(sh, sheet_name: str, df: pd.DataFrame) -> bool:
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
            ws = sh.add_worksheet(title=sheet_name, rows=len(data) + 10, cols=max(12, len(cols)))
        ws.update(data, value_input_option="USER_ENTERED")
        return True
    except Exception as e:
        st.error(f"Lưu KPI thất bại: {e}")
        return False

# -------- dữ liệu form (state) --------
if "_csv_form" not in st.session_state:
    st.session_state["_csv_form"] = {
        "Tên chỉ tiêu (KPI)": "",
        "Đơn vị tính": "",
        "Kế hoạch": 0.0,
        "Thực hiện": 0.0,
        "Trọng số": 100.0,
        "Bộ phận/người phụ trách": "",
        "Tháng": str(datetime.now().month),
        "Năm": str(datetime.now().year),
        "Phương pháp đo kết quả": "Tăng tốt hơn",
        "Ngưỡng dưới": "",
        "Ngưỡng trên": "",
        "Ghi chú": "",
        "Tên đơn vị": "",
    }

# prefill từ dòng đã chọn
if st.session_state.get("_prefill_from_row"):
    row = st.session_state.pop("_prefill_from_row")
    for k, v in row.items():
        if k in KPI_COLS:
            st.session_state["_csv_form"][k] = v
    # đồng bộ text hiển thị số định dạng VN
    if "plan_txt" not in st.session_state:
        st.session_state["plan_txt"] = ""
    if "actual_txt" not in st.session_state:
        st.session_state["actual_txt"] = ""
    st.session_state["plan_txt"]   = format_vn_number(parse_float(row.get("Kế hoạch")  or 0), 2)
    st.session_state["actual_txt"] = format_vn_number(parse_float(row.get("Thực hiện") or 0), 2)

if "plan_txt"   not in st.session_state: st.session_state["plan_txt"]   = format_vn_number(st.session_state["_csv_form"].get("Kế hoạch")  or 0.0, 2)
if "actual_txt" not in st.session_state: st.session_state["actual_txt"] = format_vn_number(st.session_state["_csv_form"].get("Thực hiện") or 0.0, 2)

# ------------------- FORM (TRÊN) -------------------
st.subheader("✍️ Biểu mẫu nhập tay")
f = st.session_state["_csv_form"]

def _on_change_plan():
    val = parse_vn_number(st.session_state["plan_txt"])
    if val is not None:
        st.session_state["_csv_form"]["Kế hoạch"] = val
        st.session_state["plan_txt"] = format_vn_number(val, 2)

def _on_change_actual():
    val = parse_vn_number(st.session_state["actual_txt"])
    if val is not None:
        st.session_state["_csv_form"]["Thực hiện"] = val
        st.session_state["actual_txt"] = format_vn_number(val, 2)

c0 = st.columns([2, 1, 1, 1])
with c0[0]:
    f["Tên chỉ tiêu (KPI)"] = st.text_input("Tên chỉ tiêu (KPI)", value=f["Tên chỉ tiêu (KPI)"])
with c0[1]:
    f["Đơn vị tính"] = st.text_input("Đơn vị tính", value=f["Đơn vị tính"])
with c0[2]:
    f["Bộ phận/người phụ trách"] = st.text_input("Bộ phận/người phụ trách", value=f["Bộ phận/người phụ trách"])
with c0[3]:
    f["Tên đơn vị"] = st.text_input("Tên đơn vị", value=f["Tên đơn vị"])

c1 = st.columns(3)
with c1[0]:
    st.text_input("Kế hoạch", key="plan_txt", on_change=_on_change_plan)
with c1[1]:
    st.text_input("Thực hiện", key="actual_txt", on_change=_on_change_actual)
with c1[2]:
    f["Trọng số"] = st.number_input("Trọng số (%)", value=float(f.get("Trọng số") or 0.0))

c2 = st.columns(3)
with c2[0]:
    options_methods = [
        "Tăng tốt hơn",
        "Giảm tốt hơn",
        "Đạt/Không đạt",
        "Trong khoảng",
        "Sai số ≤ ±1,5%: mỗi 0,1% vượt trừ 0,02 (max 3)",
    ]
    cur = f.get("Phương pháp đo kết quả", "Tăng tốt hơn")
    f["Phương pháp đo kết quả"] = st.selectbox(
        "Phương pháp đo kết quả",
        options=options_methods,
        index=options_methods.index(cur) if cur in options_methods else 0,
    )

with c2[1]:
    tmp_row = {k: f.get(k) for k in f.keys()}
    tmp_row["Điểm KPI"] = compute_score_with_method(tmp_row)
    is_penalty = "sai số" in f.get("Phương pháp đo kết quả", "").lower() and (
        "0,02" in f["Phương pháp đo kết quả"] or "0.02" in f["Phương pháp đo kết quả"]
    )
    label_metric = "Điểm trừ (tự tính)" if is_penalty else "Điểm KPI (tự tính)"
    st.metric(label_metric, tmp_row["Điểm KPI"] if tmp_row["Điểm KPI"] is not None else "—")

with c2[2]:
    f["Ghi chú"] = st.text_input("Ghi chú", value=f["Ghi chú"])

if f["Phương pháp đo kết quả"] == "Trong khoảng":
    c3 = st.columns(2)
    with c3[0]:
        f["Ngưỡng dưới"] = st.text_input("Ngưỡng dưới", value=str(f.get("Ngưỡng dưới") or ""))
    with c3[1]:
        f["Ngưỡng trên"] = st.text_input("Ngưỡng trên", value=str(f.get("Ngưỡng trên") or ""))

c4 = st.columns(2)
with c4[0]:
    f["Tháng"] = st.text_input("Tháng", value=str(f["Tháng"]))
with c4[1]:
    f["Năm"] = st.text_input("Năm", value=str(f["Năm"]))

btns = st.columns([1, 1, 1, 1, 2])
apply_clicked      = btns[0].button("Áp dụng vào bảng CSV tạm", type="primary", use_container_width=True)
save_csv_clicked   = btns[1].button("💾 Ghi CSV tạm vào sheet KPI", use_container_width=True)
refresh_clicked    = btns[2].button("🔁 Làm mới bảng CSV", use_container_width=True)
export_clicked     = btns[3].button("📤 Xuất báo cáo (Excel/PDF)", use_container_width=True)
save_drive_clicked = btns[4].button("☁️ Lưu dữ liệu vào Google Drive", use_container_width=True)

# ------------------- CSV (DƯỚI) -------------------
st.subheader("⬇️ Nhập CSV vào KPI")

up = st.file_uploader("Tải file CSV", type=["csv"])
if "_csv_cache" not in st.session_state:
    st.session_state["_csv_cache"] = pd.DataFrame(columns=KPI_COLS)
if up is not None:
    try:
        tmp = pd.read_csv(up)
    except Exception:
        up.seek(0)
        tmp = pd.read_csv(up, encoding="utf-8-sig")
    tmp = normalize_columns(tmp)
    if "Điểm KPI" not in tmp.columns:
        tmp["Điểm KPI"] = tmp.apply(compute_score_with_method, axis=1)
    st.session_state["_csv_cache"] = tmp

# dựng dataframe hiển thị với cột ✓ theo _selected_idx
base = st.session_state["_csv_cache"]
df_show = base.copy()
if "✓ Chọn" not in df_show.columns:
    df_show.insert(0, "✓ Chọn", False)
df_show["✓ Chọn"] = df_show["✓ Chọn"].astype("bool")
sel = st.session_state.get("_selected_idx", None)
if sel is not None and sel in df_show.index:
    df_show.loc[sel, "✓ Chọn"] = True

df_edit = st.data_editor(
    df_show,
    use_container_width=True,
    hide_index=True,
    num_rows="dynamic",
    column_config={
        "✓ Chọn": st.column_config.CheckboxColumn(
            label="✓ Chọn", default=False, help="Chọn 1 dòng để nạp lên biểu mẫu"
        )
    },
    key="csv_editor",
)

# lưu cache (loại cột ✓)
st.session_state["_csv_cache"] = df_edit.drop(columns=["✓ Chọn"], errors="ignore")

# phát hiện thay đổi chọn
new_selected_idxs = df_edit.index[df_edit["✓ Chọn"] == True].tolist()
new_sel = new_selected_idxs[0] if new_selected_idxs else None
if new_sel != st.session_state.get("_selected_idx"):
    st.session_state["_selected_idx"] = new_sel
    if new_sel is not None:
        st.session_state["_prefill_from_row"] = st.session_state["_csv_cache"].loc[new_sel].to_dict()
    st.rerun()

# áp dụng form vào cache
def apply_form_to_cache():
    base = st.session_state["_csv_cache"].copy()
    new_row = {c: st.session_state["_csv_form"].get(c, "") for c in KPI_COLS}
    new_row["Kế hoạch"]  = parse_vn_number(st.session_state.get("plan_txt", ""))
    new_row["Thực hiện"] = parse_vn_number(st.session_state.get("actual_txt", ""))
    new_row["Điểm KPI"]  = compute_score_with_method(new_row)

    sel = st.session_state.get("_selected_idx", None)
    if sel is not None and sel in base.index:
        for k, v in new_row.items():
            base.loc[sel, k] = v
    else:
        if str(new_row.get("Tên chỉ tiêu (KPI)", "")).strip():
            base = pd.concat([base, pd.DataFrame([new_row])], ignore_index=True)

    st.session_state["_csv_cache"] = base
    st.session_state["plan_txt"]   = format_vn_number(new_row.get("Kế hoạch") or 0, 2)
    st.session_state["actual_txt"] = format_vn_number(new_row.get("Thực hiện") or 0, 2)

if apply_clicked:
    apply_form_to_cache()
    toast("Đã áp dụng dữ liệu biểu mẫu vào CSV tạm.", "✅")
    st.rerun()

# ------------- Xuất / Ghi Sheet / Làm mới / Lưu Drive -------------
if save_csv_clicked:
    try:
        apply_form_to_cache()
        sh, sheet_name = get_sheet_and_name()
        if write_kpi_to_sheet(sh, sheet_name, st.session_state["_csv_cache"]):
            toast(f"Đã ghi dữ liệu vào sheet '{sheet_name}'.", "✅")
    except Exception as e:
        st.error(f"Lỗi khi ghi Sheets: {e}")

if refresh_clicked:
    if "confirm_refresh" not in st.session_state:
        st.session_state["confirm_refresh"] = True
    else:
        st.session_state["confirm_refresh"] = not st.session_state["confirm_refresh"]

if st.session_state.get("confirm_refresh", False):
    with st.expander("❓ Bạn xác định làm mới dữ liệu chứ? (Sẽ mất những thay đổi chưa ghi)", expanded=True):
        c = st.columns(2)
        if c[0].button("Có, làm mới ngay", type="primary"):
            st.session_state["_csv_cache"] = pd.DataFrame(columns=KPI_COLS)
            st.session_state["_selected_idx"] = None
            st.session_state["confirm_refresh"] = False
            toast("Đã làm mới bảng CSV tạm.", "✅")
            st.rerun()
        if c[1].button("Không, giữ nguyên dữ liệu"):
            st.session_state["confirm_refresh"] = False
            toast("Đã hủy làm mới.", "ℹ️")

if export_clicked:
    apply_form_to_cache()
    excel_bytes = df_to_excel_bytes(st.session_state["_csv_cache"])
    st.download_button(
        "⬇️ Tải báo cáo (Excel/CSV)",
        data=excel_bytes,
        file_name="KPI_baocao.xlsx",
        mime="application/octet-stream",
    )
    pdf_bytes = generate_pdf_from_df(st.session_state["_csv_cache"], "BÁO CÁO KPI")
    if pdf_bytes:
        st.download_button("⬇️ Tải PDF báo cáo", data=pdf_bytes, file_name="KPI_baocao.pdf", mime="application/pdf")

if save_drive_clicked:
    try:
        apply_form_to_cache()
        service = get_drive_service()
        if service is None:
            st.stop()
        use_code = st.session_state["_user"].split("\\")[-1].upper()
        folder_report = st.session_state.get("_report_folder_id") or ""
        root_raw = st.session_state.get("drive_root_id") or APP_KPI_DRIVE_ROOT_ID_DEFAULT
        root_id = extract_drive_folder_id(root_raw)
        if not folder_report:
            folder_user = ensure_folder(service, root_id, use_code)
            folder_report = ensure_folder(service, folder_user, "Báo cáo KPI")
            st.session_state["_report_folder_id"] = folder_report
        ts = datetime.now().strftime("%d-%m-%y")
        fname_xlsx = f"KPI_{ts}.xlsx"
        excel_bytes = df_to_excel_bytes(st.session_state["_csv_cache"])
        upload_or_update(
            service,
            folder_report,
            fname_xlsx,
            excel_bytes,
            "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        )
        toast(f"Đã lưu: /{use_code}/Báo cáo KPI/{fname_xlsx}", "✅")
    except Exception as e:
        st.error(f"Lỗi lưu Google Drive: {e}")
