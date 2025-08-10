import streamlit as st
import pandas as pd
import numpy as np
import io
import smtplib
from email.message import EmailMessage
from datetime import datetime
import matplotlib.pyplot as plt
from matplotlib.backends.backend_pdf import PdfPages
import base64

import gspread
from google.oauth2.service_account import Credentials
from gspread_dataframe import set_with_dataframe, get_as_dataframe

st.set_page_config(page_title="KPI Scorer – Định Hóa (Full Suite)", layout="wide", page_icon="🔐")

# ----- CSS + Logo -----
from pathlib import Path as _Path
_logo_path = _Path(__file__).parent / "assets" / "logo.png"
st.markdown('''
<style>
.brand-wrap{display:flex;align-items:center;gap:18px;padding:14px 18px;margin:8px 0 8px;
  border-radius:16px;border:1px solid rgba(245,158,11,.25);
  background: radial-gradient(900px circle at 0% -20%, rgba(245,158,11,.10), transparent 40%);}
.brand-wrap h1{font-size:28px;line-height:1.2;margin:0;}
.brand-wrap p{margin:2px 0 0;color:#6b7280}
.stButton>button{background:#f59e0b;color:white;border:0;border-radius:12px;padding:8px 14px}
.stButton>button:hover{filter:brightness(.95)}
</style>
''', unsafe_allow_html=True)
if _logo_path.exists():
    cA, cB = st.columns([1,10])
    with cA: st.image(str(_logo_path), width=64)
    with cB: st.markdown("<div class='brand-wrap'><div><h1>KPI Đội quản lý Điện lực khu vực Định Hóa</h1><p>Full Suite · Import linh hoạt · Nhập tay · Báo cáo & Email</p></div></div>", unsafe_allow_html=True)

# =====================
# Worksheet names
# =====================
WS_SOURCE = "Định Hóa"      # danh mục gốc (excel)
WS_KPI    = "KPI_DATA"      # dữ liệu KPI chấm theo tháng/bộ phận
WS_DEPTS  = "DEPARTMENTS"   # danh mục bộ phận
WS_TASKS  = "TASKS"         # danh mục công việc (Mã CV, tên...)
WS_EMPS   = "EMPLOYEES"     # CBCNV
WS_ASG    = "ASSIGNMENTS"   # phân công công việc cho cá nhân

# =====================
# Schemas
# =====================
EXPECTED_KPI_COLS = [
    "Bộ phận","Vai trò","Trọng số (%)",
    "Phương pháp đo","Đơn vị tính","Chỉ tiêu (tham chiếu)",
    "Tháng","Năm","Kế hoạch","Thực hiện","Ngưỡng/Kế hoạch (%)",
    "Sai số (%)","Bậc vượt (0.1%)","Bậc giảm (0.1%)","Điểm cộng","Điểm trừ","Kết quả (ròng)",
    "Điểm thưởng trực tiếp","Lý do thưởng","Điểm tổng",
    "Cập nhật lúc","Mã CV"
]
EXPECTED_DEPT_COLS = ["Bộ phận"]
EXPECTED_TASK_COLS = ["Mã CV","Tên công việc/Nhiệm vụ","Bộ phận","Mô tả"]
EXPECTED_EMP_COLS  = ["Mã NV","Họ và tên","Chức danh","Bộ phận","Bậc thợ","Hệ số lương","Hệ số phụ cấp","Trạng thái"]
EXPECTED_ASG_COLS  = ["Mã CV","Chỉ tiêu (tham chiếu)","Bộ phận","Tháng","Năm","Mã NV","Vai trò (Cá nhân)","Trọng số cá nhân (%)","Điểm thưởng trực tiếp (CN)","Lý do thưởng (CN)","Cập nhật lúc"]

DEFAULT_DEPTS = [
    "Tổ Kế hoạch kỹ thuật",
    "Tổ Kinh doanh tổng hợp",
    "Tổ Quản lý tổng hợp 1",
    "Tổ Quản lý tổng hợp 2",
    "Tổ Trực vận hành",
    "Tổ Kiểm tra giám sát mua bán điện",
]

# =====================
# Helpers from both files, with name conflicts resolved
# =====================
def get_client():
    # Read secrets
    try:
        svc = dict(st.secrets["google_service_account"])
    except Exception:
        st.error("❌ Chưa cấu hình secrets. Tạo .streamlit/secrets.toml và dán Service Account.")
        st.stop()

    # Support private_key_b64 (preferred) or private_key (fallback)
    if "private_key_b64" in svc and svc["private_key_b64"]:
        try:
            decoded = base64.b64decode(svc["private_key_b64"]).decode("utf-8")
            svc["private_key"] = decoded
        except Exception as e:
            st.error(f"❌ Giải mã private_key_b64 lỗi: {e}")
            st.stop()
    elif "private_key" in svc and svc["private_key"]:
        svc["private_key"] = svc["private_key"].replace("\\n", "\n")
    else:
        st.error("❌ Không tìm thấy private_key_b64 hay private_key trong secrets.")
        st.stop()

    scopes = ["https://www.googleapis.com/auth/spreadsheets","https://www.googleapis.com/auth/drive"]
    creds = Credentials.from_service_account_info(svc, scopes=scopes)
    return gspread.authorize(creds)

# This is a combined get_or_create_ws function
def get_or_create_ws(gc, spreadsheet_id, title, headers=None):
    sh = gc.open_by_key(spreadsheet_id)
    try:
        ws = sh.worksheet(title)
    except gspread.WorksheetNotFound:
        ws = sh.add_worksheet(title=title, rows=3000, cols=80)
        if headers:
            ws.update("A1", [headers])
    return ws

# This is a combined load_ws_df function
def load_ws_df(ws, expected_cols=None):
    df = get_as_dataframe(ws, evaluate_formulas=True, header=0) or pd.DataFrame()
    df = df.dropna(how="all")
    if expected_cols:
        for c in expected_cols:
            if c not in df.columns:
                df[c] = None
        df = df[expected_cols]
    return df

# This is a combined save_ws_df function
def save_ws_df(ws, df):
    set_with_dataframe(ws, df, include_index=False, include_column_header=True, resize=True)

# This is a combined safe_float function
def safe_float(x):
    if x is None or (isinstance(x, float) and np.isnan(x)):
        return None
    try:
        if isinstance(x, str):
            x = x.replace(".", "").replace(",", ".")
        return float(x)
    except:
        return None

def compute_points(row, group):
    # Group weights (Định Hóa = Nhóm 2)
    if group == 2:
        penalty_per_0_1 = 0.04
        bonus_per_0_1 = 0.30
    elif group == 1:
        penalty_per_0_1 = 0.02
        bonus_per_0_1 = 0.50
    else:
        penalty_per_0_1 = 0.05
        bonus_per_0_1 = 0.20
    max_penalty = 3.0
    max_bonus = 2.0

    unit = str(row.get("Đơn vị tính") or "").strip()
    nguong = safe_float(row.get("Ngưỡng/Kế hoạch (%)")) or 1.5
    ke_hoach = safe_float(row.get("Kế hoạch"))
    thuc_hien = safe_float(row.get("Thực hiện"))

    sai_so_pct = None
    if unit == "%":
        sai_so_pct = thuc_hien
    else:
        if ke_hoach not in (None, 0) and thuc_hien is not None:
            sai_so_pct = abs(thuc_hien - ke_hoach) / abs(ke_hoach) * 100.0

    bac = improve_bac = None
    diem_tru = diem_cong = 0.0
    if sai_so_pct is not None:
        over = max(0.0, sai_so_pct - nguong)
        improve = max(0.0, nguong - sai_so_pct)
        bac = int(np.floor(over / 0.1 + 1e-9))
        improve_bac = int(np.floor(improve / 0.1 + 1e-9))
        diem_tru = min(max_penalty, penalty_per_0_1 * bac)
        diem_cong = min(max_bonus, bonus_per_0_1 * improve_bac)

    bonus = safe_float(row.get("Điểm thưởng trực tiếp")) or 0.0
    ket_qua = (diem_cong or 0) - (diem_tru or 0)
    diem_tong = ket_qua + bonus

    return pd.Series({
        "Sai số (%)": None if sai_so_pct is None else round(sai_so_pct, 3),
        "Bậc vượt (0.1%)": bac,
        "Bậc giảm (0.1%)": improve_bac,
        "Điểm cộng": None if diem_cong is None else round(diem_cong, 2),
        "Điểm trừ": None if diem_tru is None else round(diem_tru, 2),
        "Kết quả (ròng)": round(ket_qua, 2),
        "Điểm tổng": round(diem_tong, 2)
    })

def flatten_dinh_hoa(df_raw):
    H0 = df_raw.iloc[0].astype(str).replace("nan","")
    H1 = df_raw.iloc[1].astype(str).replace("nan","")
    H2 = df_raw.iloc[2].astype(str).replace("nan","")
    ncol = df_raw.shape[1]
    final_cols = []
    for c in range(ncol):
        name = (H2.iloc[c] or H1.iloc[c] or H0.iloc[c])
        final_cols.append(str(name).strip() if name else f"Unnamed:{c}")
    body = df_raw.iloc[3:].copy()
    body.columns = final_cols
    body = body.dropna(axis=1, how='all')
    time_like = []
    for c in body.columns:
        cl = str(c).strip().lower()
        if cl in ["năm","quý i","quý ii","quý iii","quý iv"] or cl.startswith("tháng "):
            time_like.append(c)
    static_cols = [c for c in ["Chỉ tiêu","KÍ HIỆU","Ký hiệu","Đơn vị tính","ĐVT","Phương pháp đo kết quả","Đơn vị chủ trì"] if c in body.columns]
    values = body[time_like] if time_like else pd.DataFrame(index=body.index)
    flat = values.melt(ignore_index=False, var_name="Kỳ", value_name="Giá trị").join(body[static_cols], how="left").reset_index(drop=True)
    flat = flat[flat["Kỳ"].notna()]
    return flat

def send_email_smtp(to_email, subject, body, attachments=None):
    try:
        mail = st.secrets["smtp"]
        host = mail["host"]
        port = int(mail.get("port", 587))
        user = mail["user"]
        password = mail["password"]
        sender = mail.get("sender", user)
    except Exception:
        st.error("❌ Thiếu cấu hình SMTP trong secrets. Điền [smtp] host, port, user, password.")
        return False, "Missing SMTP secrets"

    msg = EmailMessage()
    msg["From"] = sender
    msg["To"] = to_email
    msg["Subject"] = subject
    msg.set_content(body)
    if attachments:
        for fname, bytes_data, mime in attachments:
            maintype, subtype = mime.split("/", 1)
            msg.add_attachment(bytes_data, maintype=maintype, subtype=subtype, filename=fname)
    try:
        with smtplib.SMTP(host, port) as server:
            server.starttls()
            server.login(user, password)
            server.send_message(msg)
        return True, "OK"
    except Exception as e:
        return False, str(e)


# =====================
# Main Application
# =====================
# The main application logic from app.py and app1.py is now combined
st.title("🧮 KPI Scorer – Định Hóa (Full Suite)")

with st.sidebar:
    st.subheader("🔐 Kết nối")
    spreadsheet_id = st.text_input("Spreadsheet ID", value="", placeholder="1A2B3C...")
    group = st.selectbox("Nhóm chấm", options=[1,2,3], index=1, help="Định Hóa thuộc Nhóm 2.")
    default_to_email = st.text_input("Email nhận báo cáo", value="phamlong666@gmail.com")

if not spreadsheet_id:
    st.info("Nhập Spreadsheet ID để bắt đầu.")
    st.stop()

try:
    gc = get_client()
    ws_kpi  = get_or_create_ws(gc, spreadsheet_id, WS_KPI, headers=EXPECTED_KPI_COLS)
    ws_src  = get_or_create_ws(gc, spreadsheet_id, WS_SOURCE)
    ws_dept = get_or_create_ws(gc, spreadsheet_id, WS_DEPTS, headers=EXPECTED_DEPT_COLS)
    ws_task = get_or_create_ws(gc, spreadsheet_id, WS_TASKS, headers=EXPECTED_TASK_COLS)
    ws_emp  = get_or_create_ws(gc, spreadsheet_id, WS_EMPS, headers=EXPECTED_EMP_COLS)
    ws_asg  = get_or_create_ws(gc, spreadsheet_id, WS_ASG, headers=EXPECTED_ASG_COLS)
except Exception as e:
    st.error(f"❌ Lỗi kết nối: {e}")
    st.stop()

# Bootstrap departments
dept_df = load_ws_df(ws_dept, expected_cols=EXPECTED_DEPT_COLS)
if dept_df.empty:
    dept_df = pd.DataFrame({"Bộ phận": DEFAULT_DEPTS})
    save_ws_df(ws_dept, dept_df)
dept_list = dept_df["Bộ phận"].dropna().astype(str).tolist()
emp_df = load_ws_df(ws_emp, expected_cols=EXPECTED_EMP_COLS)
asg_df = load_ws_df(ws_asg, expected_cols=EXPECTED_ASG_COLS)
all_kpi = load_ws_df(ws_kpi, expected_cols=EXPECTED_KPI_COLS)

# ====== Excel sync (from app.py)
st.header("0) Đồng bộ từ Excel 'Định Hóa'")
up = st.file_uploader("Chọn file Excel gốc (sheet 'Định Hóa')", type=["xlsx"], key="sync")
colA, colB, colC, colD = st.columns([1,1,1,2])
with colA:
    sync_dept = st.selectbox("Gán Bộ phận", options=dept_list)
with colB:
    sync_role = st.selectbox("Vai trò", options=["Chính","Phụ"], index=0)
with colC:
    sync_weight = st.number_input("Trọng số (%)", min_value=0.0, max_value=100.0, value=100.0, step=5.0)
with colD:
    st.caption("App sẽ gán các thuộc tính này cho dữ liệu import (sửa lại sau nếu cần).")
colE, colF = st.columns(2)
with colE:
    sync_month = st.number_input("Tháng", min_value=1, max_value=12, value=1, step=1, key="sync_month")
with colF:
    sync_year = st.number_input("Năm", min_value=2000, max_value=2100, value=datetime.now().year, step=1, key="sync_year")
overwrite = st.checkbox("Ghi đè KPI_DATA của Bộ phận/Tháng/Năm này", value=False)

if up and st.button("🔁 Đồng bộ từ Excel → KPI_DATA"):
    try:
        xls = pd.ExcelFile(up)
        sheet = "Định Hóa" if "Định Hóa" in xls.sheet_names else xls.sheet_names[0]
        raw = pd.read_excel(xls, sheet_name=sheet, header=None)
        flat = flatten_dinh_hoa(raw)
        
        chi_tieu_col = "Chỉ tiêu" if "Chỉ tiêu" in flat.columns else None
        dvt_col = "Đơn vị tính" if "Đơn vị tính" in flat.columns else ("ĐVT" if "ĐVT" in flat.columns else None)
        phuong_phap_col = "Phương pháp đo kết quả" if "Phương pháp đo kết quả" in flat.columns else None
        
        imp = pd.DataFrame({
            "Bộ phận": sync_dept,
            "Vai trò": sync_role,
            "Trọng số (%)": sync_weight,
            "Phương pháp đo": flat[phuong_phap_col] if phuong_phap_col else "",
            "Đơn vị tính": flat[dvt_col] if dvt_col else "",
            "Chỉ tiêu (tham chiếu)": flat[chi_tieu_col] if chi_tieu_col else "",
            "Tháng": int(sync_month),
            "Năm": int(sync_year),
            "Kế hoạch": None,
            "Thực hiện": flat["Giá trị"],
            "Ngưỡng/Kế hoạch (%)": 1.5,
            "Sai số (%)": None,
            "Bậc vượt (0.1%)": None,
            "Bậc giảm (0.1%)": None,
            "Điểm cộng": None,
            "Điểm trừ": None,
            "Kết quả (ròng)": None,
            "Điểm thưởng trực tiếp": 0,
            "Lý do thưởng": "",
            "Điểm tổng": None,
            "Cập nhật lúc": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "Mã CV": None
        })
        imp = imp[EXPECTED_KPI_COLS]
        
        data_df = load_ws_df(ws_kpi, expected_cols=EXPECTED_KPI_COLS)
        if overwrite:
            mask = (data_df["Bộ phận"] != sync_dept) | (data_df["Tháng"] != sync_month) | (data_df["Năm"] != sync_year)
            data_df = data_df[mask]
        
        final_df = pd.concat([data_df, imp], ignore_index=True)
        final_df = final_df.apply(lambda row: compute_points(row, group) if pd.notna(row['Thực hiện']) else row, axis=1)
        
        save_ws_df(ws_kpi, final_df)
        st.success("✅ Đồng bộ thành công!")

    except Exception as e:
        st.error(f"❌ Lỗi: {e}")

# ====== Manual input (from app.py)
st.header("1) Nhập KPI thủ công")
st.caption("Nhập KPI của một tháng và tính điểm")
with st.expander("📝 Nhập KPI", expanded=False):
    dept_kpi_options = ["- Chọn Bộ phận -"] + dept_list
    dept_kpi = st.selectbox("Bộ phận", options=dept_kpi_options, index=0, key="dept_kpi_select")
    col1, col2 = st.columns(2)
    with col1:
        month_kpi = st.number_input("Tháng", min_value=1, max_value=12, value=datetime.now().month, step=1, key="kpi_month")
    with col2:
        year_kpi = st.number_input("Năm", min_value=2000, max_value=2100, value=datetime.now().year, step=1, key="kpi_year")

    if dept_kpi != "- Chọn Bộ phận -":
        kpi_data = all_kpi[(all_kpi["Bộ phận"] == dept_kpi) & (all_kpi["Tháng"] == month_kpi) & (all_kpi["Năm"] == year_kpi)].copy()
        
        if kpi_data.empty:
            st.info("Chưa có KPI cho bộ phận này.")
        else:
            edited_kpi_df = st.data_editor(
                kpi_data,
                column_config={
                    "Bộ phận": st.column_config.SelectboxColumn("Bộ phận", options=dept_list),
                    "Vai trò": st.column_config.SelectboxColumn("Vai trò", options=["Chính", "Phụ"]),
                    "Trọng số (%)": st.column_config.NumberColumn("Trọng số (%)", format="%.2f", min_value=0.0, max_value=100.0),
                    "Tháng": st.column_config.NumberColumn("Tháng", min_value=1, max_value=12),
                    "Năm": st.column_config.NumberColumn("Năm", min_value=2000, max_value=2100),
                    "Kế hoạch": st.column_config.NumberColumn("Kế hoạch"),
                    "Thực hiện": st.column_config.NumberColumn("Thực hiện"),
                    "Ngưỡng/Kế hoạch (%)": st.column_config.NumberColumn("Ngưỡng/Kế hoạch (%)", format="%.2f", min_value=0.0, max_value=200.0),
                    "Điểm thưởng trực tiếp": st.column_config.NumberColumn("Điểm thưởng trực tiếp"),
                    "Lý do thưởng": st.column_config.TextColumn("Lý do thưởng", max_chars=200),
                    "Điểm tổng": st.column_config.NumberColumn("Điểm tổng", disabled=True),
                    "Cập nhật lúc": st.column_config.DatetimeColumn("Cập nhật lúc", format="YYYY-MM-DD HH:mm:ss", disabled=True),
                },
                use_container_width=True,
                num_rows="dynamic"
            )
            
            if st.button("💾 Cập nhật KPI"):
                edited_kpi_df["Cập nhật lúc"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                edited_kpi_df = edited_kpi_df.apply(lambda row: compute_points(row, group) if pd.notna(row['Thực hiện']) else row, axis=1)
                
                # Update the full all_kpi dataframe
                kpi_mask = (all_kpi["Bộ phận"] == dept_kpi) & (all_kpi["Tháng"] == month_kpi) & (all_kpi["Năm"] == year_kpi)
                all_kpi = all_kpi[~kpi_mask]
                all_kpi = pd.concat([all_kpi, edited_kpi_df], ignore_index=True)
                
                save_ws_df(ws_kpi, all_kpi)
                st.success("✅ Cập nhật KPI thành công!")
                st.experimental_rerun()

# ====== Individual KPI Report (from app1.py)
st.header("2) Báo cáo KPI cá nhân")
st.caption("Tổng hợp và báo cáo điểm KPI cá nhân")
rep_month = st.selectbox("Tháng", list(range(1, 13)), index=datetime.now().month - 1)
rep_year = st.selectbox("Năm", list(range(2020, datetime.now().year + 1)), index=datetime.now().year - 2020)

has_code_kpi = all_kpi["Mã CV"].notna() & (all_kpi["Mã CV"].astype(str).str.strip() != "")
kpi_with_code = all_kpi[has_code_kpi][["Bộ phận","Chỉ tiêu (tham chiếu)","Tháng","Năm","Mã CV","Điểm tổng"]].copy()
kpi_without_code = all_kpi[~has_code_kpi][["Bộ phận","Chỉ tiêu (tham chiếu)","Tháng","Năm","Điểm tổng"]].copy()

has_code_asg = asg_df["Mã CV"].notna() & (asg_df["Mã CV"].astype(str).str.strip() != "")

merged = pd.DataFrame(columns=list(asg_df.columns) + ["Điểm tổng"])
if has_code_asg.any():
    merged = pd.concat([merged, asg_df[has_code_asg].merge(kpi_with_code.dropna(subset=["Mã CV"]), on=["Mã CV", "Tháng", "Năm"], how="left")], ignore_index=True)
no_code_asg = ~has_code_asg
if no_code_asg.any():
    merged = pd.concat([merged, asg_df[no_code_asg].merge(kpi_without_code, on=["Bộ phận","Chỉ tiêu (tham chiếu)","Tháng","Năm"], how="left")], ignore_index=True)

mask_rep = (merged["Năm"] == rep_year) & (merged["Tháng"] == rep_month)
merged = merged[mask_rep].copy()
merged["Trọng số cá nhân (%)"] = pd.to_numeric(merged["Trọng số cá nhân (%)"], errors="coerce").fillna(0)
merged["Điểm tổng"] = pd.to_numeric(merged["Điểm tổng"], errors="coerce").fillna(0)
merged["Điểm thưởng trực tiếp (CN)"] = pd.to_numeric(merged["Điểm thưởng trực tiếp (CN)"], errors="coerce").fillna(0)

merged["Điểm KPI cá nhân"] = merged["Điểm tổng"] * merged["Trọng số cá nhân (%)"] / 100 + merged["Điểm thưởng trực tiếp (CN)"]
merged["Điểm KPI cá nhân"] = merged["Điểm KPI cá nhân"].apply(lambda x: round(x, 2))

per_person = merged.groupby("Mã NV").agg({
    "Điểm KPI cá nhân": "sum",
    "Trọng số cá nhân (%)": "sum"
}).reset_index()

per_person = per_person.merge(emp_df[["Mã NV", "Họ và tên", "Bộ phận"]], on="Mã NV", how="left")
per_person = per_person.sort_values(by="Điểm KPI cá nhân", ascending=False).reset_index(drop=True)

st.subheader(f"Tổng hợp KPI cá nhân tháng {rep_month:02d}/{rep_year}")
st.dataframe(per_person, use_container_width=True)

# ====== Charts (from app1.py)
st.subheader("Biểu đồ KPI cá nhân")
show_labels = st.checkbox("Hiển thị nhãn giá trị", value=True)
top_n = st.slider("Top N nhân viên", min_value=1, max_value=len(per_person) or 1, value=min(10, len(per_person) or 1))
top_df = per_person.head(top_n).sort_values(by="Điểm KPI cá nhân", ascending=True)

if not top_df.empty:
    fig1, ax1 = plt.subplots()
    ax1.barh(top_df["Họ và tên"], top_df["Điểm KPI cá nhân"], color="#f59e0b")
    ax1.set_title(f"Top {top_n} KPI cá nhân – {rep_month:02d}/{rep_year}")
    ax1.set_xlabel("Điểm")
    if show_labels:
        for i, v in enumerate(top_df["Điểm KPI cá nhân"]):
            ax1.text(v, i, f"{v:.2f}", va="center", ha="left", fontsize=8)
    st.pyplot(fig1)
else:
    st.info("Chưa có dữ liệu KPI cá nhân cho kỳ này.")

bottom_n = st.slider("Bottom N nhân viên", min_value=1, max_value=len(per_person) or 1, value=min(10, len(per_person) or 1))
bot_df = per_person.tail(bottom_n).sort_values(by="Điểm KPI cá nhân", ascending=False)
if not bot_df.empty:
    fig2, ax2 = plt.subplots(figsize=(10, 6))
    ax2.bar(bot_df["Họ và tên"], bot_df["Điểm KPI cá nhân"], color="#dc2626")
    ax2.set_title(f"Bottom {bottom_n} KPI cá nhân – {rep_month:02d}/{rep_year}")
    ax2.set_xticks(range(len(bot_df)))
    ax2.set_xticklabels(bot_df["Họ và tên"], rotation=20, ha="right")
    ax2.set_ylabel("Điểm")
    if show_labels:
        for i, v in enumerate(bot_df["Điểm KPI cá nhân"]):
            ax2.text(i, v, f"{v:.2f}", ha="center", va="bottom", fontsize=8)
    st.pyplot(fig2)
else:
    st.info("Chưa có dữ liệu KPI cá nhân cho kỳ này.")

# ====== Excel and Email (from app1.py)
excel_buf = io.BytesIO()
with pd.ExcelWriter(excel_buf, engine="xlsxwriter") as writer:
    merged.to_excel(writer, sheet_name="DETAIL_ASSIGN", index=False)
    per_person.to_excel(writer, sheet_name="KPI_PERSON", index=False)
excel_bytes = excel_buf.getvalue()

st.download_button("⬇️ Tải Excel KPI cá nhân", data=excel_bytes, file_name=f"KPI_canhan_{rep_year}_{rep_month:02d}.xlsx")

to_addr = st.text_input("Gửi tới (email)", value=default_to_email)
subject = f"Báo cáo KPI cá nhân {rep_month:02d}/{rep_year}"
body = f"Đính kèm báo cáo KPI cá nhân tháng {rep_month:02d}/{rep_year}. Gồm tổng hợp và chi tiết phân công."
if st.button("📧 Gửi email kèm Excel"):
    if not to_addr:
        st.warning("Vui lòng nhập địa chỉ email.")
    else:
        ok, msg = send_email_smtp(
            to_email=to_addr,
            subject=subject,
            body=body,
            attachments=[(
                f"KPI_canhan_{rep_year}_{rep_month:02d}.xlsx",
                excel_bytes,
                "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
            )]
        )
        if ok:
            st.success("✅ Gửi email thành công!")
        else:
            st.error(f"❌ Lỗi gửi email: {msg}")

