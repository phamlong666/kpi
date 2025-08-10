
import streamlit as st
import pandas as pd
import numpy as np
import io
import smtplib
from email.message import EmailMessage
from datetime import datetime
import matplotlib.pyplot as plt

import gspread
from google.oauth2.service_account import Credentials
from gspread_dataframe import set_with_dataframe, get_as_dataframe

st.set_page_config(page_title="KPI Scorer v3.6 – Định Hóa (CBCNV + Charts + Email)", layout="wide")

# =====================
# Config: worksheet names
# =====================
SOURCE_WS_TITLE = "Định Hóa"     # danh mục phương pháp đo/đơn vị/ chỉ tiêu (không bắt buộc trong v3.6)
DATA_WS_TITLE   = "KPI_DATA"     # dữ liệu chấm điểm theo tháng/bộ phận (đã có từ bản trước)
EMP_WS_TITLE    = "EMPLOYEES"    # danh sách CBCNV
ASSIGN_WS_TITLE = "ASSIGNMENTS"  # phân công công việc cho cá nhân
TASK_WS_TITLE   = "TASKS"        # danh mục công việc (tùy chọn, nếu dùng Mã CV)

# =====================
# Expected schemas
# =====================
EXPECTED_EMP_COLS = [
    "Mã NV","Họ và tên","Chức danh","Bộ phận","Bậc thợ","Hệ số lương","Hệ số phụ cấp","Trạng thái"
]

EXPECTED_ASSIGN_COLS = [
    "Mã CV","Chỉ tiêu (tham chiếu)","Bộ phận","Tháng","Năm",
    "Mã NV","Vai trò (Cá nhân)","Trọng số cá nhân (%)","Điểm thưởng trực tiếp (CN)","Lý do thưởng (CN)","Cập nhật lúc"
]

EXPECTED_DATA_COLS = [
    "Bộ phận","Vai trò","Trọng số (%)",
    "Phương pháp đo","Đơn vị tính","Chỉ tiêu (tham chiếu)",
    "Tháng","Năm","Kế hoạch","Thực hiện","Ngưỡng/Kế hoạch (%)",
    "Sai số (%)","Bậc vượt (0.1%)","Bậc giảm (0.1%)","Điểm cộng","Điểm trừ","Kết quả (ròng)",
    "Điểm thưởng trực tiếp","Lý do thưởng","Điểm tổng",
    "Cập nhật lúc","Mã CV"
]

# =====================
# Helpers
# =====================
def get_client():
    try:
        svc = dict(st.secrets["google_service_account"])
    except Exception:
        st.error("❌ Chưa cấu hình secrets. Tạo .streamlit/secrets.toml theo mẫu và dán khóa Service Account.")
        st.stop()
    if "private_key" in svc:
        svc["private_key"] = svc["private_key"].replace("\\n", "\n")
    scopes = ["https://www.googleapis.com/auth/spreadsheets", "https://www.googleapis.com/auth/drive"]
    creds = Credentials.from_service_account_info(svc, scopes=scopes)
    return gspread.authorize(creds)

def get_or_create_ws(gc, spreadsheet_id: str, title: str, headers=None):
    sh = gc.open_by_key(spreadsheet_id)
    try:
        ws = sh.worksheet(title)
    except gspread.WorksheetNotFound:
        ws = sh.add_worksheet(title=title, rows=3000, cols=80)
        if headers:
            ws.update("A1", [headers])
    return ws

def load_ws_df(ws, expected_cols=None):
    df = get_as_dataframe(ws, evaluate_formulas=True, header=0)
    if df is None:
        df = pd.DataFrame(columns=expected_cols or [])
    else:
        df = df.dropna(how="all")
        if expected_cols:
            for c in expected_cols:
                if c not in df.columns:
                    df[c] = None
            df = df[expected_cols]
    return df

def save_ws_df(ws, df):
    set_with_dataframe(ws, df, include_index=False, include_column_header=True, resize=True)

def send_email_smtp(to_email, subject, body, attachments=None):
    # Requires SMTP settings in secrets
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
# Connect
# =====================
st.title("🧮 KPI Scorer v3.6 – Định Hóa (CBCNV + Charts + Email)")

with st.sidebar:
    st.subheader("🔐 Google Sheets & Email")
    spreadsheet_id = st.text_input("Spreadsheet ID", value="", placeholder="1A2B3C...")
    default_to_email = st.text_input("Email nhận báo cáo", value="phamlong666@gmail.com")

if not spreadsheet_id:
    st.info("Nhập Spreadsheet ID để bắt đầu.")
    st.stop()

try:
    gc = get_client()
    data_ws = get_or_create_ws(gc, spreadsheet_id, DATA_WS_TITLE, headers=EXPECTED_DATA_COLS)
    emp_ws  = get_or_create_ws(gc, spreadsheet_id, EMP_WS_TITLE, headers=EXPECTED_EMP_COLS)
    asg_ws  = get_or_create_ws(gc, spreadsheet_id, ASSIGN_WS_TITLE, headers=EXPECTED_ASSIGN_COLS)
except Exception as e:
    st.error(f"❌ Lỗi kết nối: {e}")
    st.stop()

# Load data
emp_df  = load_ws_df(emp_ws, expected_cols=EXPECTED_EMP_COLS)
data_df = load_ws_df(data_ws, expected_cols=EXPECTED_DATA_COLS)
asg_df  = load_ws_df(asg_ws, expected_cols=EXPECTED_ASSIGN_COLS)

# =====================
# KPI cá nhân tổng hợp (theo tháng chọn)
# =====================
st.subheader("1) Chọn kỳ báo cáo KPI cá nhân")
colf = st.columns(3)
with colf[0]:
    years = sorted([int(x) for x in pd.to_numeric(data_df["Năm"], errors="coerce").dropna().unique()] + [datetime.now().year])
    sel_year = st.selectbox("Năm", options=years, index=len(years)-1 if years else 0)
with colf[1]:
    sel_month = st.selectbox("Tháng", options=list(range(1,13)), index=max(0, datetime.now().month-1))
with colf[2]:
    dept_filter = st.selectbox("Bộ phận (lọc)", options=["(Tất cả)"] + sorted([str(x) for x in emp_df["Bộ phận"].dropna().unique()]))

# Join assignments with KPI_DATA to fetch task score
kpi_key_cols = ["Bộ phận","Chỉ tiêu (tham chiếu)","Tháng","Năm","Mã CV","Điểm tổng"]
kpi_key = data_df[kpi_key_cols].copy()
asg_join = asg_df.copy()

has_code = asg_join["Mã CV"].notna() & (asg_join["Mã CV"].astype(str).str.strip()!="")
no_code  = ~has_code

merged = pd.DataFrame(columns=list(asg_join.columns) + ["Điểm tổng"])

if has_code.any():
    m1 = asg_join[has_code].merge(
        kpi_key.dropna(subset=["Mã CV"]), on=["Mã CV","Tháng","Năm"], how="left"
    )
    merged = pd.concat([merged, m1], ignore_index=True)

if no_code.any():
    m2 = asg_join[no_code].merge(
        kpi_key.drop(columns=["Mã CV"]), on=["Bộ phận","Chỉ tiêu (tham chiếu)","Tháng","Năm"], how="left"
    )
    merged = pd.concat([merged, m2], ignore_index=True)

# Filter by time & dept
mask_rep = (merged["Năm"]==sel_year) & (merged["Tháng"]==sel_month)
if dept_filter != "(Tất cả)":
    mask_rep = mask_rep & (merged["Bộ phận"]==dept_filter)
merged = merged[mask_rep].copy()

# Compute per-person KPI
merged["Trọng số cá nhân (%)"] = pd.to_numeric(merged["Trọng số cá nhân (%)"], errors="coerce").fillna(0)
merged["Điểm tổng"] = pd.to_numeric(merged["Điểm tổng"], errors="coerce").fillna(0)
merged["Điểm thưởng trực tiếp (CN)"] = pd.to_numeric(merged["Điểm thưởng trực tiếp (CN)"], errors="coerce").fillna(0)
merged["Điểm cá nhân (chưa thưởng)"] = merged["Điểm tổng"] * merged["Trọng số cá nhân (%)"] / 100.0
merged["Điểm cá nhân (cuối)"] = merged["Điểm cá nhân (chưa thưởng)"] + merged["Điểm thưởng trực tiếp (CN)"]

# Attach employee names
per_person = merged.groupby("Mã NV").agg(
    Điểm_KPI_cá_nhân=("Điểm cá nhân (cuối)", "sum")
).reset_index()
per_person = per_person.merge(emp_df[["Mã NV","Họ và tên","Bộ phận","Chức danh"]], on="Mã NV", how="left")
per_person = per_person[["Mã NV","Họ và tên","Bộ phận","Chức danh","Điểm_KPI_cá_nhân"]].sort_values("Điểm_KPI_cá_nhân", ascending=False)

st.subheader(f"2) Bảng KPI cá nhân – Tháng {sel_month:02d}/{sel_year}")
st.dataframe(per_person, use_container_width=True)

# =====================
# Biểu đồ cột KPI cá nhân
# =====================
st.subheader("3) Biểu đồ cột KPI cá nhân (Top/Bottom)")
colc = st.columns(3)
with colc[0]:
    top_n = st.number_input("Số lượng Top", min_value=3, max_value=50, value=10, step=1)
with colc[1]:
    bottom_n = st.number_input("Số lượng Bottom", min_value=3, max_value=50, value=10, step=1)
with colc[2]:
    show_labels = st.checkbox("Hiển thị nhãn giá trị", value=True)

# Top chart
if len(per_person) > 0:
    top_df = per_person.head(int(top_n))
    fig1 = plt.figure(figsize=(10,5))
    plt.bar(top_df["Họ và tên"], top_df["Điểm_KPI_cá_nhân"])
    plt.title(f"Top {int(top_n)} KPI cá nhân – {sel_month:02d}/{sel_year}")
    plt.xticks(rotation=20, ha='right')
    plt.ylabel("Điểm KPI")
    if show_labels:
        for i, v in enumerate(top_df["Điểm_KPI_cá_nhân"]):
            plt.text(i, v, f"{v:.2f}", ha='center', va='bottom', fontsize=8, rotation=0)
    st.pyplot(fig1)

    # Bottom chart
    bot_df = per_person.tail(int(bottom_n)).sort_values("Điểm_KPI_cá_nhân")
    fig2 = plt.figure(figsize=(10,5))
    plt.bar(bot_df["Họ và tên"], bot_df["Điểm_KPI_cá_nhân"])
    plt.title(f"Bottom {int(bottom_n)} KPI cá nhân – {sel_month:02d}/{sel_year}")
    plt.xticks(rotation=20, ha='right')
    plt.ylabel("Điểm KPI")
    if show_labels:
        for i, v in enumerate(bot_df["Điểm_KPI_cá_nhân"]):
            plt.text(i, v, f"{v:.2f}", ha='center', va='bottom', fontsize=8, rotation=0)
    st.pyplot(fig2)
else:
    st.info("Chưa có dữ liệu KPI cá nhân cho kỳ này.")

# =====================
# Xuất Excel & Gửi email
# =====================
st.subheader("4) Xuất & Gửi email")

# Detail + summary workbook
excel_buf = io.BytesIO()
with pd.ExcelWriter(excel_buf, engine="xlsxwriter") as writer:
    merged.to_excel(writer, sheet_name="DETAIL_ASSIGN", index=False)
    per_person.to_excel(writer, sheet_name="KPI_PERSON", index=False)
excel_bytes = excel_buf.getvalue()

colx = st.columns(2)
with colx[0]:
    st.download_button("⬇️ Tải Excel KPI cá nhân", data=excel_bytes, file_name=f"KPI_canhan_{sel_year}_{sel_month:02d}.xlsx")
with colx[1]:
    to_addr = st.text_input("Gửi tới", value=default_to_email)
    subject = f"Báo cáo KPI cá nhân {sel_month:02d}/{sel_year}"
    body = f"Đính kèm báo cáo KPI cá nhân tháng {sel_month:02d}/{sel_year}. Gồm bảng tổng hợp và chi tiết phân công."
    if st.button("📧 Gửi email kèm Excel"):
        ok, msg = send_email_smtp(
            to_email=to_addr,
            subject=subject,
            body=body,
            attachments=[(f"KPI_canhan_{sel_year}_{sel_month:02d}.xlsx", excel_bytes, "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")]
        )
        if ok:
            st.success(f"✅ Đã gửi email tới {to_addr}")
        else:
            st.error(f"❌ Không gửi được email: {msg}")

st.divider()
st.caption("© KPI Scorer v3.6 – Thêm biểu đồ Top/Bottom KPI cá nhân và nút gửi email báo cáo.")
