
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

st.set_page_config(page_title="KPI Scorer v4.0 – Định Hóa (Full Suite, Secure)", layout="wide")

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
# Helpers
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

def get_or_create_ws(gc, spreadsheet_id, title, headers=None):
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
# Connect
# =====================
st.title("🧮 KPI Scorer v4.0 – Định Hóa (Full Suite, Secure)")

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

# ====== Excel sync
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
    sync_month = st.number_input("Tháng", min_value=1, max_value=12, value=1, step=1)
with colF:
    sync_year = st.number_input("Năm", min_value=2000, max_value=2100, value=datetime.now().year, step=1)
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
            data_df = data_df[~((data_df["Bộ phận"]==sync_dept) & (data_df["Tháng"]==sync_month) & (data_df["Năm"]==sync_year))]
        data_df2 = pd.concat([data_df, imp], ignore_index=True)
        save_ws_df(ws_kpi, data_df2[EXPECTED_KPI_COLS])
        st.success(f"✅ Đã đồng bộ {len(imp)} dòng vào KPI_DATA.")
    except Exception as e:
        st.error(f"❌ Lỗi đồng bộ: {e}")

st.divider()

# ====== KPI entry & scoring per department
st.header("1) Nhập KPI theo Tháng/Năm & Bộ phận (tính điểm Nhóm 2 + thưởng)")
col_m, col_y, col_d = st.columns([1,1,2])
with col_m:
    month = st.selectbox("Tháng", options=list(range(1,13)), index=0)
with col_y:
    year = st.selectbox("Năm", options=list(range(datetime.now().year-2, datetime.now().year+3)), index=2)
with col_d:
    dept = st.selectbox("Bộ phận", options=dept_list, index=0)

kpi_df = load_ws_df(ws_kpi, expected_cols=EXPECTED_KPI_COLS)
mask = (kpi_df["Tháng"]==month) & (kpi_df["Năm"]==year) & (kpi_df["Bộ phận"]==dept)
cur = kpi_df[mask].copy()

if cur.empty:
    cur = pd.DataFrame([{
        "Bộ phận": dept, "Vai trò": "Chính", "Trọng số (%)": 100.0,
        "Phương pháp đo": "", "Đơn vị tính": "%", "Chỉ tiêu (tham chiếu)": "",
        "Tháng": month, "Năm": year, "Kế hoạch": None, "Thực hiện": None, "Ngưỡng/Kế hoạch (%)": 1.5,
        "Sai số (%)": None, "Bậc vượt (0.1%)": None, "Bậc giảm (0.1%)": None,
        "Điểm cộng": None, "Điểm trừ": None, "Kết quả (ròng)": None,
        "Điểm thưởng trực tiếp": 0.0, "Lý do thưởng": "", "Điểm tổng": None,
        "Cập nhật lúc": None, "Mã CV": None
    }], columns=EXPECTED_KPI_COLS)

editor = st.data_editor(
    cur, key="kpi_editor", num_rows="dynamic", use_container_width=True,
    column_config={
        "Vai trò": st.column_config.SelectboxColumn("Vai trò", options=["Chính","Phụ"]),
        "Trọng số (%)": st.column_config.NumberColumn(min_value=0, max_value=100, step=1),
        "Đơn vị tính": st.column_config.SelectboxColumn("Đơn vị tính", options=["%","kWh","MWh","GWh","khác"]),
        "Điểm thưởng trực tiếp": st.column_config.NumberColumn(min_value=-5.0, max_value=5.0, step=0.1),
    }
)

if st.button("🧮 Tính & Lưu KPI bộ phận"):
    df_calc = editor.copy()
    df_calc["Tháng"] = month
    df_calc["Năm"] = year
    calc = df_calc.apply(lambda r: compute_points(r, group), axis=1)
    for col in ["Sai số (%)","Bậc vượt (0.1%)","Bậc giảm (0.1%)","Điểm cộng","Điểm trừ","Kết quả (ròng)","Điểm tổng"]:
        df_calc[col] = calc[col]
    df_calc["Cập nhật lúc"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    kpi_df2 = kpi_df[~((kpi_df["Bộ phận"]==dept) & (kpi_df["Tháng"]==month) & (kpi_df["Năm"]==year))].copy()
    kpi_df2 = pd.concat([kpi_df2, df_calc], ignore_index=True)
    try:
        save_ws_df(ws_kpi, kpi_df2[EXPECTED_KPI_COLS])
        st.success("✅ Đã lưu KPI_DATA.")
    except Exception as e:
        st.error(f"❌ Lỗi lưu: {e}")

    st.dataframe(df_calc, use_container_width=True)
    total_weighted = (pd.to_numeric(df_calc["Điểm tổng"], errors="coerce").fillna(0) * pd.to_numeric(df_calc["Trọng số (%)"], errors="coerce").fillna(0) / 100.0).sum()
    st.metric(f"Điểm KPI (đã phân bổ) – {dept}", f"{total_weighted:.2f}")

st.divider()

# ====== Department comparison
st.header("2) So sánh KPI giữa các Bộ phận")
all_kpi = load_ws_df(ws_kpi, expected_cols=EXPECTED_KPI_COLS)
mask_m = (all_kpi["Tháng"]==month) & (all_kpi["Năm"]==year)
month_kpi = all_kpi[mask_m].copy()
if month_kpi.empty:
    st.info("Chưa có dữ liệu tháng này.")
else:
    month_kpi["Điểm tổng (weighted)"] = pd.to_numeric(month_kpi["Điểm tổng"], errors="coerce").fillna(0) * pd.to_numeric(month_kpi["Trọng số (%)"], errors="coerce").fillna(0) / 100.0
    agg = month_kpi.groupby("Bộ phận").agg(
        Diem_tong_weighted=("Điểm tổng (weighted)", "sum"),
        So_chi_tieu_khong_dat=("Sai số (%)", lambda s: (pd.to_numeric(s, errors="coerce").fillna(0) > pd.to_numeric(month_kpi["Ngưỡng/Kế hoạch (%)"], errors="coerce").fillna(1.5)).sum())
    ).reset_index().sort_values("Diem_tong_weighted", ascending=False)
    st.dataframe(agg, use_container_width=True)

    fig = plt.figure(figsize=(10,5))
    plt.bar(agg["Bộ phận"], agg["Diem_tong_weighted"])
    plt.title(f"So sánh KPI (đã phân bổ) – {month:02d}/{year}")
    plt.xticks(rotation=20, ha='right'); plt.ylabel("Điểm")
    st.pyplot(fig)

    dept_excel = io.BytesIO()
    with pd.ExcelWriter(dept_excel, engine="xlsxwriter") as writer:
        month_kpi.to_excel(writer, sheet_name=f"{year}_{month:02d}_DETAIL", index=False)
        agg.to_excel(writer, sheet_name="COMPARE_DEPTS", index=False)
    st.download_button("⬇️ Tải Excel so sánh bộ phận", data=dept_excel.getvalue(), file_name=f"SoSanhBoPhan_{year}_{month:02d}.xlsx")

    pdf_buf = io.BytesIO()
    from matplotlib.backends.backend_pdf import PdfPages
    with PdfPages(pdf_buf) as pdf:
        fig1 = plt.figure(figsize=(8.27, 11.69)); fig1.clf()
        plt.title(f"TÓM TẮT KPI – {month:02d}/{year}")
        tot = round(agg["Diem_tong_weighted"].sum(), 2)
        y = 0.9
        for t in [f"Tổng điểm KPI (toàn đơn vị, đã phân bổ): {tot}", f"Số bộ phận: {len(agg)}", "Top bộ phận:"]:
            plt.text(0.05, y, t, transform=plt.gca().transAxes, fontsize=12); y-=0.06
        for _, r in agg.head(5).iterrows():
            plt.text(0.08, y, f"- {r['Bộ phận']}: {r['Diem_tong_weighted']:.2f} điểm", transform=plt.gca().transAxes, fontsize=11); y-=0.045
        plt.axis('off'); pdf.savefig(fig1); plt.close(fig1)

        fig2 = plt.figure(figsize=(11.69, 8.27)); fig2.clf()
        plt.bar(agg["Bộ phận"], agg["Diem_tong_weighted"])
        plt.title("Biểu đồ cột: Điểm KPI theo Bộ phận")
        plt.xticks(rotation=20, ha='right'); plt.ylabel("Điểm")
        plt.tight_layout(); pdf.savefig(fig2); plt.close(fig2)
    st.download_button("⬇️ Tải PDF tóm tắt bộ phận", data=pdf_buf.getvalue(), file_name=f"BaoCao_BoPhan_{year}_{month:02d}.pdf")

st.divider()

# ====== EMPLOYEES CRUD
st.header("3) Danh sách CBCNV (CRUD + Import)")
ws_emp  = get_or_create_ws(gc, spreadsheet_id, WS_EMPS, headers=EXPECTED_EMP_COLS)
emp_df = load_ws_df(ws_emp, expected_cols=EXPECTED_EMP_COLS)
col1, col2 = st.columns([1,1])
with col1:
    st.caption("Thêm/sửa trực tiếp, đảm bảo 'Mã NV' duy nhất.")
with col2:
    up_emp = st.file_uploader("⬆️ Import CBCNV (.xlsx)", type=["xlsx"], key="emp_up")
    if up_emp and st.button("Nhập file CBCNV"):
        try:
            df_imp = pd.read_excel(up_emp)
            for c in EXPECTED_EMP_COLS:
                if c not in df_imp.columns: df_imp[c] = None
            df_imp = df_imp[EXPECTED_EMP_COLS]
            save_ws_df(ws_emp, df_imp)
            st.success(f"✅ Đã nhập {len(df_imp)} dòng.")
            emp_df = df_imp
        except Exception as e:
            st.error(f"❌ Lỗi import: {e}")
emp_editor = st.data_editor(emp_df, key="emp_editor", num_rows="dynamic", use_container_width=True)
if st.button("💾 Lưu CBCNV"):
    tmp = emp_editor.copy()
    if tmp["Mã NV"].isna().any() or (tmp["Mã NV"].astype(str).str.strip()=="").any():
        st.error("Thiếu 'Mã NV'."); st.stop()
    if tmp["Mã NV"].duplicated().any():
        st.error("Trùng 'Mã NV'."); st.stop()
    save_ws_df(ws_emp, tmp[EXPECTED_EMP_COLS])
    st.success("✅ Đã lưu CBCNV.")

st.divider()

# ====== ASSIGNMENTS (per-person)
st.header("4) Phân công công việc cho cá nhân")
data_df = load_ws_df(ws_kpi, expected_cols=EXPECTED_KPI_COLS)
ws_asg  = get_or_create_ws(gc, spreadsheet_id, WS_ASG, headers=EXPECTED_ASG_COLS)
asg_df = load_ws_df(ws_asg, expected_cols=EXPECTED_ASG_COLS)

colf = st.columns(4)
with colf[0]:
    sel_year = st.selectbox("Năm", options=sorted([int(x) for x in pd.to_numeric(data_df["Năm"], errors="coerce").dropna().unique()] + [datetime.now().year]))
with colf[1]:
    sel_month = st.selectbox("Tháng", options=list(range(1,13)))
with colf[2]:
    sel_dept = st.selectbox("Bộ phận (lọc)", options=["(Tất cả)"] + dept_list)
with colf[3]:
    sel_task_code = st.text_input("Mã CV (tùy chọn)", value="")

mask = (data_df["Năm"]==sel_year) & (data_df["Tháng"]==sel_month)
if sel_dept != "(Tất cả)": mask &= (data_df["Bộ phận"]==sel_dept)
kpi_cur = data_df[mask].copy()
st.write("Các dòng KPI (lọc theo trên):")
st.dataframe(kpi_cur[["Mã CV","Bộ phận","Chỉ tiêu (tham chiếu)","Điểm tổng"]], use_container_width=True)

asg_mask = (asg_df["Năm"]==sel_year) & (asg_df["Tháng"]==sel_month)
if sel_dept != "(Tất cả)": asg_mask &= (asg_df["Bộ phận"]==sel_dept)
if sel_task_code.strip(): asg_mask &= (asg_df["Mã CV"].astype(str)==sel_task_code.strip())
asg_cur = asg_df[asg_mask].copy()

if asg_cur.empty and len(kpi_cur)>0:
    first = kpi_cur.iloc[0]
    asg_cur = pd.DataFrame([{
        "Mã CV": first.get("Mã CV"),
        "Chỉ tiêu (tham chiếu)": first.get("Chỉ tiêu (tham chiếu)"),
        "Bộ phận": first.get("Bộ phận"),
        "Tháng": first.get("Tháng"),
        "Năm": first.get("Năm"),
        "Mã NV": None,
        "Vai trò (Cá nhân)": "Phối hợp",
        "Trọng số cá nhân (%)": 100,
        "Điểm thưởng trực tiếp (CN)": 0.0,
        "Lý do thưởng (CN)": "",
        "Cập nhật lúc": None
    }], columns=EXPECTED_ASG_COLS)

emp_df = load_ws_df(ws_emp, expected_cols=EXPECTED_EMP_COLS)
asg_editor = st.data_editor(
    asg_cur, key="asg_editor", num_rows="dynamic", use_container_width=True,
    column_config={
        "Mã NV": st.column_config.SelectboxColumn("Mã NV", options=emp_df["Mã NV"].dropna().astype(str).tolist(), required=True),
        "Vai trò (Cá nhân)": st.column_config.SelectboxColumn("Vai trò (Cá nhân)", options=["Chủ trì","Phối hợp"]),
        "Trọng số cá nhân (%)": st.column_config.NumberColumn(min_value=0, max_value=100, step=1),
        "Điểm thưởng trực tiếp (CN)": st.column_config.NumberColumn(min_value=-5.0, max_value=5.0, step=0.1),
    }
)

c1, c2 = st.columns([1,1])
with c1:
    if st.button("💾 Lưu phân công"):
        try:
            tmp = asg_editor.copy()
            tmp["Tháng"] = sel_month; tmp["Năm"] = sel_year
            tmp["Cập nhật lúc"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            df_to_save = asg_df[~asg_mask].copy()
            df_to_save = pd.concat([df_to_save, tmp[EXPECTED_ASG_COLS]], ignore_index=True)
            save_ws_df(ws_asg, df_to_save[EXPECTED_ASG_COLS]); st.success("✅ Đã lưu phân công.")
        except Exception as e:
            st.error(f"❌ Lỗi lưu phân công: {e}")
with c2:
    st.caption("App sẽ cảnh báo nếu tổng trọng số cá nhân cho **cùng 1 công việc** ≠ 100%.")

st.divider()

# ====== Per-person KPI
st.header("5) KPI cá nhân (Top/Bottom + Email)")
asg_df = load_ws_df(ws_asg, expected_cols=EXPECTED_ASG_COLS)  # reload after save
kpi_key_cols = ["Bộ phận","Chỉ tiêu (tham chiếu)","Tháng","Năm","Mã CV","Điểm tổng"]
kpi_key = data_df[kpi_key_cols].copy()
has_code = asg_df["Mã CV"].notna() & (asg_df["Mã CV"].astype(str).str.strip()!="")
no_code  = ~has_code

merged = pd.DataFrame(columns=list(asg_df.columns) + ["Điểm tổng"])
if has_code.any():
    m1 = asg_df[has_code].merge(kpi_key.dropna(subset=["Mã CV"]), on=["Mã CV","Tháng","Năm"], how="left")
    merged = pd.concat([merged, m1], ignore_index=True)
if no_code.any():
    m2 = asg_df[no_code].merge(kpi_key.drop(columns=["Mã CV"]), on=["Bộ phận","Chỉ tiêu (tham chiếu)","Tháng","Năm"], how="left")
    merged = pd.concat([merged, m2], ignore_index=True)

colp = st.columns(3)
with colp[0]:
    rep_year = st.selectbox("Năm báo cáo", options=sorted([int(x) for x in pd.to_numeric(merged["Năm"], errors="coerce").dropna().unique()] + [year]), index=0)
with colp[1]:
    rep_month = st.selectbox("Tháng báo cáo", options=list(range(1,13)), index=month-1 if 1<=month<=12 else 0)
with colp[2]:
    rep_dept = st.selectbox("Bộ phận (lọc)", options=["(Tất cả)"] + dept_list)

mask_rep = (merged["Năm"]==rep_year) & (merged["Tháng"]==rep_month)
if rep_dept != "(Tất cả)": mask_rep &= (merged["Bộ phận"]==rep_dept)
merged = merged[mask_rep].copy()

merged["Trọng số cá nhân (%)"] = pd.to_numeric(merged["Trọng số cá nhân (%)"], errors="coerce").fillna(0)
merged["Điểm tổng"] = pd.to_numeric(merged["Điểm tổng"], errors="coerce").fillna(0)
merged["Điểm thưởng trực tiếp (CN)"] = pd.to_numeric(merged["Điểm thưởng trực tiếp (CN)"], errors="coerce").fillna(0)
merged["Điểm cá nhân (chưa thưởng)"] = merged["Điểm tổng"] * merged["Trọng số cá nhân (%)"] / 100.0
merged["Điểm cá nhân (cuối)"] = merged["Điểm cá nhân (chưa thưởng)"] + merged["Điểm thưởng trực tiếp (CN)"]

wcheck = merged.groupby(["Mã CV","Chỉ tiêu (tham chiếu)","Bộ phận","Tháng","Năm"], dropna=False)["Trọng số cá nhân (%)"].sum().reset_index(name="Tổng trọng số (%)")
bad = wcheck[(wcheck["Tổng trọng số (%)"].round(2) != 100.00)]
if len(bad)>0:
    st.warning("⚠️ Có công việc có tổng **Trọng số cá nhân (%)** ≠ 100%. Vui lòng rà soát:")
    st.dataframe(bad, use_container_width=True)

per_person = merged.groupby("Mã NV").agg(Điểm_KPI_cá_nhân=("Điểm cá nhân (cuối)","sum")).reset_index()
emp_df2 = load_ws_df(ws_emp, expected_cols=EXPECTED_EMP_COLS)
per_person = per_person.merge(emp_df2[["Mã NV","Họ và tên","Bộ phận","Chức danh"]], on="Mã NV", how="left")
per_person = per_person[["Mã NV","Họ và tên","Bộ phận","Chức danh","Điểm_KPI_cá_nhân"]].sort_values("Điểm_KPI_cá_nhân", ascending=False)

st.subheader(f"Bảng KPI cá nhân – {rep_month:02d}/{rep_year}")
st.dataframe(per_person, use_container_width=True)

colc = st.columns(3)
with colc[0]:
    top_n = st.number_input("Top N", min_value=3, max_value=50, value=10, step=1)
with colc[1]:
    bottom_n = st.number_input("Bottom N", min_value=3, max_value=50, value=10, step=1)
with colc[2]:
    show_labels = st.checkbox("Hiển thị nhãn", value=True)

if len(per_person)>0:
    top_df = per_person.head(int(top_n))
    fig1 = plt.figure(figsize=(10,5)); plt.bar(top_df["Họ và tên"], top_df["Điểm_KPI_cá_nhân"])
    plt.title(f"Top {int(top_n)} KPI cá nhân – {rep_month:02d}/{rep_year}"); plt.xticks(rotation=20, ha='right'); plt.ylabel("Điểm")
    if show_labels:
        for i, v in enumerate(top_df["Điểm_KPI_cá_nhân"]): plt.text(i, v, f"{v:.2f}", ha='center', va='bottom', fontsize=8)
    st.pyplot(fig1)

    bot_df = per_person.tail(int(bottom_n)).sort_values("Điểm_KPI_cá_nhân")
    fig2 = plt.figure(figsize=(10,5)); plt.bar(bot_df["Họ và tên"], bot_df["Điểm_KPI_cá_nhân"])
    plt.title(f"Bottom {int(bottom_n)} KPI cá nhân – {rep_month:02d}/{rep_year}"); plt.xticks(rotation=20, ha='right'); plt.ylabel("Điểm")
    if show_labels:
        for i, v in enumerate(bot_df["Điểm_KPI_cá_nhân"]): plt.text(i, v, f"{v:.2f}", ha='center', va='bottom', fontsize=8)
    st.pyplot(fig2)
else:
    st.info("Chưa có dữ liệu KPI cá nhân cho kỳ này.")

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
    ok, msg = send_email_smtp(to_email=to_addr, subject=subject, body=body, attachments=[(f"KPI_canhan_{rep_year}_{rep_month:02d}.xlsx", excel_bytes, "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")])
    if ok: st.success(f"✅ Đã gửi email tới {to_addr}")
    else: st.error(f"❌ Không gửi được email: {msg}")

st.divider()
st.caption("© KPI Scorer v4.0 (Secure) – Đồng bộ Excel, chấm KPI Nhóm 2 + thưởng, so sánh bộ phận, CBCNV & phân công, KPI cá nhân, xuất Excel/PDF & email.")
