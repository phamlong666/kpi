from pathlib import Path

FINAL_APP = r'''# -*- coding: utf-8 -*-
"""
KPI App – Định Hóa (v2.3 FINAL)
- Đăng nhập bắt buộc: sau khi đăng nhập ẩn hẳn form, chỉ còn lời chào + nút Đăng xuất.
- Quên mật khẩu: sinh MK tạm 10 ký tự -> cập nhật Google Sheet (tab USE, cột "Mật khẩu mặc định") -> gửi email tới phamlong666@gmail.com.
- Đổi mật khẩu: chính chủ (có MK cũ) hoặc Admin (không cần MK cũ) -> cập nhật Google Sheet -> gửi email xác nhận.
- KPI: Bảng KPI (lọc, export), Nhập CSV vào KPI, Quản trị.
- Đã xử lý so khớp USE không phân biệt hoa/thường, bỏ khoảng trắng thừa.
"""
import re
import io
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
import random
import string
import streamlit as st
import pandas as pd
import gspread
from google.oauth2.service_account import Credentials

# ================ CẤU HÌNH ================
st.set_page_config(page_title="KPI – Định Hóa", layout="wide")
APP_TITLE = "📊 KPI – Đội quản lý Điện lực khu vực Định Hóa"
GOOGLE_SHEET_ID_DEFAULT = "1nXFKJrn8oHwQgUzv5QYihoazYRhhS1PeN-xyo7Er2iM"
ADMIN_ACCOUNTS = {r"pctn\\admin", r"npc\\longph"}
FORGOT_TARGET_EMAIL = "phamlong666@gmail.com"  # cố định theo yêu cầu

# ================ TIỆN ÍCH ================
def is_admin(username: str) -> bool:
    return bool(username) and username.strip().lower() in ADMIN_ACCOUNTS

def toast(msg, icon="ℹ️"):
    try:
        st.toast(msg, icon=icon)
    except Exception:
        pass

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
        return gspread.authorize(creds)
    except Exception as e:
        st.session_state["_gs_error"] = f"SECRETS_ERROR: {e}"
        return None

def open_spreadsheet(sid_or_url: str):
    sid = extract_sheet_id(sid_or_url or GOOGLE_SHEET_ID_DEFAULT) or GOOGLE_SHEET_ID_DEFAULT
    client = st.session_state.get("_gs_client") or get_gs_client()
    st.session_state["_gs_client"] = client
    if client is None:
        raise RuntimeError("no_client")
    return client.open_by_key(sid)

def df_from_ws(ws) -> pd.DataFrame:
    records = ws.get_all_records(expected_headers=ws.row_values(1))
    return pd.DataFrame(records)

# Chuẩn hoá tên cột
ALIAS = {
    "USE (mã đăng nhập)": [
        "USE (mã đăng nhập)",
        r"Tài khoản (USE\\username)",
        "Tài khoản (USE/username)",
        "Tài khoản", "Username", "USE", "User"
    ],
    "Mật khẩu mặc định": [
        "Mật khẩu mặc định","Password mặc định","Password","Mật khẩu","Mat khau mac dinh"
    ],
    # KPI
    "Tên chỉ tiêu (KPI)": ["Tên chỉ tiêu (KPI)","Tên KPI","Chỉ tiêu"],
    "Đơn vị tính": ["Đơn vị tính","Unit"],
    "Kế hoạch": ["Kế hoạch","Plan","Target"],
    "Thực hiện": ["Thực hiện","Thực hiện (tháng)","Actual (month)"],
    "Trọng số": ["Trọng số","Weight"],
    "Bộ phận/người phụ trách": ["Bộ phận/người phụ trách","Phụ trách"],
    "Tháng": ["Tháng","Month"],
    "Năm": ["Năm","Year"],
    "Điểm KPI": ["Điểm KPI","Score"],
    "Ghi chú": ["Ghi chú","Notes"],
    "Tên đơn vị": ["Tên đơn vị","Đơn vị"],
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
    return df

def find_use_ws_and_cols(sh):
    """Trả về (ws, idx_col_use, idx_col_pwd, headers)."""
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
    client = get_gs_client()
    if client is not None and sid:
        try:
            sh = client.open_by_key(sid)
            ws, _, _, _ = find_use_ws_and_cols(sh)
            return df_from_ws(ws)
        except Exception as e:
            st.session_state["_gs_error"] = f"OPEN_ERROR: {e}"
    try:
        return pd.read_excel("USE.xlsx", sheet_name="USE")
    except Exception:
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

def generate_temp_password(n=10) -> str:
    chars = string.ascii_letters + string.digits
    return "".join(random.choice(chars) for _ in range(n))

def update_password_on_sheet(user_use: str, new_password: str, spreadsheet_id_or_url: str = "") -> dict:
    """Cập nhật MK trên sheet. Trả về dict {'ok':bool, 'row':int|None, 'col_pwd':int|None, 'message':str}"""
    diag = {'ok': False, 'row': None, 'col_pwd': None, 'message': ""}
    try:
        sh = open_spreadsheet(spreadsheet_id_or_url or GOOGLE_SHEET_ID_DEFAULT)
        ws, idx_use, idx_pwd, headers = find_use_ws_and_cols(sh)
        values = ws.col_values(idx_use)
        row_number = None
        needle = str(user_use).strip().lower()
        for i, v in enumerate(values, start=1):
            if i == 1:  # header
                continue
            if str(v).strip().lower() == needle:
                row_number = i
                break
        if not row_number:
            diag['message'] = "Không tìm thấy USE trên sheet."
            return diag
        ws.update_cell(row_number, idx_pwd, new_password)
        diag.update({'ok': True, 'row': row_number, 'col_pwd': idx_pwd, 'message': "Đã cập nhật MK trên sheet."})
        return diag
    except Exception as e:
        diag['message'] = f"Lỗi cập nhật sheet: {e}"
        return diag

def send_email(subject: str, body: str, to_email: str) -> dict:
    """Gửi email; trả dict {'ok':bool,'mode':'smtp|mock','message':str}"""
    try:
        user = st.secrets["email"]["EMAIL_USER"]
        pwd  = st.secrets["email"]["EMAIL_PASS"]
        server_name = st.secrets["email"].get("SMTP_SERVER", "smtp.gmail.com")
        port = int(st.secrets["email"].get("SMTP_PORT", 465))
    except Exception:
        toast("(Giả lập) Đã gửi email: " + subject, "✉️")
        return {'ok': True, 'mode': 'mock', 'message': 'Giả lập gửi email (thiếu secrets[email]).'}

    try:
        msg = MIMEMultipart()
        msg["Subject"] = subject
        msg["From"] = user
        msg["To"] = to_email
        msg.attach(MIMEText(body, "plain", "utf-8"))

        if port == 465:
            with smtplib.SMTP_SSL(server_name, port) as server:
                server.login(user, pwd)
                server.sendmail(user, [to_email], msg.as_string())
        else:
            with smtplib.SMTP(server_name, port) as server:
                server.starttls()
                server.login(user, pwd)
                server.sendmail(user, [to_email], msg.as_string())
        return {'ok': True, 'mode': 'smtp', 'message': 'Đã gửi email bằng SMTP.'}
    except Exception as e:
        return {'ok': False, 'mode': 'smtp', 'message': f"Lỗi gửi email: {e}"}

# ================ SIDEBAR (LOGIN/LOGOUT) ================
with st.sidebar:
    st.header("🔒 Đăng nhập")

    if "_user" not in st.session_state:
        # Form đăng nhập
        use_input = st.text_input("USE (vd: PCTN\\KVDHA)", key="login_use")
        pwd_input = st.text_input("Mật khẩu", type="password", key="login_pwd")
        c1, c3 = st.columns([1,1])
        with c1:
            login_clicked = st.button("Đăng nhập", use_container_width=True, type="primary", key="btn_login")
        with c3:
            forgot_use = st.text_input("USE để cấp MK tạm", key="forgot_use")
            forgot_clicked = st.button("Quên mật khẩu", use_container_width=True, key="btn_forgot")

        if login_clicked:
            df_users = load_users(st.session_state.get("spreadsheet_id",""))
            if check_credentials(df_users, use_input, pwd_input):
                st.session_state["_user"] = use_input
                toast("Chào mừng bạn vào làm việc, chúc bạn luôn vui vẻ nhé! 🌟", "✅")
                st.rerun()

        if forgot_clicked:
            u = (forgot_use or "").strip()
            if not u:
                toast("Nhập USE trước khi bấm 'Quên mật khẩu'.", "❗")
            else:
                temp_pw = generate_temp_password(10)
                res_sheet = update_password_on_sheet(u, temp_pw, st.session_state.get("spreadsheet_id",""))
                subject = f"[KPI Định Hóa] Mật khẩu tạm cho {u}"
                body = f"Chào anh/chị,\n\nHệ thống KPI đã tạo mật khẩu tạm cho tài khoản: {u}\nMật khẩu tạm: {temp_pw}\n\nVui lòng đăng nhập và đổi mật khẩu ngay trong mục Quản trị.\nTrân trọng."
                res_mail = send_email(subject, body, FORGOT_TARGET_EMAIL)

                if res_sheet['ok'] and res_mail['ok']:
                    st.success(f"✅ ĐÃ CẤP MẬT KHẨU TẠM cho USE: {u}. (Sheet dòng {res_sheet['row']}, cột MK {res_sheet['col_pwd']}; Email: {res_mail['mode']})")
                elif res_sheet['ok'] and not res_mail['ok']:
                    st.warning(f"MK tạm đã cập nhật trên Sheet (dòng {res_sheet['row']}), nhưng email lỗi: {res_mail['message']}")
                elif not res_sheet['ok'] and res_mail['ok']:
                    st.warning(f"ĐÃ GỬI EMAIL mật khẩu tạm, nhưng cập nhật Sheet thất bại: {res_sheet['message']}")
                else:
                    st.error(f"Không cấp được MK tạm. Lỗi Sheet: {res_sheet['message']} | Lỗi email: {res_mail['message']}")

    else:
        # Sau khi đăng nhập: KHÔNG hiển thị form đăng nhập nữa
        st.success("Chào mừng bạn vào làm việc, chúc bạn luôn vui vẻ nhé!")
        st.write(f"👤 Đang đăng nhập: **{st.session_state['_user']}**")
        logout_clicked = st.button("Đăng xuất", use_container_width=True, key="btn_logout")
        if logout_clicked:
            st.session_state.pop("_user", None)
            toast("Đã đăng xuất.", "✅")
            st.rerun()

        # Quản trị nhanh (cấu hình Sheet) chỉ hiển thị khi đã đăng nhập
        st.markdown("---")
        st.header("⚙️ Cấu hình Sheet")
        sid_val = st.text_input("Google Sheet ID/URL", value=st.session_state.get("spreadsheet_id",""))
        st.session_state["spreadsheet_id"] = sid_val
        kpi_sheet_name = st.text_input("Tên sheet KPI", value=st.session_state.get("kpi_sheet_name","KPI"))
        st.session_state["kpi_sheet_name"] = kpi_sheet_name

        # Đổi mật khẩu (chính chủ)
        with st.expander("🔐 Đổi mật khẩu (Chính chủ)"):
            old_pw_me = st.text_input("Mật khẩu hiện tại", type="password", key="me_old")
            new_pw_me = st.text_input("Mật khẩu mới", type="password", key="me_new")
            new_pw2_me = st.text_input("Nhập lại mật khẩu mới", type="password", key="me_new2")
            me_change = st.button("Cập nhật mật khẩu của tôi", type="primary", key="me_change_btn")
            if me_change:
                df_users = load_users(st.session_state.get("spreadsheet_id",""))
                if not check_credentials(df_users, st.session_state["_user"], old_pw_me):
                    st.error("Mật khẩu hiện tại không đúng.")
                elif not new_pw_me or new_pw_me != new_pw2_me:
                    st.error("Mật khẩu mới không khớp.")
                else:
                    res_sheet = update_password_on_sheet(st.session_state["_user"], new_pw_me, st.session_state.get("spreadsheet_id",""))
                    if res_sheet['ok']:
                        st.success("✅ Đã đổi mật khẩu thành công (đã cập nhật Google Sheet).")
                        try:
                            send_email("[KPI Định Hóa] Đổi mật khẩu thành công",
                                       f"Tài khoản {st.session_state['_user']} vừa đổi mật khẩu thành công.",
                                       FORGOT_TARGET_EMAIL)
                        except Exception:
                            pass
                    else:
                        st.error(f"Đổi mật khẩu thất bại: {res_sheet['message']}")

        # Đổi mật khẩu cho user khác (Admin)
        if is_admin(st.session_state["_user"]):
            with st.expander("🛠 Đổi mật khẩu cho người dùng (Admin)"):
                target_use = st.text_input("USE cần đổi", value="", key="admin_target")
                new_pw_adm = st.text_input("Mật khẩu mới", type="password", key="adm_new")
                apply_clicked = st.button("Áp dụng", type="primary", key="adm_apply")
                if apply_clicked:
                    if not target_use or not new_pw_adm:
                        st.error("Nhập đủ USE và mật khẩu mới.")
                    else:
                        res_sheet = update_password_on_sheet(target_use, new_pw_adm, st.session_state.get("spreadsheet_id",""))
                        if res_sheet['ok']:
                            st.success(f"✅ Đã đổi mật khẩu cho {target_use} (dòng {res_sheet['row']}).")
                            try:
                                send_email("[KPI Định Hóa] Admin đổi mật khẩu",
                                           f"Admin đã đổi mật khẩu cho tài khoản {target_use}.",
                                           FORGOT_TARGET_EMAIL)
                            except Exception:
                                pass
                        else:
                            st.error(f"Đổi mật khẩu thất bại: {res_sheet['message']}")

# ================ GATING CHÍNH ================
st.title(APP_TITLE)
if "_user" not in st.session_state:
    st.info("Vui lòng đăng nhập để làm việc.")
    st.stop()

# ================ KPI CORE ================
KPI_COLS = ["Tên chỉ tiêu (KPI)","Đơn vị tính","Kế hoạch","Thực hiện","Trọng số",
            "Bộ phận/người phụ trách","Tháng","Năm","Điểm KPI","Ghi chú","Tên đơn vị"]

def safe_float(x):
    try:
        s = str(x).replace(",",".")
        return float(s)
    except Exception:
        return None

def compute_score(row):
    plan = safe_float(row.get("Kế hoạch"))
    actual = safe_float(row.get("Thực hiện") or row.get("Thực hiện (tháng)"))
    weight = safe_float(row.get("Trọng số")) or 0.0
    if plan in (None,0) or actual is None: return None
    ratio = max(min(actual/plan, 2.0), 0.0)
    w = weight/100.0 if weight and weight>1 else (weight or 0.0)
    return round(ratio*10*w, 2)

def read_kpi_from_sheet(sh, sheet_name: str):
    try:
        ws = sh.worksheet(sheet_name)
    except Exception:
        ws = None
        for w in sh.worksheets():
            hdr = [h.strip() for h in w.row_values(1)]
            if ("Kế hoạch" in hdr) and ("Thực hiện" in hdr or "Thực hiện (tháng)" in hdr):
                ws = w; break
        if ws is None: return pd.DataFrame()
    df = df_from_ws(ws)
    df = normalize_columns(df)
    if "Thực hiện (tháng)" in df.columns and "Thực hiện" not in df.columns:
        df = df.rename(columns={"Thực hiện (tháng)":"Thực hiện"})
    if "Điểm KPI" not in df.columns:
        df["Điểm KPI"] = df.apply(compute_score, axis=1)
    return df

def write_kpi_to_sheet(sh, sheet_name: str, df: pd.DataFrame):
    df = df.copy()
    df = normalize_columns(df)
    if "Thực hiện (tháng)" in df.columns and "Thực hiện" not in df.columns:
        df = df.rename(columns={"Thực hiện (tháng)":"Thực hiện"})
    if "Điểm KPI" not in df.columns:
        df["Điểm KPI"] = df.apply(compute_score, axis=1)
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
    sheet_name = st.session_state.get("kpi_sheet_name","KPI")
    sh = open_spreadsheet(sid_cfg)
    return sh, sheet_name

tab1, tab2 = st.tabs(["📋 Bảng KPI","⬆️ Nhập CSV vào KPI"])

with tab1:
    st.subheader("Bảng KPI")
    try:
        sh, sheet_name = get_sheet_and_name()
        df_kpi = read_kpi_from_sheet(sh, sheet_name)
    except Exception as e:
        st.error(f"Không đọc được KPI: {e}")
        df_kpi = pd.DataFrame()

    if not df_kpi.empty:
        months = ["Tất cả"] + sorted(df_kpi.get("Tháng", pd.Series(dtype=str)).dropna().astype(str).unique().tolist())
        years  = ["Tất cả"] + sorted(df_kpi.get("Năm", pd.Series(dtype=str)).dropna().astype(str).unique().tolist())
        colf1, colf2, colf3 = st.columns([1,1,2])
        with colf1:
            m = st.selectbox("Tháng", options=months, index=0)
        with colf2:
            y = st.selectbox("Năm", options=years, index=0)
        if m!="Tất cả" and "Tháng" in df_kpi.columns: df_kpi = df_kpi[df_kpi["Tháng"].astype(str)==str(m)]
        if y!="Tất cả" and "Năm" in df_kpi.columns:   df_kpi = df_kpi[df_kpi["Năm"].astype(str)==str(y)]

        if "Tên đơn vị" in df_kpi.columns:
            units = ["Tất cả"] + sorted(df_kpi["Tên đơn vị"].dropna().astype(str).unique().tolist())
            unit = st.selectbox("Đơn vị", options=units, index=0)
            if unit!="Tất cả": df_kpi = df_kpi[df_kpi["Tên đơn vị"].astype(str)==unit]

        if "Điểm KPI" in df_kpi.columns:
            if st.checkbox("Sắp xếp theo Điểm KPI (giảm dần)", True):
                df_kpi = df_kpi.sort_values(by="Điểm KPI", ascending=False)

        st.dataframe(df_kpi, use_container_width=True, hide_index=True)

        buf = io.BytesIO()
        with pd.ExcelWriter(buf, engine="xlsxwriter") as writer:
            df_kpi.to_excel(writer, sheet_name="KPI", index=False)
        st.download_button("⬇️ Tải Excel", data=buf.getvalue(), file_name="KPI_export.xlsx",
                           mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")
    else:
        st.info("Chưa có dữ liệu KPI hoặc Admin chưa cấu hình sheet.")

with tab2:
    st.subheader("Nhập CSV vào KPI")
    st.caption("CSV gợi ý các cột: 'Tên chỉ tiêu (KPI)', 'Đơn vị tính', 'Kế hoạch', 'Thực hiện', 'Trọng số', 'Bộ phận/người phụ trách', 'Tháng', 'Năm', 'Ghi chú', 'Tên đơn vị'.")
    up = st.file_uploader("Tải file CSV", type=["csv"])
    if up is not None:
        try:
            df_csv = pd.read_csv(up)
        except Exception:
            up.seek(0)
            df_csv = pd.read_csv(up, encoding="utf-8-sig")
        df_csv = normalize_columns(df_csv)
        if "Thực hiện (tháng)" in df_csv.columns and "Thực hiện" not in df_csv.columns:
            df_csv = df_csv.rename(columns={"Thực hiện (tháng)":"Thực hiện"})
        if "Điểm KPI" not in df_csv.columns:
            df_csv["Điểm KPI"] = df_csv.apply(compute_score, axis=1)
        st.dataframe(df_csv, use_container_width=True, hide_index=True)

        save_clicked = st.button("💾 Ghi vào sheet KPI", use_container_width=True, type="primary")
        if save_clicked:
            try:
                sh, sheet_name = get_sheet_and_name()
                ok = write_kpi_to_sheet(sh, sheet_name, df_csv)
                if ok: toast("Đã ghi dữ liệu CSV vào sheet KPI.", "✅")
            except Exception as e:
                st.error(f"Lưu thất bại: {e}")
'''

Path("/mnt/data/app.py").write_text(FINAL_APP, encoding="utf-8")
print("Wrote FINAL app.py v2.3 (~{} KB)".format(round(len(FINAL_APP.encode('utf-8'))/1024,1)))
