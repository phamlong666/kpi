# -*- coding: utf-8 -*-
"""
app.py — KPI (one-file)
- Đăng nhập theo tài khoản định dạng USE\username
- Đồng bộ tự động từ sheet "USE" (các cột: STT, Tên đơn vị, USE (mã đăng nhập), Mật khẩu mặc định)
  -> tạo/ghi sheet "Users" (hash SHA256), gán vai trò admin cho hàng có Tên đơn vị = Admin
- Nhập KPI từ CSV/Excel, lưu vào sheet KPI_DB (USE/Đơn vị/Tháng/Năm)
- Báo cáo: tháng hiện tại, so tháng trước, so cùng kỳ
- Bảo mật: giải mã Service Account từ secrets (fernet_key + gsa_enc)
"""

import streamlit as st
import pandas as pd
import gspread
from oauth2client.service_account import ServiceAccountCredentials
from datetime import datetime
from io import BytesIO
from typing import List
import hashlib, uuid
import base64, json
from cryptography.fernet import Fernet

# =========================
# 0) Page & CSS
# =========================
st.set_page_config(page_title="KPI – EVN USE", layout="wide")
st.markdown(
    """
    <style>
    .block-container { padding-top: 0.8rem; }
    .section-title{font-size:22px;margin:.5rem 0 .3rem}
    .stButton>button{border-radius:12px}
    </style>
    """,
    unsafe_allow_html=True,
)

st.title("📊 Hệ thống KPI (bản một file)")

# =========================
# 1) Helpers
# =========================

def hash_pw(p: str) -> str:
    return hashlib.sha256((p or "").encode()).hexdigest()

def verify_pw(plain: str, hashed: str) -> bool:
    try:
        return hash_pw(plain) == str(hashed)
    except Exception:
        return False

# Load Service Account from encrypted secrets

def load_sa_from_secret():
    gsa_enc_b64 = st.secrets.get("gsa_enc")
    fernet_key  = st.secrets.get("fernet_key")
    if not gsa_enc_b64 or not fernet_key:
        return None
    blob = base64.b64decode(gsa_enc_b64.encode())
    sa_bytes = Fernet(fernet_key.encode()).decrypt(blob)
    sa = json.loads(sa_bytes.decode())
    if "private_key" in sa:
        sa["private_key"] = sa["private_key"].replace("\\n", "\n")
    return sa

@st.cache_resource(show_spinner=False)
def get_client():
    sa = load_sa_from_secret()
    if not sa:
        return None, "Thiếu secrets fernet_key/gsa_enc"
    try:
        scope = [
            "https://spreadsheets.google.com/feeds",
            "https://www.googleapis.com/auth/drive",
        ]
        creds = ServiceAccountCredentials.from_json_keyfile_dict(sa, scope)
        return gspread.authorize(creds), None
    except Exception as e:
        return None, f"Lỗi Google auth: {e}"

client, client_err = get_client()
connected = client is not None
if not connected:
    st.error(client_err or "Chưa cấu hình Service Account")

# =========================
# 2) Sheet helpers
# =========================

def open_ws(spreadsheet_id: str, sheet_name: str):
    sh = client.open_by_key(spreadsheet_id)
    try:
        return sh.worksheet(sheet_name)
    except Exception:
        return sh.add_worksheet(title=sheet_name, rows=2000, cols=100)


def ensure_headers(ws, headers: List[str]):
    try:
        cur = ws.row_values(1)
    except Exception:
        cur = []
    if cur != headers:
        ws.clear(); ws.append_row(headers, value_input_option="RAW")


def ws_to_df(ws) -> pd.DataFrame:
    rows = ws.get_all_values()
    if not rows: return pd.DataFrame()
    return pd.DataFrame(rows[1:], columns=rows[0])


def df_to_ws(ws, df: pd.DataFrame):
    ws.clear(); ws.append_row(list(df.columns), value_input_option="RAW")
    if not df.empty:
        ws.append_rows(df.astype(str).values.tolist(), value_input_option="RAW")

# =========================
# 3) Sidebar: kết nối + đăng nhập + bootstrap Users
# =========================
with st.sidebar:
    st.subheader("🔗 Kết nối dữ liệu")
    spreadsheet_id = st.text_input("Spreadsheet ID", value=st.session_state.get("sheet_id", ""))
    st.session_state["sheet_id"] = spreadsheet_id

    st.caption("Dán phần giữa của URL Google Sheet. Ví dụ: https://docs.google.com/spreadsheets/d/**THIS_ID**/edit…")

    st.divider()
    st.subheader("🔐 Đăng nhập")
    acc_input = st.text_input("Tài khoản (USE\\username)", value=st.session_state.get("auth_acc", ""))
    pw_input  = st.text_input("Mật khẩu", type="password")

    if st.button("Đăng nhập", use_container_width=True):
        if not connected or not spreadsheet_id:
            st.error("Chưa kết nối Google Sheets/ID rỗng")
        else:
            try:
                ws = open_ws(spreadsheet_id, "Users")
                ensure_headers(ws, ["USE","Tài khoản (USE\\username)","Họ tên","Email","Mật khẩu_băm","Vai trò","Kích hoạt"]) 
                df = ws_to_df(ws)
                row = df[df["Tài khoản (USE\\username)"].astype(str)==acc_input]
                if row.empty:
                    st.error("Không tìm thấy tài khoản.")
                else:
                    r = row.iloc[0]
                    if str(r.get("Kích hoạt","1"))=="0":
                        st.error("Tài khoản chưa kích hoạt")
                    elif verify_pw(pw_input, r.get("Mật khẩu_băm","")):
                        st.session_state["is_auth"] = True
                        st.session_state["auth_acc"] = acc_input
                        st.session_state["auth_use"] = str(r.get("USE",""))
                        st.success("Đăng nhập thành công")
                    else:
                        st.error("Sai mật khẩu")
            except Exception as e:
                st.error(f"Lỗi đăng nhập: {e}")

    if st.button("Đăng xuất", use_container_width=True):
        st.session_state.clear(); st.experimental_rerun()

    with st.expander("🧩 Đồng bộ Users từ sheet 'USE' (1 lần đầu)"):
        st.caption("Đọc sheet 'USE' (cột: Tên đơn vị, USE (mã đăng nhập), Mật khẩu mặc định) → tạo/bổ sung sheet 'Users'.")
        if st.button("Đồng bộ ngay"):
            if not spreadsheet_id:
                st.error("Chưa nhập Spreadsheet ID")
            else:
                try:
                    ws_src = open_ws(spreadsheet_id, "USE")
                    df_src = ws_to_df(ws_src)
                    need_cols = {"Tên đơn vị","USE (mã đăng nhập)","Mật khẩu mặc định"}
                    if not need_cols.issubset(set(df_src.columns)):
                        st.error("Sheet USE thiếu cột bắt buộc")
                    else:
                        ws_users = open_ws(spreadsheet_id, "Users")
                        ensure_headers(ws_users, ["USE","Tài khoản (USE\\username)","Họ tên","Email","Mật khẩu_băm","Vai trò","Kích hoạt"]) 
                        df_users = ws_to_df(ws_users)
                        if df_users.empty:
                            df_users = pd.DataFrame(columns=["USE","Tài khoản (USE\\username)","Họ tên","Email","Mật khẩu_băm","Vai trò","Kích hoạt"]) 

                        add_rows = []
                        for _, r in df_src.iterrows():
                            unit = str(r.get("Tên đơn vị",""))
                            acc  = str(r.get("USE (mã đăng nhập)","")).strip()
                            pw0  = str(r.get("Mật khẩu mặc định","123456")) or "123456"
                            if not acc: 
                                continue
                            role = "admin" if unit.strip().lower()=="admin" or "\\ADMIN" in acc.upper() else "user"
                            if (df_users["Tài khoản (USE\\username)"].astype(str)==acc).any():
                                # cập nhật nếu đã tồn tại
                                df_users.loc[df_users["Tài khoản (USE\\username)"].astype(str)==acc, ["USE","Mật khẩu_băm","Vai trò","Kích hoạt"]] = [acc.split("\\")[0], hash_pw(pw0), role, "1"]
                            else:
                                add_rows.append({
                                    "USE": acc.split("\\")[0],
                                    "Tài khoản (USE\\username)": acc,
                                    "Họ tên": "",
                                    "Email": "",
                                    "Mật khẩu_băm": hash_pw(pw0),
                                    "Vai trò": role,
                                    "Kích hoạt": "1",
                                })
                        if add_rows:
                            df_users = pd.concat([df_users, pd.DataFrame(add_rows)], ignore_index=True)
                        df_to_ws(ws_users, df_users)
                        st.success(f"Đồng bộ xong: tổng {len(df_users)} tài khoản")
                except Exception as e:
                    st.error(f"Lỗi đồng bộ: {e}")

# =========================
# 4) Dừng nếu chưa đăng nhập
# =========================
if not st.session_state.get("is_auth"):
    st.info("Hãy đăng nhập hoặc chạy đồng bộ Users trước.")
    st.stop()

use_login = st.session_state.get("auth_use","")
st.success(f"Xin chào: {st.session_state.get('auth_acc','')} — USE: {use_login}")

# =========================
# 5) Đổi/Quên mật khẩu (trên main)
# =========================
st.markdown('<div class="section-title">Đổi mật khẩu</div>', unsafe_allow_html=True)
col1,col2,col3 = st.columns(3)
with col1: old_pw = st.text_input("Mật khẩu hiện tại", type="password")
with col2: new_pw = st.text_input("Mật khẩu mới", type="password")
with col3: new_pw2= st.text_input("Xác nhận", type="password")
if st.button("Đổi mật khẩu"):
    try:
        ws = open_ws(spreadsheet_id, "Users")
        df = ws_to_df(ws)
        acc = st.session_state.get("auth_acc")
        row = df[df["Tài khoản (USE\\username)"].astype(str)==acc]
        if row.empty:
            st.error("Không tìm thấy tài khoản")
        else:
            r = row.iloc[0]
            if not verify_pw(old_pw, r.get("Mật khẩu_băm","")):
                st.error("Mật khẩu hiện tại không đúng")
            elif new_pw != new_pw2:
                st.error("Mật khẩu mới không khớp")
            else:
                df.loc[df["Tài khoản (USE\\username)"].astype(str)==acc, "Mật khẩu_băm"] = hash_pw(new_pw)
                df_to_ws(ws, df)
                st.success("Đổi mật khẩu thành công")
    except Exception as e:
        st.error(f"Lỗi đổi mật khẩu: {e}")

st.markdown('<div class="section-title">Quên mật khẩu</div>', unsafe_allow_html=True)
acc_forgot = st.text_input("Nhập tài khoản (USE\\username) để cấp tạm")
if st.button("Cấp mật khẩu tạm"):
    try:
        ws = open_ws(spreadsheet_id, "Users")
        df = ws_to_df(ws)
        if (df["Tài khoản (USE\\username)"].astype(str)==acc_forgot).any():
            tmp = uuid.uuid4().hex[:8]
            df.loc[df["Tài khoản (USE\\username)"].astype(str)==acc_forgot, ["Mật khẩu_băm","Kích hoạt"]] = [hash_pw(tmp), "1"]
            df_to_ws(ws, df)
            # log
            wslog = open_ws(spreadsheet_id, "ResetRequests")
            ensure_headers(wslog, ["USE","Tài khoản","Thời điểm","Trạng thái","Ghi chú"]) 
            log = ws_to_df(wslog)
            use_of_acc = df.loc[df["Tài khoản (USE\\username)"].astype(str)==acc_forgot, "USE"].iloc[0]
            log = pd.concat([log, pd.DataFrame([{ "USE": use_of_acc, "Tài khoản": acc_forgot, "Thời điểm": datetime.now().isoformat(timespec='seconds'), "Trạng thái": "Cấp mật khẩu tạm", "Ghi chú": "User yêu cầu" }])], ignore_index=True)
            df_to_ws(wslog, log)
            st.success(f"Mật khẩu tạm: **{tmp}** (đã ghi sổ)")
        else:
            st.error("Không tồn tại tài khoản")
    except Exception as e:
        st.error(f"Lỗi cấp mật khẩu tạm: {e}")

# =========================
# 6) Nhập liệu KPI & Ghi KPI_DB
# =========================
st.markdown('<div class="section-title">Nhập dữ liệu KPI (CSV/Excel)</div>', unsafe_allow_html=True)
up = st.file_uploader("Chọn tệp .csv hoặc .xlsx", type=["csv","xlsx"]) 
if up is not None:
    try:
        df_in = pd.read_csv(up) if up.name.lower().endswith(".csv") else pd.read_excel(up)
        needed = {"Bộ phận/người phụ trách","Tên chỉ tiêu (KPI)","Đơn vị tính","Kế hoạch","Thực hiện","Trọng số"}
        miss = [c for c in needed if c not in df_in.columns]
        if miss:
            st.error(f"Thiếu cột: {miss}")
        else:
            st.session_state["kpi_df"] = df_in.copy()
            st.dataframe(df_in, use_container_width=True)
            st.success("Đã nạp dữ liệu")
    except Exception as e:
        st.error(f"Lỗi đọc tệp: {e}")

thang = st.number_input("Tháng",1,12,datetime.now().month)
nam   = st.number_input("Năm",2000,2100,datetime.now().year)

def compute_point(row: pd.Series) -> float:
    try:
        ten = str(row.get("Tên chỉ tiêu (KPI)","")).lower()
        ke_hoach = float(row.get("Kế hoạch",0) or 0)
        thuc_hien= float(row.get("Thực hiện",0) or 0)
        ts = float(row.get("Trọng số",0) or 0)
        if ke_hoach!=0:
            ti_le = max(0.0, min(100.0, thuc_hien/ke_hoach*100))
        else:
            ti_le = 0.0
        return ti_le * (ts/100.0)
    except Exception:
        return 0.0

if st.button("💾 Ghi KPI_DB"):
    if not spreadsheet_id:
        st.error("Thiếu Spreadsheet ID")
    elif "kpi_df" not in st.session_state:
        st.error("Chưa nạp dữ liệu")
    else:
        try:
            ws = open_ws(spreadsheet_id, "KPI_DB")
            cols = ["USE","Đơn vị","Bộ phận/người phụ trách","Tên chỉ tiêu (KPI)","Đơn vị tính","Kế hoạch","Thực hiện","Trọng số","Điểm KPI","Tháng","Năm","CreatedAt","UpdatedAt"]
            ensure_headers(ws, cols)
            cur = ws_to_df(ws)

            src = st.session_state["kpi_df"].copy()
            src["Điểm KPI"] = src.apply(compute_point, axis=1)
            src["USE"] = use_login
            src["Đơn vị"] = ""
            src["Tháng"], src["Năm"] = int(thang), int(nam)
            now = datetime.now().isoformat(timespec='seconds')
            src["CreatedAt"], src["UpdatedAt"] = now, now
            src = src[["USE","Đơn vị","Bộ phận/người phụ trách","Tên chỉ tiêu (KPI)","Đơn vị tính","Kế hoạch","Thực hiện","Trọng số","Điểm KPI","Tháng","Năm","CreatedAt","UpdatedAt"]]

            out = pd.concat([cur, src], ignore_index=True)
            df_to_ws(ws, out)
            st.success(f"Đã ghi {len(src)} dòng")
        except Exception as e:
            st.error(f"Lỗi ghi KPI_DB: {e}")

# =========================
# 7) Báo cáo nhanh
# =========================
st.markdown('<div class="section-title">Báo cáo nhanh</div>', unsafe_allow_html=True)
if st.button("Xuất Excel 3 bảng (tháng/so tháng trước/so cùng kỳ)"):
    try:
        ws = open_ws(spreadsheet_id, "KPI_DB")
        df = ws_to_df(ws)
        for c in ["Kế hoạch","Thực hiện","Trọng số","Điểm KPI","Tháng","Năm"]:
            if c in df.columns: df[c] = pd.to_numeric(df[c], errors="coerce")
        cur = df[(df["USE"]==use_login) & (df["Tháng"]==int(thang)) & (df["Năm"]==int(nam))]
        prev= df[(df["USE"]==use_login) & (df["Năm"]==int(nam)) & (df["Tháng"]==int(thang)-1)] if int(thang)>1 else pd.DataFrame()
        yoy = df[(df["USE"]==use_login) & (df["Tháng"]==int(thang)) & (df["Năm"]==int(nam)-1)]
        def agg(d):
            if d is None or d.empty:
                return pd.DataFrame(columns=["Bộ phận/người phụ trách","Tổng TS","Tổng điểm","% hoàn thành"]) 
            g = d.groupby("Bộ phận/người phụ trách", dropna=False).agg(**{"Tổng TS": ("Trọng số","sum"),"Tổng điểm": ("Điểm KPI","sum")}).reset_index()
            g["% hoàn thành"] = g.apply(lambda r: (r["Tổng điểm"]/r["Tổng TS"]*100) if r["Tổng TS"] else 0, axis=1); return g
        cur_g, prev_g, yoy_g = agg(cur), agg(prev), agg(yoy)
        out = BytesIO()
        with pd.ExcelWriter(out, engine="xlsxwriter") as w:
            cur_g.to_excel(w, index=False, sheet_name="Thang_hien_tai")
            if not prev_g.empty: prev_g.to_excel(w, index=False, sheet_name="So_thang_truoc")
            if not yoy_g.empty:  yoy_g.to_excel(w, index=False, sheet_name="So_cung_ky")
        st.download_button("Tải báo cáo", out.getvalue(), file_name=f"Bao_cao_USE_{use_login}_{int(nam)}_{int(thang):02d}.xlsx", mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")
    except Exception as e:
        st.error(f"Lỗi xuất báo cáo: {e}")

# =========================
# End
# =========================
