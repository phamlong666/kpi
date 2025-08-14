# -*- coding: utf-8 -*-
# =============================================================
#  app.py – KPI One-File (Streamlit)
#  - Nhập liệu KPI từ CSV/Excel hoặc nhập tay
#  - Lưu theo USE/Đơn vị/Tháng/Năm lên Google Sheets (KPI_DB)
#  - Báo cáo: tháng hiện tại, so tháng trước, so cùng kỳ
#  - USE Admin: xếp hạng đơn vị + cấp mật khẩu tạm (ghi Users/ResetRequests)
#  Lưu ý: Cấu hình secrets.toml phải có khối [gdrive_service_account]
# =============================================================

import streamlit as st
import pandas as pd
from datetime import datetime
from io import BytesIO
import json
import uuid
import hashlib
from typing import List

# Google Sheets / Drive
import gspread
from oauth2client.service_account import ServiceAccountCredentials

# =========================
# 1) Page config & CSS
# =========================
st.set_page_config(page_title="KPI – EVN USE Center", layout="wide")

CUSTOM_CSS = """
<style>
:root { --mn-blue: #2457F5; }
.block-container { padding-top: 1rem; }
h1,h2,h3 { letter-spacing: .2px; }
.section-title { font-size: 22px; margin: 8px 0 12px 0; }
.stButton>button { border-radius: 12px; padding: .5rem 1rem; }
.dataframe tbody tr:hover {background: #f9fbff}
</style>
"""
st.markdown(CUSTOM_CSS, unsafe_allow_html=True)

st.title("KPI – Trung tâm điều hành số (bản một file)")

# =============================
# 2) Secrets & Google Sheets auth
# =============================
@st.cache_resource(show_spinner=False)
def get_gspread_client_if_possible():
    try:
        s = st.secrets["gdrive_service_account"]
    except Exception:
        return None, "❌ Không tìm thấy 'gdrive_service_account' trong secrets."
    try:
        sa_dict = dict(s)
        # Streamlit secrets cần chuyển \n về \n thực
        if "private_key" in sa_dict:
            sa_dict["private_key"] = sa_dict["private_key"].replace("\\n", "\n")
        scope = [
            "https://spreadsheets.google.com/feeds",
            "https://www.googleapis.com/auth/drive"
        ]
        creds = ServiceAccountCredentials.from_json_keyfile_dict(sa_dict, scope)
        client = gspread.authorize(creds)
        return client, None
    except Exception as e:
        return None, f"❌ Lỗi khi khởi tạo Google Client: {e}"

client, client_err = get_gspread_client_if_possible()
connected = client is not None
if not connected:
    st.info(client_err or "Chưa cấu hình Google Service Account.")

# Spreadsheet ID & email quản trị để hiển thị (không gửi mail)
with st.sidebar:
    st.subheader("🔗 Kết nối dữ liệu")
    spreadsheet_id = st.text_input("Spreadsheet ID (Google Sheets)", value=st.session_state.get("spreadsheet_id", ""))
    st.session_state["spreadsheet_id"] = spreadsheet_id
    email_nhan_bao_cao = st.text_input("Email quản trị / nhận báo cáo", value=st.session_state.get("email_admin", "phamlong666@gmail.com"))
    st.session_state["email_admin"] = email_nhan_bao_cao

# =============================
# 3) Tiện ích Sheets: mở/đảm bảo header/DF <-> sheet
# =============================

def _open_sheet(client, spreadsheet_id: str, sheet_name: str):
    sh = client.open_by_key(spreadsheet_id)
    try:
        ws = sh.worksheet(sheet_name)
    except Exception:
        ws = sh.add_worksheet(title=sheet_name, rows=1000, cols=50)
    return ws


def _ensure_headers(ws, headers: List[str]):
    try:
        cur = ws.row_values(1)
    except Exception:
        cur = []
    if cur != headers:
        ws.clear()
        ws.append_row(headers, value_input_option="RAW")


def _ws_to_df(ws) -> pd.DataFrame:
    rows = ws.get_all_values()
    if not rows:
        return pd.DataFrame()
    return pd.DataFrame(rows[1:], columns=rows[0])


def _df_to_ws(ws, df: pd.DataFrame):
    ws.clear()
    ws.append_row(list(df.columns), value_input_option="RAW")
    if not df.empty:
        ws.append_rows(df.astype(str).values.tolist(), value_input_option="RAW")


# =============================
# 4) Sidebar – USE / Đơn vị / Kỳ làm việc
# =============================
use_id = ""
don_vi = ""
month_work = datetime.now().month
year_work = datetime.now().year

with st.sidebar:
    st.subheader("🏷 USE & Kỳ làm việc")
    if connected and spreadsheet_id:
        try:
            ws_meta = _open_sheet(client, spreadsheet_id, "Meta_Units")
            meta_cols = ["USE", "Đơn vị", "Email quản trị"]
            _ensure_headers(ws_meta, meta_cols)
            df_meta = _ws_to_df(ws_meta)
            use_list = sorted(df_meta["USE"].dropna().unique().tolist()) if not df_meta.empty else []
        except Exception as e:
            df_meta = pd.DataFrame()
            use_list = []
            st.warning(f"Không tải được Meta_Units: {e}")
    else:
        df_meta = pd.DataFrame(); use_list = []

    if use_list:
        use_id = st.selectbox("USE", use_list, index=0)
        don_vi_list = sorted(df_meta[df_meta["USE"] == use_id]["Đơn vị"].dropna().unique().tolist()) if not df_meta.empty else []
        don_vi = st.selectbox("Đơn vị", don_vi_list, index=0 if don_vi_list else None) if don_vi_list else st.text_input("Đơn vị", value="Đơn vị A")
    else:
        use_id = st.text_input("USE (vd: DH01)")
        don_vi = st.text_input("Đơn vị", value="Đơn vị A")

    month_work = st.number_input("Tháng", 1, 12, value=month_work, step=1)
    year_work = st.number_input("Năm", 2000, 2100, value=year_work, step=1)

# =============================
# 5) Khởi tạo state
# =============================
if "temp_kpi_df" not in st.session_state:
    st.session_state.temp_kpi_df = pd.DataFrame(columns=[
        "Chọn", "Bộ phận/người phụ trách", "Tên chỉ tiêu (KPI)", "Đơn vị tính",
        "Kế hoạch", "Thực hiện", "Trọng số", "Điểm KPI", "Tháng", "Năm"
    ])

# =============================
# 6) Nhập liệu – CSV/Excel và nhập tay
# =============================
st.markdown('<h2 class="section-title">1) Nhập dữ liệu KPI</h2>', unsafe_allow_html=True)

c1, c2 = st.columns([2, 1])
with c1:
    up = st.file_uploader("Tải tệp CSV/Excel (một tháng)", type=["csv", "xlsx"])  # CSV hoặc Excel 1 sheet
    if up is not None:
        try:
            if up.name.lower().endswith(".csv"):
                df_in = pd.read_csv(up)
            else:
                df_in = pd.read_excel(up)
            # Bắt buộc cột tối thiểu
            required = {
                "Bộ phận/người phụ trách", "Tên chỉ tiêu (KPI)", "Đơn vị tính",
                "Kế hoạch", "Thực hiện", "Trọng số"
            }
            missing = [c for c in required if c not in df_in.columns]
            if missing:
                st.error(f"Thiếu cột bắt buộc: {missing}")
            else:
                df_in["Tháng"] = int(month_work)
                df_in["Năm"] = int(year_work)
                # Tính điểm KPI nếu có cột % sai số (ví dụ 2 chỉ tiêu dự báo)
                df_in["Điểm KPI"] = df_in.apply(lambda r: compute_point_safe(r), axis=1)
                st.session_state.temp_kpi_df = tidy_columns(df_in)
                st.success(f"Đã nạp {len(df_in)} dòng từ tệp {up.name}")
        except Exception as e:
            st.error(f"Lỗi đọc tệp: {e}")

with c2:
    st.info("Hoặc nhập nhanh một dòng:")
    with st.form("quick_add"):
        bp = st.text_input("Bộ phận/người phụ trách", value="")
        ten = st.text_input("Tên KPI", value="")
        dv = st.text_input("Đơn vị tính", value="%")
        kehoach = st.number_input("Kế hoạch", value=0.0, format="%f")
        thuchien = st.number_input("Thực hiện", value=0.0, format="%f")
        ts = st.number_input("Trọng số", value=0.0, format="%f")
        ok = st.form_submit_button("➕ Thêm vào bảng tạm")
    if ok:
        row = {
            "Chọn": True,
            "Bộ phận/người phụ trách": bp,
            "Tên chỉ tiêu (KPI)": ten,
            "Đơn vị tính": dv,
            "Kế hoạch": kehoach,
            "Thực hiện": thuchien,
            "Trọng số": ts,
            "Điểm KPI": compute_point_quick(ten, kehoach, thuchien, ts),
            "Tháng": int(month_work),
            "Năm": int(year_work)
        }
        st.session_state.temp_kpi_df = pd.concat([st.session_state.temp_kpi_df, pd.DataFrame([row])], ignore_index=True)
        st.success("Đã thêm 1 dòng")

# =============================
# 7) Bảng tạm – chỉnh sửa trực tiếp
# =============================
st.markdown('<h2 class="section-title">2) Bảng tạm (chỉnh sửa và tính điểm)</h2>', unsafe_allow_html=True)

if st.session_state.temp_kpi_df.empty:
    st.warning("Bảng tạm đang rỗng. Hãy nạp tệp hoặc nhập tay ở trên.")
else:
    edited = st.data_editor(
        st.session_state.temp_kpi_df,
        num_rows="dynamic",
        use_container_width=True,
        key="editor_temp",
        column_config={"Chọn": st.column_config.CheckboxColumn(default=True)}
    )
    st.session_state.temp_kpi_df = edited

    cA, cB, cC, cD = st.columns([1,1,1,1])
    with cA:
        if st.button("🧹 Xóa dòng đã chọn"):
            df = st.session_state.temp_kpi_df
            if "Chọn" in df.columns:
                df = df[df["Chọn"] != True]
            st.session_state.temp_kpi_df = df
    with cB:
        if st.button("🧮 Tính lại điểm KPI"):
            df = st.session_state.temp_kpi_df.copy()
            df["Điểm KPI"] = df.apply(lambda r: compute_point_safe(r), axis=1)
            st.session_state.temp_kpi_df = df
    with cC:
        if st.button("↩️ Làm mới tháng/năm"):
            df = st.session_state.temp_kpi_df.copy()
            df["Tháng"], df["Năm"] = int(month_work), int(year_work)
            st.session_state.temp_kpi_df = df
    with cD:
        if st.button("⬇️ Xuất Excel (Bảng tạm)"):
            out = BytesIO()
            with pd.ExcelWriter(out, engine="xlsxwriter") as writer:
                st.session_state.temp_kpi_df.to_excel(writer, index=False, sheet_name="Bang_tam")
            st.download_button("Tải bảng tạm", out.getvalue(), file_name="Bang_tam_KPI.xlsx", mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")

# =============================
# 8) Ghi KPI_DB theo USE/Tháng/Năm
# =============================
st.markdown('<h2 class="section-title">3) Ghi dữ liệu lên KPI_DB</h2>', unsafe_allow_html=True)

if st.button("💾 Ghi 'Bảng tạm' vào KPI_DB", type="primary"):
    if not connected or not spreadsheet_id:
        st.error("Chưa kết nối Google Sheets hoặc thiếu Spreadsheet ID.")
    elif st.session_state.temp_kpi_df.empty:
        st.warning("Bảng tạm đang rỗng.")
    elif not use_id or not don_vi:
        st.warning("Thiếu USE/Đơn vị.")
    else:
        try:
            ws_db = _open_sheet(client, spreadsheet_id, "KPI_DB")
            cols_db = [
                "USE","Đơn vị","Bộ phận/người phụ trách","Tên chỉ tiêu (KPI)","Đơn vị tính",
                "Kế hoạch","Thực hiện","Trọng số","Điểm KPI","Tháng","Năm",
                "Nhóm/Parent","Phương pháp đo kết quả","CreatedAt","UpdatedAt"
            ]
            _ensure_headers(ws_db, cols_db)
            df_db = _ws_to_df(ws_db)

            src = st.session_state.temp_kpi_df.drop(columns=["Chọn"], errors="ignore").copy()
            src["USE"] = use_id
            src["Đơn vị"] = don_vi
            now_iso = datetime.now().isoformat(timespec="seconds")
            src["CreatedAt"] = now_iso
            src["UpdatedAt"] = now_iso
            src["Nhóm/Parent"] = ""
            src["Phương pháp đo kết quả"] = ""
            src = src[[
                "USE","Đơn vị","Bộ phận/người phụ trách","Tên chỉ tiêu (KPI)","Đơn vị tính",
                "Kế hoạch","Thực hiện","Trọng số","Điểm KPI","Tháng","Năm",
                "Nhóm/Parent","Phương pháp đo kết quả","CreatedAt","UpdatedAt"
            ]]

            key_cols = ["USE","Tháng","Năm","Bộ phận/người phụ trách","Tên chỉ tiêu (KPI)"]

            if df_db.empty:
                df_new = src
            else:
                for c in key_cols:
                    if c in df_db.columns: df_db[c] = df_db[c].astype(str)
                    src[c] = src[c].astype(str)
                cur_keys = src[key_cols].apply(lambda r: "||".join(r.values), axis=1).unique().tolist()
                df_db = df_db[~df_db[key_cols].apply(lambda r: "||".join(r.values), axis=1).isin(cur_keys)]
                df_new = pd.concat([df_db, src], ignore_index=True)

            _df_to_ws(ws_db, df_new)
            st.success(f"Đã ghi {len(src)} dòng vào KPI_DB cho USE {use_id} ({int(month_work)}/{int(year_work)}).")
        except Exception as e:
            st.error(f"Lỗi khi ghi KPI_DB: {e}")

# =============================
# 9) Báo cáo: tháng / so tháng trước / so cùng kỳ
# =============================
st.markdown('<h2 class="section-title">4) Báo cáo KPI</h2>', unsafe_allow_html=True)

if not connected or not spreadsheet_id:
    st.info("Kết nối Google Sheets để xem báo cáo (đọc KPI_DB).")
else:
    try:
        ws_db = _open_sheet(client, spreadsheet_id, "KPI_DB")
        df_db = _ws_to_df(ws_db)
        for c in ["Kế hoạch","Thực hiện","Trọng số","Điểm KPI","Tháng","Năm"]:
            if c in df_db.columns:
                df_db[c] = pd.to_numeric(df_db[c], errors="coerce")

        cur = df_db[(df_db["USE"] == str(use_id)) & (df_db["Tháng"] == int(month_work)) & (df_db["Năm"] == int(year_work))].copy()
        prev = df_db[(df_db["USE"] == str(use_id)) & (df_db["Năm"] == int(year_work)) & (df_db["Tháng"] == int(month_work) - 1)].copy() if int(month_work) > 1 else pd.DataFrame()
        yoy  = df_db[(df_db["USE"] == str(use_id)) & (df_db["Tháng"] == int(month_work)) & (df_db["Năm"] == int(year_work) - 1)].copy()

        def _agg(df):
            if df is None or df.empty:
                return pd.DataFrame(columns=["Bộ phận/người phụ trách","Tổng trọng số","Tổng điểm","% hoàn thành"])
            g = df.groupby("Bộ phận/người phụ trách", dropna=False).agg(**{
                "Tổng trọng số": ("Trọng số", "sum"),
                "Tổng điểm": ("Điểm KPI", "sum"),
            }).reset_index()
            g["% hoàn thành"] = g.apply(lambda r: (r["Tổng điểm"]/r["Tổng trọng số"]*100) if r["Tổng trọng số"] else 0, axis=1)
            return g

        cur_g = _agg(cur); prev_g = _agg(prev); yoy_g = _agg(yoy)

        st.subheader(f"📊 USE {use_id} – Tháng {int(month_work)}/{int(year_work)}")
        st.dataframe(cur_g, use_container_width=True, hide_index=True)

        st.subheader("↔️ So với tháng trước")
        if prev_g.empty:
            st.info("Không có dữ liệu tháng trước.")
        else:
            comp_prev = cur_g.merge(prev_g, on="Bộ phận/người phụ trách", how="outer", suffixes=("_hiện tại", "_tháng trước")).fillna(0)
            comp_prev["Δ điểm"] = comp_prev["Tổng điểm_hiện tại"] - comp_prev["Tổng điểm_tháng trước"]
            comp_prev["Δ %"] = comp_prev["% hoàn thành_hiện tại"] - comp_prev["% hoàn thành_tháng trước"]
            st.dataframe(comp_prev, use_container_width=True, hide_index=True)

        st.subheader("📈 So với cùng kỳ năm trước")
        if yoy_g.empty:
            st.info("Không có dữ liệu cùng kỳ.")
        else:
            comp_yoy = cur_g.merge(yoy_g, on="Bộ phận/người phụ trách", how="outer", suffixes=("_hiện tại", "_cùng kỳ")).fillna(0)
            comp_yoy["Δ điểm"] = comp_yoy["Tổng điểm_hiện tại"] - comp_yoy["Tổng điểm_cùng kỳ"]
            comp_yoy["Δ %"] = comp_yoy["% hoàn thành_hiện tại"] - comp_yoy["% hoàn thành_cùng kỳ"]
            st.dataframe(comp_yoy, use_container_width=True, hide_index=True)

        if st.button("⬇️ Xuất báo cáo Excel (3 bảng)"):
            out = BytesIO()
            with pd.ExcelWriter(out, engine="xlsxwriter") as writer:
                cur_g.to_excel(writer, index=False, sheet_name="Thang_hien_tai")
                if not prev_g.empty: comp_prev.to_excel(writer, index=False, sheet_name="So_thang_truoc")
                if not yoy_g.empty: comp_yoy.to_excel(writer, index=False, sheet_name="So_cung_ky")
            st.download_button(
                "Tải báo cáo",
                out.getvalue(),
                file_name=f"Bao_cao_KPI_USE_{use_id}_{int(year_work)}_{int(month_work):02d}.xlsx",
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
            )

    except Exception as e:
        st.error(f"Lỗi khi đọc KPI_DB: {e}")

# =============================
# 10) USE Admin – Xếp hạng đơn vị & Quên mật khẩu
# =============================
st.markdown('<h2 class="section-title">5) USE Admin</h2>', unsafe_allow_html=True)

if connected and spreadsheet_id:
    # Xác định quyền: email trong Meta_Units hoặc default admin
    is_admin = False
    try:
        ws_meta = _open_sheet(client, spreadsheet_id, "Meta_Units")
        df_meta = _ws_to_df(ws_meta)
        admin_emails = df_meta[df_meta["USE"] == str(use_id)]["Email quản trị"].dropna().tolist() if "Email quản trị" in df_meta.columns else []
        is_admin = (email_nhan_bao_cao in admin_emails) or (email_nhan_bao_cao.lower() == "phamlong666@gmail.com")
    except Exception:
        pass

    if not is_admin:
        st.info("Bạn không có quyền USE Admin.")
    else:
        st.success(f"USE Admin – {use_id}")

        # Xếp hạng giữa các đơn vị
        try:
            ws_db = _open_sheet(client, spreadsheet_id, "KPI_DB")
            df_db = _ws_to_df(ws_db)
            for c in ["Kế hoạch","Thực hiện","Trọng số","Điểm KPI","Tháng","Năm"]:
                if c in df_db.columns:
                    df_db[c] = pd.to_numeric(df_db[c], errors="coerce")
            filt = df_db[(df_db["USE"] == str(use_id)) & (df_db["Tháng"] == int(month_work)) & (df_db["Năm"] == int(year_work))].copy()
            if filt.empty:
                st.info("Chưa có dữ liệu kỳ này.")
            else:
                g = filt.groupby("Đơn vị", dropna=False).agg(**{
                    "Tổng trọng số": ("Trọng số", "sum"),
                    "Tổng điểm": ("Điểm KPI", "sum")
                }).reset_index()
                g["% hoàn thành"] = g.apply(lambda r: (r["Tổng điểm"]/r["Tổng trọng số"]*100) if r["Tổng trọng số"] else 0, axis=1)
                g = g.sort_values("% hoàn thành", ascending=False)
                st.subheader("🏁 Xếp hạng đơn vị")
                st.dataframe(g, use_container_width=True, hide_index=True)
        except Exception as e:
            st.error(f"Lỗi so sánh đơn vị: {e}")

        # Quên mật khẩu – ghi nhận & cấp tạm
        st.subheader("🔐 Quên mật khẩu")
        tk = st.text_input("Tài khoản (định dạng USE\\username)")
        if st.button("Cấp mật khẩu tạm"):
            if not tk:
                st.warning("Nhập tài khoản.")
            else:
                try:
                    tmp_pass = uuid.uuid4().hex[:8]
                    pass_hash = hashlib.sha256(tmp_pass.encode()).hexdigest()

                    # Ghi ResetRequests
                    ws_req = _open_sheet(client, spreadsheet_id, "ResetRequests")
                    _ensure_headers(ws_req, ["USE","Tài khoản","Thời điểm","Trạng thái","Ghi chú"])
                    df_req = _ws_to_df(ws_req)
                    row = pd.DataFrame([{
                        "USE": use_id,
                        "Tài khoản": tk,
                        "Thời điểm": datetime.now().isoformat(timespec="seconds"),
                        "Trạng thái": "Đã cấp mật khẩu tạm",
                        "Ghi chú": f"Gửi admin {email_nhan_bao_cao}"
                    }])
                    df_req = pd.concat([df_req, row], ignore_index=True)
                    _df_to_ws(ws_req, df_req)

                    # Cập nhật Users (upsert mật khẩu_băm)
                    ws_users = _open_sheet(client, spreadsheet_id, "Users")
                    _ensure_headers(ws_users, ["USE","Tài khoản (USE\\username)","Họ tên","Email","Mật khẩu_băm","Vai trò (admin/user)","Kích hoạt"])
                    df_users = _ws_to_df(ws_users)
                    if df_users.empty:
                        df_users = pd.DataFrame(columns=["USE","Tài khoản (USE\\username)","Họ tên","Email","Mật khẩu_băm","Vai trò (admin/user)","Kích hoạt"])

                    if (not df_users.empty) and (df_users["Tài khoản (USE\\username)"].astype(str) == tk).any():
                        df_users.loc[df_users["Tài khoản (USE\\username)"].astype(str) == tk, ["USE","Mật khẩu_băm","Kích hoạt"]] = [use_id, pass_hash, "1"]
                    else:
                        df_users = pd.concat([df_users, pd.DataFrame([{
                            "USE": use_id,
                            "Tài khoản (USE\\username)": tk,
                            "Họ tên": "",
                            "Email": "",
                            "Mật khẩu_băm": pass_hash,
                            "Vai trò (admin/user)": "user",
                            "Kích hoạt": "1"
                        }])], ignore_index=True)

                    _df_to_ws(ws_users, df_users)
                    st.success(f"Đã cấp mật khẩu tạm: **{tmp_pass}** (đã ghi sổ). Admin {email_nhan_bao_cao} sẽ kiểm tra và cung cấp lại cho người dùng.")
                except Exception as e:
                    st.error(f"Lỗi cấp mật khẩu tạm: {e}")

# =============================
# 11) Hàm tính điểm KPI – xử lý an toàn
# =============================

def compute_point_quick(ten_kpi: str, ke_hoach: float, thuc_hien: float, trong_so: float) -> float:
    """Tính điểm đơn giản khi nhập tay. Có nhánh riêng cho 2 KPI dự báo của EVN.
    - Hai KPI "Dự báo tổng thương phẩm…": điểm tối đa 3đ; mỗi 0.1% vượt sai số trừ 0.04đ.
    - Mặc định: điểm = min(100, thực hiện/ kế hoạch * 100) * (trọng số/100)
    """
    try:
        name = (ten_kpi or "").lower()
        if "dự báo tổng thương phẩm" in name:
            # giả định cột Thực hiện là % sai số tuyệt đối
            sai_so = abs(float(thuc_hien))
            base = 3.0
            if sai_so <= 1.5:
                phat = 0.0
            else:
                phat = ((sai_so - 1.5) / 0.1) * 0.04
            return max(0.0, base - phat)
        # Mặc định
        if ke_hoach and ke_hoach != 0:
            ti_le = (float(thuc_hien) / float(ke_hoach)) * 100
        else:
            ti_le = 0
        return min(100.0, max(0.0, ti_le)) * (float(trong_so) / 100.0)
    except Exception:
        return 0.0


def compute_point_safe(r: pd.Series) -> float:
    try:
        return compute_point_quick(
            str(r.get("Tên chỉ tiêu (KPI)", "")),
            float(r.get("Kế hoạch", 0) or 0),
            float(r.get("Thực hiện", 0) or 0),
            float(r.get("Trọng số", 0) or 0),
        )
    except Exception:
        return 0.0


def tidy_columns(df: pd.DataFrame) -> pd.DataFrame:
    cols = [
        "Chọn", "Bộ phận/người phụ trách", "Tên chỉ tiêu (KPI)", "Đơn vị tính",
        "Kế hoạch", "Thực hiện", "Trọng số", "Điểm KPI", "Tháng", "Năm"
    ]
    for c in cols:
        if c not in df.columns:
            df[c] = None
    df = df[cols]
    # ép kiểu số
    for c in ["Kế hoạch","Thực hiện","Trọng số","Điểm KPI","Tháng","Năm"]:
        df[c] = pd.to_numeric(df[c], errors="coerce")
    # default tick chọn
    if "Chọn" in df.columns:
        df["Chọn"] = df["Chọn"].fillna(True)
    return df

# =============================
# End of file
# =============================
