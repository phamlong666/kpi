# -*- coding: utf-8 -*-
"""
app.py — KPI - Đội quản lý Điện lực khu vực Định Hóa
- Logo từ GitHub + tiêu đề app
- Đăng nhập / Đăng xuất / Đồng bộ Users từ sheet USE
- Đổi mật khẩu / Quên mật khẩu (ghi ResetRequests)
- Nhập KPI từ CSV/XLSX → ghi KPI_DB
- Báo cáo: tháng hiện tại / so tháng trước / so cùng kỳ
- Admin: so sánh KPI giữa các đơn vị
- Bảo mật: loader 3 chế độ (Fernet, JSON, base64)
"""

import streamlit as st
import pandas as pd
import gspread
from oauth2client.service_account import ServiceAccountCredentials
from datetime import datetime
from io import BytesIO
import hashlib, uuid, base64, json
# Thêm import cho Fernet
from cryptography.fernet import Fernet

# =========================
# Page config + Logo + Title
# =========================
st.set_page_config(page_title="KPI - Đội quản lý Điện lực khu vực Định Hóa", layout="wide")
st.image("https://raw.githubusercontent.com/phamlong666/kpi/main/logo_hinh_tron.png", width=80)
st.title("📊 KPI - Đội quản lý Điện lực khu vực Định Hóa")

# =========================
# Helpers: Password hash
# =========================
def hash_pw(p: str) -> str:
    return hashlib.sha256((p or "").encode()).hexdigest()

def verify_pw(plain: str, hashed: str) -> bool:
    return hash_pw(plain) == str(hashed)

# =========================
# Loader Google SA (multi-mode) + Client SAFE
# =========================
def _from_plain_block():
    try:
        sa = dict(st.secrets["gdrive_service_account"])
        if "private_key_b64" in st.secrets["gdrive_service_account"]:
            pk = base64.b64decode(st.secrets["gdrive_service_account"]["private_key_b64"]).decode()
            sa["private_key"] = pk
        if "private_key" in sa:
            sa["private_key"] = sa["private_key"].replace("\\n", "\n")
        return sa
    except Exception:
        return None

def load_sa_from_secret():
    """Ưu tiên Fernet (fernet_key + gsa_enc), fallback JSON / base64. Không raise, chỉ trả None nếu thiếu."""
    # Thử Fernet nếu có
    try:
        gsa_enc_b64 = st.secrets.get("gsa_enc")
        fernet_key  = st.secrets.get("fernet_key")
        if gsa_enc_b64 and fernet_key:
            blob = base64.b64decode(gsa_enc_b64.encode())
            sa_bytes = Fernet(fernet_key.encode()).decrypt(blob)
            sa = json.loads(sa_bytes.decode())
            if "private_key" in sa:
                sa["private_key"] = sa["private_key"].replace("\\n", "\n")
            return sa, "fernet"
    except Exception:
        pass
    # Fallback: block JSON/base64
    sa_legacy = _from_plain_block()
    if sa_legacy:
        return sa_legacy, "legacy"
    return None, "none"

def get_client_safe():
    sa_dict, mode = load_sa_from_secret()
    if not sa_dict:
        return None, mode, "Thiếu secrets (fernet_key+gsa_enc) hoặc [gdrive_service_account]."
    try:
        scope = ["https://spreadsheets.google.com/feeds",
                 "https://www.googleapis.com/auth/drive"]
        creds = ServiceAccountCredentials.from_json_keyfile_dict(sa_dict, scope)
        client = gspread.authorize(creds)
        return client, mode, None
    except Exception as e:
        return None, mode, f"Lỗi Google auth: {e}"

# Khởi tạo session state
if "client" not in st.session_state:
    st.session_state["client"] = None
if "connected" not in st.session_state:
    st.session_state["connected"] = False
if "client_err" not in st.session_state:
    st.session_state["client_err"] = ""

# Banner trạng thái kết nối
if not st.session_state.connected:
    st.warning(st.session_state.client_err or "Chưa cấu hình Service Account. Vẫn có thể xem giao diện, nhưng sẽ không đọc/ghi Google Sheets.")
elif st.session_state.sa_mode == "legacy":
    st.info("Đang dùng chế độ kết nối dự phòng (không dùng Fernet).")
elif st.session_state.sa_mode == "fernet":
    st.caption("Đang dùng Service Account giải mã bằng Fernet.")


# =========================
# Sheet helpers
# =========================
def open_ws(spreadsheet_id, sheet_name):
    if st.session_state.client is None:
        st.error("Client chưa được kết nối.")
        return None
    try:
        sh = st.session_state.client.open_by_key(spreadsheet_id)
        return sh.worksheet(sheet_name)
    except gspread.exceptions.WorksheetNotFound:
        st.error(f"Sheet '{sheet_name}' không tồn tại. Vui lòng tạo sheet này trong Google Sheet.")
        return None
    except Exception as e:
        st.error(f"Lỗi khi mở sheet: {e}")
        return None

def ensure_headers(ws, headers):
    try:
        cur = ws.row_values(1)
    except:
        cur = []
    if cur != headers:
        ws.clear(); ws.append_row(headers, value_input_option="RAW")

def ws_to_df(ws):
    rows = ws.get_all_values()
    if not rows: return pd.DataFrame()
    return pd.DataFrame(rows[1:], columns=rows[0])

def df_to_ws(ws, df):
    ws.clear(); ws.append_row(list(df.columns), value_input_option="RAW")
    if not df.empty:
        ws.append_rows(df.astype(str).values.tolist(), value_input_option="RAW")

# =========================
# Sidebar: kết nối + đăng nhập + sync Users
# =========================
with st.sidebar:
    st.subheader("🔗 Kết nối dữ liệu")
    spreadsheet_id = st.text_input("Spreadsheet ID", value=st.session_state.get("sheet_id", ""))
    st.session_state["sheet_id"] = spreadsheet_id
    st.caption("Dán phần giữa URL Google Sheet (/d/<ID>/edit)")

    st.divider()
    st.subheader("🔐 Đăng nhập")
    acc_input = st.text_input("Tài khoản (USE\\username)", value=st.session_state.get("auth_acc",""))
    pw_input  = st.text_input("Mật khẩu", type="password")

    if st.button("Đăng nhập", use_container_width=True):
        client, sa_mode, client_err = get_client_safe()
        if client:
            st.session_state.client = client
            st.session_state.connected = True
            st.session_state.sa_mode = sa_mode
            st.session_state.client_err = None
            
            # Mở sheet "USE" để đăng nhập
            ws = open_ws(spreadsheet_id, "USE")
            if ws:
                df = ws_to_df(ws)
                # Tìm tài khoản trong cột "USE (mã đăng nhập)"
                row = df[df["USE (mã đăng nhập)"].astype(str)==acc_input]
                if row.empty: st.error("Không tìm thấy tài khoản")
                else:
                    r=row.iloc[0]
                    # Đảm bảo cột "Mật khẩu_băm" và "Kích hoạt" tồn tại
                    if "Mật khẩu_băm" not in r or "Kích hoạt" not in r:
                        st.error("Sheet USE chưa được đồng bộ. Vui lòng đồng bộ trước khi đăng nhập.")
                    elif str(r.get("Kích hoạt","1"))=="0": st.error("Chưa kích hoạt")
                    elif verify_pw(pw_input, r.get("Mật khẩu_băm","")):
                        st.session_state.update({"is_auth":True,
                                                "auth_acc":acc_input,
                                                "auth_use":str(r.get("USE (mã đăng nhập)","")).split("\\")[0],
                                                "role":r.get("Vai trò","user")})
                        st.success("Đăng nhập thành công")
                    else: st.error("Sai mật khẩu")
        else:
            st.session_state.connected = False
            st.session_state.client_err = client_err
            st.error(client_err)


    if st.button("Đăng xuất", use_container_width=True):
        st.session_state.clear(); st.experimental_rerun()

    with st.expander("🧩 Đồng bộ Users từ sheet USE"):
        st.caption("Ứng dụng sẽ hash mật khẩu từ cột 'Mật khẩu mặc định' và lưu vào cột 'Mật khẩu_băm' trên cùng sheet.")
        if st.button("Đồng bộ ngay"):
            client, sa_mode, client_err = get_client_safe()
            if client:
                st.session_state.client = client
                st.session_state.connected = True
                st.session_state.sa_mode = sa_mode
                st.session_state.client_err = None
                
                ws_use = open_ws(spreadsheet_id,"USE")
                if ws_use:
                    df_use = ws_to_df(ws_use)
                    need = {"Tên đơn vị","USE (mã đăng nhập)","Mật khẩu mặc định"}
                    if not need.issubset(df_use.columns): st.error("Sheet USE thiếu cột bắt buộc: 'Tên đơn vị', 'USE (mã đăng nhập)', 'Mật khẩu mặc định'")
                    else:
                        # Thêm các cột cần thiết nếu chưa có
                        if "Mật khẩu_băm" not in df_use.columns: df_use["Mật khẩu_băm"]=""
                        if "Vai trò" not in df_use.columns: df_use["Vai trò"]=""
                        if "Kích hoạt" not in df_use.columns: df_use["Kích hoạt"]="1"
                        
                        # Cập nhật các cột
                        for idx, row in df_use.iterrows():
                            unit = str(row.get("Tên đơn vị", "")).lower()
                            acc = str(row.get("USE (mã đăng nhập)", "")).strip()
                            pw_default = str(row.get("Mật khẩu mặc định", "123456"))
                            
                            if acc:
                                # Tạo mật khẩu băm
                                df_use.at[idx, "Mật khẩu_băm"] = hash_pw(pw_default)
                                # Phân quyền admin/user
                                df_use.at[idx, "Vai trò"] = "admin" if "admin" in unit else "user"
                                # Kích hoạt tài khoản
                                df_use.at[idx, "Kích hoạt"] = "1"
                        
                        # Ghi lại dữ liệu đã cập nhật vào sheet "USE"
                        df_to_ws(ws_use, df_use)
                        st.success(f"Đồng bộ xong {len(df_use)} tài khoản vào sheet 'USE'")
            else:
                st.session_state.connected = False
                st.session_state.client_err = client_err
                st.error(client_err)


# =========================
# Stop nếu chưa login
# =========================
if not st.session_state.get("is_auth"):
    st.info("Hãy đăng nhập hoặc đồng bộ Users trước")
else:
    use_login=st.session_state.get("auth_use","")
    st.success(f"Xin chào {st.session_state.get('auth_acc')} — USE: {use_login}")

    # =========================
    # Đổi mật khẩu / Quên mật khẩu
    # =========================
    old_pw,new_pw,new_pw2=st.columns(3)
    with old_pw: op=st.text_input("Mật khẩu hiện tại",type="password")
    with new_pw: np=st.text_input("Mật khẩu mới",type="password")
    with new_pw2: np2=st.text_input("Xác nhận",type="password")
    if st.button("Đổi mật khẩu"):
        ws=open_ws(spreadsheet_id,"USE");
        if ws:
            df=ws_to_df(ws);acc=st.session_state["auth_acc"]
            row=df[df["USE (mã đăng nhập)"].astype(str)==acc]
            if row.empty: st.error("Không tìm thấy acc")
            elif not verify_pw(op,row.iloc[0]["Mật khẩu_băm"]): st.error("Sai mật khẩu hiện tại")
            elif np!=np2: st.error("Không khớp")
            else:
                df.loc[df["USE (mã đăng nhập)"].astype(str)==acc,"Mật khẩu_băm"]=hash_pw(np)
                df_to_ws(ws,df);st.success("Đổi thành công")

    acc_f=st.text_input("Tài khoản cần cấp mật khẩu tạm")
    if st.button("Cấp mật khẩu tạm"):
        ws=open_ws(spreadsheet_id,"USE")
        if ws:
            df=ws_to_df(ws)
            if (df["USE (mã đăng nhập)"].astype(str)==acc_f).any():
                tmp=uuid.uuid4().hex[:8]
                df.loc[df["USE (mã đăng nhập)"].astype(str)==acc_f,
                       ["Mật khẩu_băm","Kích hoạt"]]=[hash_pw(tmp),"1"]
                df_to_ws(ws,df)
                wslog=open_ws(spreadsheet_id,"ResetRequests")
                ensure_headers(wslog,["USE","Tài khoản","Thời điểm","Trạng thái","Ghi chú"])
                log=ws_to_df(wslog)
                use_acc=df.loc[df["USE (mã đăng nhập)"].astype(str)==acc_f,"USE (mã đăng nhập)"].iloc[0]
                log=pd.concat([log,pd.DataFrame([{"USE":use_acc.split("\\")[0], "Tài khoản":acc_f,
                                                  "Thời điểm":datetime.now().isoformat(timespec='seconds'),
                                                  "Trạng thái":"Cấp mật khẩu tạm","Ghi chú":"User yêu cầu"}])],
                              ignore_index=True)
                df_to_ws(wslog,log)
                st.success(f"Mật khẩu tạm: {tmp}")
            else: st.error("Không tồn tại")

    # =========================
    # Nhập KPI + Ghi KPI_DB
    # =========================
    up=st.file_uploader("Chọn CSV/XLSX")
    if up is not None:
        df_in=pd.read_csv(up) if up.name.endswith(".csv") else pd.read_excel(up)
        st.dataframe(df_in)
        st.session_state["kpi_df"]=df_in

    thang=st.number_input("Tháng",1,12,datetime.now().month)
    nam=st.number_input("Năm",2000,2100,datetime.now().year)

    def compute_point(r):
        try:
            kh=float(r.get("Kế hoạch",0));th=float(r.get("Thực hiện",0));ts=float(r.get("Trọng số",0))
            tile=th/kh*100 if kh else 0
            return max(0,min(100,tile))*(ts/100)
        except: return 0

    if st.button("💾 Ghi KPI_DB"):
        ws=open_ws(spreadsheet_id,"KPI_DB")
        if ws:
            cols=["USE","Đơn vị","Bộ phận/người phụ trách","Tên chỉ tiêu (KPI)","Đơn vị tính",
                  "Kế hoạch","Thực hiện","Trọng số","Điểm KPI","Tháng","Năm","CreatedAt","UpdatedAt"]
            ensure_headers(ws,cols);cur=ws_to_df(ws)
            df=st.session_state.get("kpi_df");df["Điểm KPI"]=df.apply(compute_point,axis=1)
            df["USE"],df["Đơn vị"],df["Tháng"],df["Năm"]=use_login,"",int(thang),int(nam)
            now=datetime.now().isoformat(timespec='seconds');df["CreatedAt"],df["UpdatedAt"]=now,now
            out=pd.concat([cur,df[cols]],ignore_index=True);df_to_ws(ws,out)
            st.success("Đã ghi")

    # =========================
    # Báo cáo
    # =========================
    if st.button("Xuất báo cáo Excel"):
        ws=open_ws(spreadsheet_id,"KPI_DB")
        if ws:
            df=ws_to_df(ws)
            for c in ["Kế hoạch","Thực hiện","Trọng số","Điểm KPI","Tháng","Năm"]:
                if c in df.columns: df[c]=pd.to_numeric(df[c],errors="coerce")
            cur=df[(df["USE"]==use_login)&(df["Tháng"]==int(thang))&(df["Năm"]==int(nam))]
            prev=df[(df["USE"]==use_login)&(df["Năm"]==int(nam))&(df["Tháng"]==int(thang)-1)] if int(thang)>1 else pd.DataFrame()
            yoy=df[(df["USE"]==use_login)&(df["Tháng"]==int(thang))&(df["Năm"]==int(nam)-1)]
            def agg(d):
                if d.empty: return pd.DataFrame(columns=["Bộ phận","Tổng TS","Tổng điểm","% hoàn thành"])
                g=d.groupby("Bộ phận/người phụ trách").agg(**{"Tổng TS":("Trọng số","sum"),
                                                              "Tổng điểm":("Điểm KPI","sum")}).reset_index()
                g["% hoàn thành"]=g.apply(lambda r:(r["Tổng điểm"]/r["Tổng TS"]*100) if r["Tổng TS"] else 0,axis=1);return g
            cur_g,prev_g,yoy_g=agg(cur),agg(prev),agg(yoy)
            out=BytesIO()
            with pd.ExcelWriter(out,engine="xlsxwriter") as w:
                cur_g.to_excel(w,index=False,sheet_name="Thang_hien_tai")
                if not prev_g.empty: prev_g.to_excel(w,index=False,sheet_name="So_thang_truoc")
                if not yoy_g.empty: yoy_g.to_excel(w,index=False,sheet_name="So_cung_ky")
            st.download_button("Tải báo cáo",out.getvalue(),
                               file_name=f"Bao_cao_{use_login}_{nam}_{thang:02d}.xlsx",
                               mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")

    # =========================
    # Admin compare
    # =========================
    if st.session_state.get("role")=="admin":
        st.subheader("📈 So sánh KPI các đơn vị")
        ws=open_ws(spreadsheet_id,"KPI_DB")
        if ws:
            df=ws_to_df(ws)
            if not df.empty:
                df["Điểm KPI"]=pd.to_numeric(df["Điểm KPI"],errors="coerce")
                ranking=df.groupby(["USE","Tháng","Năm"]).agg({"Điểm KPI":"sum"}).reset_index()
                st.dataframe(ranking.sort_values(["Năm","Tháng","Điểm KPI"],ascending=[False,False,False]))
