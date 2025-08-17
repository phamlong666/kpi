# -*- coding: utf-8 -*-
# KPI – Đội quản lý Điện lực khu vực Định Hóa
# Bản cập nhật:
# - Tự nạp form khi tick "Chọn" (có rerun tức thì để hiển thị ngay)
# - "📥 Nhập CSV vào KPI" đặt dưới vùng nhập liệu (form luôn ở trên)
# - Ô "Phương pháp đo kết quả" có combo chọn RULES → tự điền [CODE]
# - Login bắt buộc, form sticky, 4 nút 4 màu, ghi Google Sheet, xuất Excel

import re, io, json, math
from datetime import datetime
from pathlib import Path
import pandas as pd
import streamlit as st

# ================== PAGE & STYLE ==================
st.set_page_config(page_title="KPI – Định Hóa", page_icon="📊", layout="wide")
st.markdown("""
<style>
.app-title{margin:0;font-size:22px;font-weight:800;
  background:linear-gradient(90deg,#0ea5e9,#22c55e 50%,#a855f7);
  -webkit-background-clip:text;-webkit-text-fill-color:transparent;}
.app-sub{color:#64748b;font-size:12px;margin:2px 0 10px}
.kpi-stick{position:sticky;top:0;z-index:50;background:var(--background-color);
  border:1px solid #e5e7eb;border-radius:12px;padding:12px;box-shadow:0 2px 8px rgba(0,0,0,.05);}
.btn-apply  button{background:#0ea5e9 !important;color:#fff !important;border:0 !important}
.btn-save   button{background:#22c55e !important;color:#fff !important;border:0 !important}
.btn-refresh button{background:#f59e0b !important;color:#111 !important;border:0 !important}
.btn-export button{background:#3b82f6 !important;color:#fff !important;border:0 !important}
.total-box{background:#f0f9ff;border:1px dashed #7dd3fc;color:#0c4a6e;font-weight:700;
  padding:8px 12px;border-radius:10px;margin:.5rem 0}
section.main > div.block-container{padding-top:.7rem}
</style>
""", unsafe_allow_html=True)

# ================== HEADER ==================
LOGO_URL = "https://raw.githubusercontent.com/phamlong666/kpi/main/logo_hinh_tron.png"
c1,c2 = st.columns([1,10], vertical_alignment="center")
with c1: st.image(LOGO_URL, width=56)
with c2:
    st.markdown('<h1 class="app-title">KPI – Đội quản lý Điện lực khu vực Định Hóa</h1>', unsafe_allow_html=True)
    st.markdown('<div class="app-sub">Form nhập tay GHIM CỨNG + chấm điểm theo RULES</div>', unsafe_allow_html=True)

# ================== NUMBER HELPERS ==================
def _to_float(x):
    try:
        if x is None: return None
        if isinstance(x,(int,float)): return float(x)
        s=str(x).strip()
        if s=="" or s.lower() in ("none","nan"): return None
        s=s.replace(" ","").replace(".","").replace(",",".")
        return float(s)
    except: return None

def _fmt_vn(v, nd=2):
    if v is None: return ""
    try: v=round(float(v), nd)
    except: return str(v)
    return f"{v:,.{nd}f}".replace(",","_").replace(".",",").replace("_",".")

def _w_ratio(w):
    v=_to_float(w)
    if v is None: return 0.0
    return v/100.0 if v>1 else v

# ================== GOOGLE SHEETS (optional) ==================
try:
    import gspread
    from google.oauth2.service_account import Credentials
    HAS_GS=True
except Exception:
    HAS_GS=False

SCOPES=[
 "https://www.googleapis.com/auth/spreadsheets",
 "https://www.googleapis.com/auth/drive",
 "https://www.googleapis.com/auth/drive.file",
]

def _extract_sheet_id(url_or_id:str)->str:
    return url_or_id.split("/d/")[1].split("/")[0] if "/d/" in url_or_id else url_or_id

def _open_spreadsheet():
    if not HAS_GS: raise RuntimeError("Thiếu gspread/google-auth.")
    sid=st.session_state.get("sheet_url_or_id","").strip()
    if not sid: raise RuntimeError("Chưa nhập ID/URL Google Sheet.")
    if "gcp_service_account" in st.secrets:
        info=dict(st.secrets["gcp_service_account"])
        creds=Credentials.from_service_account_info(info, scopes=SCOPES)
    elif Path("credentials.json").exists():
        creds=Credentials.from_service_account_file("credentials.json", scopes=SCOPES)
    else:
        raise RuntimeError("Thiếu Service Account.")
    gc=gspread.authorize(creds)
    return gc.open_by_key(_extract_sheet_id(sid))

# ================== RULES LOADER ==================
@st.cache_data(ttl=600)
def load_rules():
    reg={
        "PASS_FAIL_DEFAULT":{"Type":"PASS_FAIL","Label":"Đạt / Không đạt"},
        "RATIO_UP_DEFAULT":{"Type":"RATIO_UP","Label":"Tăng – đạt/vượt kế hoạch"},
        "RATIO_DOWN_DEFAULT":{"Type":"RATIO_DOWN","Label":"Giảm – càng thấp càng tốt"},
        "PENALTY_ERR_004":{"Type":"PENALTY_ERR","thr":1.5,"step":0.1,"pen":0.04,"cap":3,"apply_weight":"false","Label":"Dự báo sai số ±1.5%, trừ 0.04/0.1% (max 3đ)"},
        "MANUAL_POINT":{"Type":"MANUAL","apply_weight":"false","Label":"Nhập tay điểm KPI"},
    }
    try:
        sh=_open_spreadsheet()
        try: ws=sh.worksheet("RULES")
        except: return reg
        df=pd.DataFrame(ws.get_all_records())
        for _,r in df.iterrows():
            code=str(r.get("Code") or "").strip().upper()
            if not code: continue
            t=str(r.get("Type") or "").strip().upper()
            if code not in reg:
                reg[code]={"Type":t,"Label":f"{code} ({t})"}
    except Exception:
        pass
    return reg

# ================== SCORING ==================
def _score_ratio_up(row):
    W=_w_ratio(row.get("Trọng số"))
    plan=_to_float(row.get("Kế hoạch")); actual=_to_float(row.get("Thực hiện"))
    if plan in (None,0) or actual is None: return None
    ratio=max(0.0,min(actual/plan,2.0))
    return round(ratio*10*W,2)

def _score_ratio_down(row):
    W=_w_ratio(row.get("Trọng số"))
    plan=_to_float(row.get("Kế hoạch")); actual=_to_float(row.get("Thực hiện"))
    if plan is None or actual is None: return None
    if actual<=plan: return round(10*W,2)
    over=(actual-plan)/plan
    return round(max(0.0,1-over*2)*10*W,2)

def _score_pass_fail(row):
    W=_w_ratio(row.get("Trọng số"))
    note=str(row.get("Ghi chú") or "").lower()
    plan=_to_float(row.get("Kế hoạch")); actual=_to_float(row.get("Thực hiện"))
    ok=None
    m=re.search(r"pass\s*=\s*(true|false|1|0)",note)
    if m: ok=m.group(1) in ("true","1")
    else: ok=(plan is not None and actual is not None and actual>=plan)
    return round(10*W if ok else 0.0,2)

def _score_penalty_err(row):
    W=_w_ratio(row.get("Trọng số"))
    thr,step,pen,cap = 1.5,0.1,0.04,3.0
    plan=_to_float(row.get("Kế hoạch")); actual=_to_float(row.get("Thực hiện"))
    if plan in (None,0) or actual is None: return None
    err=abs(actual-plan)/plan*100.0
    if err<=thr: return round(10*W,2)
    times=math.floor((err-thr)/step+1e-9)
    minus=min(cap,times*pen)
    return round(10*W - minus,2)

def _score_manual(row):
    v=_to_float(row.get("Điểm KPI"))
    if v is None:
        m=re.search(r"point\s*=\s*([0-9\.,\-]+)", str(row.get("Ghi chú") or ""))
        if m: v=_to_float(m.group(1))
    return None if v is None else float(v)

def compute_score(row):
    method=str(row.get("Phương pháp đo kết quả") or "")
    m=re.search(r"\[([A-Za-z0-9_]+)\]",method)
    code=m.group(1).upper() if m else ""
    if   code=="PENALTY_ERR_004": return _score_penalty_err(row)
    elif code=="MANUAL_POINT":    return _score_manual(row)
    ml=method.lower()
    if "đạt/không đạt" in ml: return _score_pass_fail(row)
    if "≤" in ml or "<=" in ml or "giảm tốt hơn" in ml: return _score_ratio_down(row)
    return _score_ratio_up(row)

# ================== STATE ==================
if "df" not in st.session_state: st.session_state.df=pd.DataFrame()
if "logged_in" not in st.session_state: st.session_state.logged_in=False
if "last_selected_index" not in st.session_state: st.session_state.last_selected_index=None
if "method_selected_code" not in st.session_state: st.session_state.method_selected_code="PASS_FAIL_DEFAULT"
if "_pending_sync" not in st.session_state: st.session_state._pending_sync=False

# ================== SIDEBAR (LOGIN + SHEETS) ==================
with st.sidebar:
    st.header("🔐 Đăng nhập (bắt buộc)")
    with st.form("login_form", clear_on_submit=False):
        st.text_input("USE (vd: PCTN\\KVDHA)", key="use_username")
        st.text_input("Mật khẩu", type="password", key="use_password")
        do_login=st.form_submit_button("Đăng nhập")
    if do_login:
        if st.session_state.use_username and st.session_state.use_password:
            st.session_state.logged_in=True
            st.success("Đăng nhập thành công.")
        else:
            st.error("Nhập đủ USE & Mật khẩu.")
    if st.button("Đăng xuất", use_container_width=True):
        st.session_state.logged_in=False
        st.info("Đã đăng xuất.")

    st.divider()
    st.header("🔗 Google Sheets")
    st.text_input("ID/URL Google Sheet", key="sheet_url_or_id", placeholder="Dán URL hoặc ID")
    st.text_input("Tên sheet KPI", key="kpi_sheet_name", value="KPI")

# ============== GUARD: stop if not login ==============
if not st.session_state.logged_in:
    st.warning("Vui lòng đăng nhập để bắt đầu làm việc.")
    st.stop()

# ================== AUTO-SYNC TRƯỚC KHI VẼ FORM ==================
def _sync_form_from_selected_index(idx):
    df = st.session_state.df
    r = df.loc[idx]
    st.session_state.form_kpi_name = str(r.get("Tên chỉ tiêu (KPI)") or r.get("Tên chỉ tiêu") or "")
    st.session_state.unit_txt      = str(r.get("Đơn vị tính") or "")
    st.session_state.dept_txt      = str(r.get("Bộ phận/người phụ trách") or "")
    st.session_state.owner_txt     = str(r.get("Tên đơn vị") or "")
    st.session_state.plan_txt      = str(r.get("Kế hoạch") or "")
    st.session_state.actual_txt    = str(r.get("Thực hiện") or "")
    st.session_state.weight_txt    = str(r.get("Trọng số") or "100")
    st.session_state.method_txt    = str(r.get("Phương pháp đo kết quả") or "")
    st.session_state.month_txt     = str(r.get("Tháng") or "")
    st.session_state.year_txt      = str(r.get("Năm") or str(datetime.now().year))
    st.session_state.note_txt      = str(r.get("Ghi chú") or "")
    # cập nhật combo theo [CODE] nếu có
    m = re.search(r"\[([A-Za-z0-9_]+)\]", st.session_state.method_txt)
    if m: st.session_state.method_selected_code = m.group(1).upper()

# Nếu có flag pending từ lần tick trước, đồng bộ rồi xóa flag (trước khi tạo widget)
if st.session_state._pending_sync and not st.session_state.df.empty and "Chọn" in st.session_state.df.columns:
    sel = st.session_state.df.index[st.session_state.df["Chọn"]==True].tolist()
    if len(sel)==1:
        st.session_state.last_selected_index = sel[0]
        _sync_form_from_selected_index(sel[0])
    st.session_state._pending_sync = False

# ================== VÙNG NHẬP LIỆU (STICKY – LUÔN Ở TRÊN) ==================
with st.container():
    st.markdown('<div class="kpi-stick">', unsafe_allow_html=True)

    r1 = st.columns([3,1,1,2])
    with r1[0]: name  = st.text_input("Tên chỉ tiêu (KPI)", key="form_kpi_name", value=st.session_state.get("form_kpi_name",""))
    with r1[1]: unit  = st.text_input("Đơn vị tính", key="unit_txt", value=st.session_state.get("unit_txt",""))
    with r1[2]: dept  = st.text_input("Bộ phận/người phụ trách", key="dept_txt", value=st.session_state.get("dept_txt",""))
    with r1[3]: owner = st.text_input("Tên đơn vị", key="owner_txt", value=st.session_state.get("owner_txt",""))

    r2 = st.columns([1,1,1,2])
    with r2[0]: plan   = st.text_input("Kế hoạch", key="plan_txt", value=st.session_state.get("plan_txt","0,00"))
    with r2[1]: actual = st.text_input("Thực hiện", key="actual_txt", value=st.session_state.get("actual_txt","0,00"))
    with r2[2]: weight = st.text_input("Trọng số (%)", key="weight_txt", value=st.session_state.get("weight_txt","100"))

    # Combo chọn RULES → tự gán [CODE] vào ô phương pháp
    rules = load_rules()
    preferred = ["PASS_FAIL_DEFAULT","RATIO_UP_DEFAULT","RATIO_DOWN_DEFAULT","PENALTY_ERR_004","MANUAL_POINT"]
    options = []
    for c in preferred + [c for c in rules.keys() if c not in preferred]:
        label = rules.get(c,{}).get("Label") or c
        options.append((f"{label}  [{c}]", c))
    labels = [o[0] for o in options]; codes = [o[1] for o in options]
    default_code = st.session_state.get("method_selected_code","PASS_FAIL_DEFAULT")
    try: idx = codes.index(default_code)
    except ValueError: idx = 0
    selected_label = st.selectbox("Chọn phương pháp", labels, index=idx, key="method_select_box")
    selected_code  = codes[labels.index(selected_label)]
    # cập nhật method_txt nếu khác
    if f"[{selected_code}]" not in (st.session_state.get("method_txt") or ""):
        st.session_state.method_txt = f"[{selected_code}]"

    with r2[3]:
        method = st.text_input("Phương pháp đo kết quả (có thể ghi [CODE]...)", key="method_txt", value=st.session_state.get("method_txt","[PASS_FAIL_DEFAULT]"))

    r3 = st.columns([1,1,2])
    with r3[0]: month = st.text_input("Tháng", key="month_txt", value=st.session_state.get("month_txt","7"))
    with r3[1]: year  = st.text_input("Năm", key="year_txt", value=st.session_state.get("year_txt", str(datetime.now().year)))
    with r3[2]: note  = st.text_input("Ghi chú", key="note_txt", value=st.session_state.get("note_txt",""))

    # Xem trước điểm
    def compute_score(row):
        mth=str(row.get("Phương pháp đo kết quả") or "")
        m=re.search(r"\[([A-Za-z0-9_]+)\]",mth)
        code=m.group(1).upper() if m else ""
        if   code=="PENALTY_ERR_004": return _score_penalty_err(row)
        elif code=="MANUAL_POINT":    return _score_manual(row)
        ml=mth.lower()
        if "đạt/không đạt" in ml: return _score_pass_fail(row)
        if "≤" in ml or "<=" in ml or "giảm tốt hơn" in ml: return _score_ratio_down(row)
        return _score_ratio_up(row)

    preview = compute_score({"Phương pháp đo kết quả":method,"Kế hoạch":plan,"Thực hiện":actual,"Trọng số":weight,"Ghi chú":note})
    st.markdown(f"<div class='total-box'>Điểm xem trước: <b>{'—' if preview is None else preview}</b></div>", unsafe_allow_html=True)

    # Hàng nút thao tác
    b = st.columns([1,1,1,1])
    with b[0]:
        st.markdown('<div class="btn-apply">', unsafe_allow_html=True)
        apply_btn = st.button("Áp dụng vào bảng CSV tạm", use_container_width=True)
        st.markdown('</div>', unsafe_allow_html=True)
    with b[1]:
        st.markdown('<div class="btn-save">', unsafe_allow_html=True)
        write_btn = st.button("💾 Ghi CSV tạm vào sheet KPI", use_container_width=True)
        st.markdown('</div>', unsafe_allow_html=True)
    with b[2]:
        st.markdown('<div class="btn-refresh">', unsafe_allow_html=True)
        refresh_btn = st.button("🔄 Làm mới bảng CSV", use_container_width=True)
        st.markdown('</div>', unsafe_allow_html=True)
    with b[3]:
        st.markdown('<div class="btn-export">', unsafe_allow_html=True)
        export_btn = st.button("📤 Xuất báo cáo (Excel)", use_container_width=True)
        st.markdown('</div>', unsafe_allow_html=True)

    st.markdown('</div>', unsafe_allow_html=True)  # /kpi-stick

# ================== BẢNG KPI (CSV TẠM) – nằm dưới form ==================
st.subheader("📋 Bảng KPI (CSV tạm)")
if not st.session_state.df.empty:
    df = st.session_state.df.copy()

    # Tính lại điểm
    def compute_score_for_df_row(r):
        return compute_score({"Phương pháp đo kết quả":r.get("Phương pháp đo kết quả"),
                              "Kế hoạch":r.get("Kế hoạch"),"Thực hiện":r.get("Thực hiện"),
                              "Trọng số":r.get("Trọng số"),"Ghi chú":r.get("Ghi chú")})
    new_scores=[]
    for _, r in df.iterrows():
        s=compute_score_for_df_row(r)
        new_scores.append(s if s is not None else r.get("Điểm KPI"))
    df["Điểm KPI"]=new_scores

    # Tổng điểm
    total=0.0
    for v in df["Điểm KPI"].tolist():
        vv=_to_float(v)
        if vv is not None: total+=vv
    st.markdown(f"<div class='total-box'>Tổng điểm KPI (tạm tính): <b>{_fmt_vn(total,2)}</b></div>", unsafe_allow_html=True)

    # Hiển thị editor (tick -> sau đây ta phát hiện và rerun)
    edited = st.data_editor(
        df,
        use_container_width=True,
        height=460,
        num_rows="dynamic",
        column_config={"Chọn": st.column_config.CheckboxColumn("Chọn", help="Tích để nạp form tự động")},
        disabled=[],  # cho phép tick
        key="kpi_editor"
    )
    # Cập nhật df vào state
    st.session_state.df = edited

    # >>> PHÁT HIỆN THAY ĐỔI SELECTION & RERUN NGAY <<<
    try:
        sel = edited.index[edited["Chọn"]==True].tolist()
    except Exception:
        sel = []
    # Chỉ auto khi đúng 1 dòng được tick
    if len(sel)==1:
        if st.session_state.get("last_selected_index") != sel[0]:
            # đánh dấu pending sync & rerun để form được nạp TRƯỚC khi vẽ widget
            st.session_state._pending_sync = True
            # chọn index mới để sync ở lượt sau
            # (không set form fields ở đây để tránh "cannot modify after widget instantiated")
            st.rerun()
else:
    st.info("Chưa có dữ liệu – vui lòng tải CSV mẫu ở phần bên dưới.")

# ================== ÁP DỤNG VÀO CSV ==================
if 'apply_btn' in locals() and apply_btn:
    if st.session_state.df.empty:
        st.warning("Chưa có bảng CSV.")
    else:
        df=st.session_state.df.copy()
        if "Chọn" not in df.columns:
            st.warning("Thiếu cột 'Chọn'.")
        else:
            mask=(df["Chọn"]==True)
            if mask.sum()==0:
                st.warning("Hãy tích chọn ít nhất 1 dòng.")
            else:
                df.loc[mask,"Tên chỉ tiêu (KPI)"]=st.session_state.form_kpi_name
                df.loc[mask,"Đơn vị tính"]=st.session_state.unit_txt
                df.loc[mask,"Bộ phận/người phụ trách"]=st.session_state.dept_txt
                df.loc[mask,"Tên đơn vị"]=st.session_state.owner_txt
                df.loc[mask,"Kế hoạch"]=st.session_state.plan_txt
                df.loc[mask,"Thực hiện"]=st.session_state.actual_txt
                df.loc[mask,"Trọng số"]=st.session_state.weight_txt
                df.loc[mask,"Phương pháp đo kết quả"]=st.session_state.method_txt
                df.loc[mask,"Tháng"]=st.session_state.month_txt
                df.loc[mask,"Năm"]=st.session_state.year_txt
                df.loc[mask,"Ghi chú"]=st.session_state.note_txt
                if preview is not None:
                    df.loc[mask,"Điểm KPI"]=preview
                st.session_state.df=df
                st.success(f"Đã áp dụng cho {mask.sum()} dòng.")

# ================== GHI SHEETS / LÀM MỚI / XUẤT EXCEL ==================
def _write_to_sheet(df_out: pd.DataFrame):
    sh=_open_spreadsheet()
    sheet_name=st.session_state.get("kpi_sheet_name","KPI")
    try:
        ws=sh.worksheet(sheet_name); ws.clear()
    except Exception:
        ws=sh.add_worksheet(title=sheet_name, rows=2000, cols=max(26,len(df_out.columns)))
    ws.update([df_out.columns.tolist()]+df_out.fillna("").astype(str).values.tolist(), value_input_option="USER_ENTERED")
    return True

if 'write_btn' in locals() and write_btn:
    if st.session_state.df.empty:
        st.warning("Không có dữ liệu để ghi.")
    else:
        try:
            _write_to_sheet(st.session_state.df.copy())
            st.success("Đã ghi dữ liệu lên Google Sheet.")
        except Exception as e:
            st.error(f"Lỗi khi ghi Sheets: {e}")

if 'refresh_btn' in locals() and refresh_btn:
    st.session_state.df=pd.DataFrame()
    st.session_state.last_selected_index=None
    st.session_state._pending_sync=False
    st.success("Đã làm mới bảng CSV.")

if 'export_btn' in locals() and export_btn:
    if st.session_state.df.empty:
        st.warning("Không có dữ liệu để xuất.")
    else:
        try:
            out=io.BytesIO()
            with pd.ExcelWriter(out, engine="xlsxwriter") as w:
                st.session_state.df.to_excel(w, sheet_name="KPI", index=False)
            st.download_button("⬇️ Tải Excel", data=out.getvalue(),
                               file_name=f"KPI_{datetime.now():%Y%m%d_%H%M}.xlsx",
                               mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")
            st.success("Đã tạo file Excel.")
        except Exception as e:
            st.error(f"Lỗi xuất báo cáo: {e}")

# ================== (ĐẶT Ở DƯỚI) NHẬP CSV ==================
st.subheader("📥 Nhập CSV vào KPI")
up = st.file_uploader("Tải file CSV (mẫu KPI_Input_template.csv)", type=["csv"], accept_multiple_files=False)
if up is not None:
    try:
        df=pd.read_csv(up)
        if "Chọn" not in df.columns: df.insert(0,"Chọn",False)
        needed=["Tên chỉ tiêu (KPI)","Đơn vị tính","Kế hoạch","Thực hiện","Trọng số",
                "Bộ phận/người phụ trách","Tháng","Năm","Phương pháp đo kết quả",
                "Điểm KPI","Ghi chú","Tên đơn vị"]
        for c in needed:
            if c not in df.columns: df[c]=""
        st.session_state.df=df
        st.session_state.last_selected_index=None
        st.session_state._pending_sync=False
        st.success("Đã nạp CSV.")
    except Exception as e:
        st.error(f"Lỗi đọc CSV: {e}")
