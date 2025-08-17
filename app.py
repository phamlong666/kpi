# -*- coding: utf-8 -*-
"""
KPI – Đội quản lý Điện lực khu vực Định Hóa
Bản đầy đủ (login bắt buộc, sticky form, chọn dòng → nạp form, RULES: RATIO_UP/DOWN, PASS_FAIL,
PENALTY_ERR (0.04/0.1% tối đa 3đ), MANUAL, ghi Google Sheets, xuất Excel, xác nhận làm mới,
nút nhiều màu, tiêu đề có màu, tổng điểm KPI).
"""

import re, io, json, math, base64
from datetime import datetime
from pathlib import Path

import pandas as pd
import streamlit as st

# ================== PAGE CONFIG & CSS ==================
st.set_page_config(page_title="KPI – Định Hóa", page_icon="📊", layout="wide")

st.markdown("""
<style>
/* Title màu & nhỏ lại */
.app-title h1{font-size:1.6rem !important;margin:0;color:#0ea5e9 !important;}
.app-sub{color:#64748b;font-size:.9rem;margin:.2rem 0 1rem;}

/* Sticky form */
.sticky-box{
  position:sticky;top:0;z-index:50;background:var(--background-color);
  border:1px solid #e5e7eb;border-radius:12px;padding:12px;
  box-shadow:0 2px 10px rgba(0,0,0,.05);
}

/* Nút màu (bọc bằng div) */
.btn-save  button{background:#22c55e !important;color:#fff !important;border:0 !important}
.btn-clear button{background:#f59e0b !important;color:#111 !important;border:0 !important}
.btn-export button{background:#3b82f6 !important;color:#fff !important;border:0 !important}
.btn-drive button{background:#8b5cf6 !important;color:#fff !important;border:0 !important}
.btn-apply button{background:#0ea5e9 !important;color:#fff !important;border:0 !important}

/* Tổng điểm */
.total-box{background:#f0f9ff;border:1px dashed #7dd3fc;padding:8px 12px;border-radius:10px;color:#0c4a6e;font-weight:600}

/* Nhẹ phần top padding */
section.main > div.block-container{padding-top:.7rem}
</style>
""", unsafe_allow_html=True)

# ================== HEADER ==================
LOGO_URL = "https://raw.githubusercontent.com/phamlong666/kpi/main/logo_hinh_tron.png"
c1,c2 = st.columns([1,10], vertical_alignment="center")
with c1:
    st.image(LOGO_URL, width=60)
with c2:
    st.markdown('<div class="app-title"><h1>KPI – Đội quản lý Điện lực khu vực Định Hóa</h1></div>', unsafe_allow_html=True)
    st.markdown('<div class="app-sub">Biểu mẫu nhập tay & chấm điểm linh hoạt theo RULES (cấu hình trong Google Sheets)</div>', unsafe_allow_html=True)

# ================== UTIL: number ==================
def parse_float(x):
    try:
        if x is None: return None
        if isinstance(x,(int,float)): return float(x)
        s = str(x).strip()
        if s=="" or s.lower() in ("none","nan"): return None
        s = s.replace(" ", "").replace(".", "").replace(",", ".")
        return float(s)
    except: return None

def format_vn(v, nd=2):
    if v is None: return ""
    try: v = round(float(v), nd)
    except: return str(v)
    s = f"{v:,.{nd}f}".replace(",", "_").replace(".", ",").replace("_", ".")
    return s

def weight_to_ratio(w):
    v = parse_float(w)
    if v is None: return 0.0
    # nếu nhập 40 nghĩa 40% thì dùng 40/100; nếu nhập 0.4 thì dùng 0.4
    return v/100.0 if v>1 else v

# ================== GOOGLE SHEETS (optional) ==================
try:
    import gspread
    from google.oauth2.service_account import Credentials
    HAS_GS = True
except Exception:
    HAS_GS = False

SCOPES = [
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive",
    "https://www.googleapis.com/auth/drive.file",
]

def extract_sheet_id(url_or_id:str)->str:
    return url_or_id.split("/d/")[1].split("/")[0] if "/d/" in url_or_id else url_or_id

def get_spreadsheet():
    if not HAS_GS: raise RuntimeError("Môi trường chưa cài gspread/google-auth.")
    sid = st.session_state.get("sheet_url_or_id","").strip()
    if not sid: raise RuntimeError("Chưa nhập ID/URL Google Sheet.")
    if "gcp_service_account" in st.secrets:
        info = dict(st.secrets["gcp_service_account"])
        creds = Credentials.from_service_account_info(info, scopes=SCOPES)
    elif Path("credentials.json").exists():
        creds = Credentials.from_service_account_file("credentials.json", scopes=SCOPES)
    else:
        raise RuntimeError("Thiếu thông tin Service Account (secrets['gcp_service_account'] hoặc credentials.json).")
    gc = gspread.authorize(creds)
    return gc.open_by_key(extract_sheet_id(sid))

# ================== RULES (registry) ==================
@st.cache_data(ttl=600)
def load_rules():
    """Đọc sheet RULES nếu có, nếu không dùng mặc định."""
    reg = {
        "RATIO_UP_DEFAULT": {"Type":"RATIO_UP"},
        "RATIO_DOWN_DEFAULT": {"Type":"RATIO_DOWN"},
        "PASS_FAIL_DEFAULT": {"Type":"PASS_FAIL"},
        "PENALTY_ERR_004": {"Type":"PENALTY_ERR","thr":1.5,"step":0.1,"pen":0.04,"cap":3,"apply_weight":"false"},
        "MANUAL_POINT": {"Type":"MANUAL","apply_weight":"false"},
    }
    try:
        sh = get_spreadsheet()
        try:
            ws = sh.worksheet("RULES")
        except Exception:
            return reg
        df = pd.DataFrame(ws.get_all_records())
        for _,r in df.iterrows():
            code = str(r.get("Code") or "").strip().upper()
            if not code: continue
            rule = {k:r.get(k) for k in df.columns}
            rule["Type"] = str(rule.get("Type") or "").strip().upper()
            for k in ("thr","step","pen","cap","lo","hi"):
                if k in rule: rule[k] = parse_float(rule[k])
            reg[code]=rule
    except Exception:
        pass
    return reg

def ensure_rules_template():
    """Tạo/cập nhật RULES mẫu."""
    sh = get_spreadsheet()
    headers = ["Code","Type","keywords","thr","step","pen","cap","op","lo","hi","metric","apply_weight","points_json","expr","description"]
    rows = [
        ["RATIO_UP_DEFAULT","RATIO_UP","tăng tốt hơn; ≥","","","","","","","","","TRUE","","","Tăng đạt/vượt: min(ACTUAL/PLAN,2)*10*W"],
        ["RATIO_DOWN_DEFAULT","RATIO_DOWN","giảm tốt hơn; ≤","","","","","","","","","TRUE","","","Giảm càng tốt"],
        ["PASS_FAIL_DEFAULT","PASS_FAIL","đạt/không đạt","","","","","","","","","TRUE","","","Đạt = 10*W"],
        ["PENALTY_ERR_004","PENALTY_ERR","sai số ±1,5%; trừ 0,04","1.5","0.1","0.04","3","","","","","FALSE","","","Dự báo – trừ tối đa 3đ"],
        ["MANUAL_POINT","MANUAL","nhập tay","","","","","","","","","FALSE","","","Điểm nhập tay ở cột Điểm KPI hoặc ghi chú: point=..."],
    ]
    try:
        try:
            ws = sh.worksheet("RULES"); ws.clear()
        except Exception:
            ws = sh.add_worksheet(title="RULES", rows=100, cols=len(headers))
        ws.update([headers]+rows, value_input_option="USER_ENTERED")
        return True
    except Exception as e:
        st.error(f"Lỗi tạo RULES: {e}")
        return False

# ================== SCORING ==================
def _score_ratio_up(row):
    W = weight_to_ratio(row.get("Trọng số"))
    plan = parse_float(row.get("Kế hoạch")); actual = parse_float(row.get("Thực hiện"))
    if plan in (None,0) or actual is None: return None
    ratio = max(0.0, min(actual/plan, 2.0))
    return round(ratio*10*W, 2)

def _score_ratio_down(row):
    W = weight_to_ratio(row.get("Trọng số"))
    plan = parse_float(row.get("Kế hoạch")); actual = parse_float(row.get("Thực hiện"))
    if plan is None or actual is None: return None
    if actual <= plan: return round(10*W,2)
    over = (actual-plan)/plan
    return round(max(0.0, 1 - over*2)*10*W, 2)

def _score_pass_fail(row):
    W = weight_to_ratio(row.get("Trọng số"))
    note = str(row.get("Ghi chú") or "").lower()
    plan = parse_float(row.get("Kế hoạch")); actual = parse_float(row.get("Thực hiện"))
    ok = None
    m = re.search(r"pass\s*=\s*(true|false|1|0)", note)
    if m: ok = m.group(1) in ("true","1")
    else: ok = (plan is not None and actual is not None and actual>=plan)
    return round(10*W if ok else 0.0, 2)

def _score_penalty_err(row, rule):
    """Sai số ±1.5% rồi trừ 0.04/0.1% đến tối đa cap=3 điểm. (chỉ trừ)"""
    W = weight_to_ratio(row.get("Trọng số"))
    thr  = rule.get("thr",1.5); step = rule.get("step",0.1)
    pen  = rule.get("pen",0.04); cap  = rule.get("cap",3.0)
    plan = parse_float(row.get("Kế hoạch")); actual = parse_float(row.get("Thực hiện"))
    if plan in (None,0) or actual is None: return None
    err = abs(actual-plan)/plan*100.0
    if err<=thr: return round(10*W,2)  # ko trừ
    times = math.floor((err-thr)/step + 1e-9)
    minus = min(cap, times*pen)
    # tuỳ chính sách: trả "10*W - minus" (điểm đã trừ) hay trả "-minus" (điểm trừ)
    # Ở đây ta trả điểm sau trừ để cộng tổng (nếu KPI này chỉ trừ, trọng số nên là 100 hoặc phù hợp).
    return round(10*W - minus, 2)

def _score_manual(row):
    v = parse_float(row.get("Điểm KPI"))
    if v is None:
        m = re.search(r"point\s*=\s*([0-9\.,\-]+)", str(row.get("Ghi chú") or ""))
        if m: v = parse_float(m.group(1))
    return None if v is None else float(v)

def compute_score(row, reg):
    method = str(row.get("Phương pháp đo kết quả") or "")
    # Ưu tiên [CODE]
    m = re.search(r"\[([A-Za-z0-9_]+)\]", method)
    code = m.group(1).upper() if m else ""
    rule = reg.get(code)
    t = (rule.get("Type") if rule else "").upper()
    if   t=="RATIO_UP":    return _score_ratio_up(row)
    elif t=="RATIO_DOWN":  return _score_ratio_down(row)
    elif t=="PASS_FAIL":   return _score_pass_fail(row)
    elif t=="PENALTY_ERR": return _score_penalty_err(row, rule)
    elif t=="MANUAL":      return _score_manual(row)
    # fallback theo từ khóa
    ml = method.lower()
    if "đạt/không đạt" in ml: return _score_pass_fail(row)
    if "≤" in ml or "<=" in ml: return _score_ratio_down(row)
    return _score_ratio_up(row)

# ================== STATE ==================
if "df" not in st.session_state: st.session_state.df = pd.DataFrame()
if "logged_in" not in st.session_state: st.session_state.logged_in = False
if "confirm_clear" not in st.session_state: st.session_state.confirm_clear = False

# ================== SIDEBAR: LOGIN & SETTINGS ==================
with st.sidebar:
    st.header("🔐 Đăng nhập (bắt buộc)")
    with st.form("login_form", clear_on_submit=False):
        st.text_input("USE (vd: PCTN\\KVDHA)", key="use_username")
        st.text_input("Mật khẩu", type="password", key="use_password")
        do_login = st.form_submit_button("Đăng nhập")
    if do_login:
        if st.session_state.use_username and st.session_state.use_password:
            st.session_state.logged_in = True
            st.success("Đăng nhập thành công.")
        else:
            st.error("Nhập đủ USE & Mật khẩu.")

    if st.button("Đăng xuất", use_container_width=True):
        st.session_state.logged_in = False
        st.info("Đã đăng xuất.")

    st.divider()
    st.header("🔗 Google Sheets")
    st.text_input("ID/URL Google Sheet", key="sheet_url_or_id", placeholder="Dán URL hoặc ID")
    st.text_input("Tên sheet KPI", key="kpi_sheet_name", value="KPI")

    cA,cB = st.columns(2)
    with cA:
        if st.button("📄 Tạo/ cập nhật RULES (mẫu)", use_container_width=True):
            try:
                if ensure_rules_template():
                    st.success("Đã tạo/cập nhật RULES.")
            except Exception as e:
                st.error(f"Lỗi: {e}")
    with cB:
        if st.button("🔁 Nạp lại RULES", use_container_width=True):
            load_rules.clear()
            st.success("Đã làm mới RULES cache.")

# ---------------- BLOCK: stop if not logged in ----------------
if not st.session_state.logged_in:
    st.warning("Vui lòng đăng nhập để làm việc.")
    st.stop()

# ================== CSV INPUT ==================
st.subheader("📥 Nhập CSV vào KPI")
up = st.file_uploader("Tải file CSV (mẫu KPI_Input_template.csv)", type=["csv"], accept_multiple_files=False, label_visibility="collapsed")
if up is not None:
    try:
        df = pd.read_csv(up)
        if "Chọn" not in df.columns: df.insert(0,"Chọn",False)
        # đảm bảo các cột chính
        needed = ["Tên chỉ tiêu (KPI)","Đơn vị tính","Kế hoạch","Thực hiện","Trọng số","Bộ phận/người phụ trách",
                  "Tháng","Năm","Phương pháp đo kết quả","Điểm KPI","Ghi chú","Tên đơn vị"]
        for c in needed:
            if c not in df.columns: df[c] = ""
        st.session_state.df = df
        st.success("Đã nạp CSV.")
    except Exception as e:
        st.error(f"Lỗi đọc CSV: {e}")

# ================== STICKY FORM ==================
with st.container():
    st.markdown('<div class="sticky-box">', unsafe_allow_html=True)

    # Nút nạp dòng đã chọn lên form (đảm bảo method/plan/actual… được load)
    if st.button("⬆️ Nạp dòng đã tích lên biểu mẫu", use_container_width=True):
        if st.session_state.df.empty:
            st.warning("Chưa có bảng CSV.")
        else:
            df = st.session_state.df
            if "Chọn" in df.columns and df["Chọn"].sum()==1:
                r = df[df["Chọn"]==True].iloc[0]
                # set vào session_state để làm default value cho widget
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
                st.success("Đã nạp dòng được chọn.")
            else:
                st.warning("Hãy tích chọn đúng 1 dòng.")

    c1,c2,c3,c4 = st.columns([3,1,1,2])
    with c1: name = st.text_input("Tên chỉ tiêu (KPI)", key="form_kpi_name", value=st.session_state.get("form_kpi_name",""))
    with c2: unit = st.text_input("Đơn vị tính", key="unit_txt", value=st.session_state.get("unit_txt",""))
    with c3: dept = st.text_input("Bộ phận/người phụ trách", key="dept_txt", value=st.session_state.get("dept_txt",""))
    with c4: owner= st.text_input("Tên đơn vị", key="owner_txt", value=st.session_state.get("owner_txt",""))

    c5,c6,c7 = st.columns([1,1,1])
    with c5: plan   = st.text_input("Kế hoạch", key="plan_txt", value=st.session_state.get("plan_txt","0,00"))
    with c6: actual = st.text_input("Thực hiện", key="actual_txt", value=st.session_state.get("actual_txt","0,00"))
    with c7: weight = st.text_input("Trọng số (%)", key="weight_txt", value=st.session_state.get("weight_txt","100"))

    method = st.text_input("Phương pháp đo kết quả (có thể ghi [CODE]...)", key="method_txt", value=st.session_state.get("method_txt","Đạt/Không đạt"))

    c8,c9,c10 = st.columns([1,1,2])
    with c8: month = st.text_input("Tháng", key="month_txt", value=st.session_state.get("month_txt","7"))
    with c9: year  = st.text_input("Năm", key="year_txt", value=st.session_state.get("year_txt",str(datetime.now().year)))
    with c10: note = st.text_input("Ghi chú", key="note_txt", value=st.session_state.get("note_txt",""))

    # Xem trước điểm theo RULES
    reg = load_rules()
    preview = compute_score({"Phương pháp đo kết quả":method,"Kế hoạch":plan,"Thực hiện":actual,"Trọng số":weight,"Ghi chú":note}, reg)
    st.markdown(f"<div class='total-box'>Điểm xem trước: <b>{'—' if preview is None else preview}</b></div>", unsafe_allow_html=True)

    # Hàng nút thao tác
    b0,b1,b2,b3,b4 = st.columns([1.1,1,1,1,1])
    with b0:
        st.markdown('<div class="btn-apply">', unsafe_allow_html=True)
        apply_btn = st.button("Áp dụng vào bảng CSV tạm", use_container_width=True)
        st.markdown('</div>', unsafe_allow_html=True)
    with b1:
        st.markdown('<div class="btn-save">', unsafe_allow_html=True)
        write_btn = st.button("💾 Ghi CSV tạm vào sheet KPI", use_container_width=True)
        st.markdown('</div>', unsafe_allow_html=True)
    with b2:
        st.markdown('<div class="btn-clear">', unsafe_allow_html=True)
        clear_btn = st.button("🔄 Làm mới bảng CSV", use_container_width=True)
        st.markdown('</div>', unsafe_allow_html=True)
    with b3:
        st.markdown('<div class="btn-export">', unsafe_allow_html=True)
        export_btn = st.button("📤 Xuất báo cáo (Excel)", use_container_width=True)
        st.markdown('</div>', unsafe_allow_html=True)
    with b4:
        st.markdown('<div class="btn-drive">', unsafe_allow_html=True)
        drive_btn = st.button("☁️ Lưu dữ liệu (ghi lại lên Sheet)", use_container_width=True)
        st.markdown('</div>', unsafe_allow_html=True)

    st.markdown('</div>', unsafe_allow_html=True)  # /sticky-box

# ================== ÁP DỤNG VÀO BẢNG CSV ==================
if apply_btn:
    if st.session_state.df.empty:
        st.warning("Chưa có bảng CSV.")
    else:
        df = st.session_state.df.copy()
        if "Chọn" not in df.columns:
            st.warning("Chưa có cột 'Chọn' để đánh dấu dòng.")
        else:
            mask = (df["Chọn"]==True)
            if mask.sum()==0:
                st.warning("Hãy tích chọn ít nhất 1 dòng.")
            else:
                df.loc[mask,"Tên chỉ tiêu (KPI)"] = name
                df.loc[mask,"Đơn vị tính"] = unit
                df.loc[mask,"Bộ phận/người phụ trách"] = dept
                df.loc[mask,"Tên đơn vị"] = owner
                df.loc[mask,"Kế hoạch"] = plan
                df.loc[mask,"Thực hiện"] = actual
                df.loc[mask,"Trọng số"] = weight
                df.loc[mask,"Phương pháp đo kết quả"] = method
                df.loc[mask,"Tháng"] = month
                df.loc[mask,"Năm"] = year
                df.loc[mask,"Ghi chú"] = note
                if preview is not None:
                    df.loc[mask,"Điểm KPI"] = preview
                st.session_state.df = df
                st.success(f"Đã áp dụng cho {mask.sum()} dòng.")

# ================== BẢNG KPI ==================
st.subheader("📋 Bảng KPI (CSV tạm)")
if not st.session_state.df.empty:
    df = st.session_state.df.copy()

    # Tính lại điểm (trừ khi MANUAL đã điền)
    reg = load_rules()
    sc = []
    for _, r in df.iterrows():
        s = compute_score(r, reg)
        sc.append(s if s is not None else r.get("Điểm KPI"))
    df["Điểm KPI"] = sc

    # Tổng điểm
    total = 0.0
    for v in df["Điểm KPI"].tolist():
        vv = parse_float(v)
        if vv is not None: total += vv
    st.markdown(f"<div class='total-box'>Tổng điểm KPI (tạm tính): <b>{format_vn(total,2)}</b></div>", unsafe_allow_html=True)

    edited = st.data_editor(
        df,
        use_container_width=True,
        num_rows="dynamic",
        height=460,
        column_config={"Chọn": st.column_config.CheckboxColumn("Chọn", help="Tích để nạp dòng lên biểu mẫu")},
        disabled=[],   # cho phép tick chọn
        key="kpi_editor",
    )
    st.session_state.df = edited
else:
    st.info("Chưa có dữ liệu – vui lòng tải CSV mẫu lên.")

# ================== GHI SHEET / LÀM MỚI / XUẤT EXCEL ==================
def write_to_sheet(df_out: pd.DataFrame):
    sh = get_spreadsheet()
    sheet_name = st.session_state.get("kpi_sheet_name","KPI")
    try:
        ws = sh.worksheet(sheet_name); ws.clear()
    except Exception:
        ws = sh.add_worksheet(title=sheet_name, rows=2000, cols=max(26, len(df_out.columns)))
    ws.update([df_out.columns.tolist()] + df_out.fillna("").astype(str).values.tolist(), value_input_option="USER_ENTERED")
    return True

if write_btn:
    if st.session_state.df.empty:
        st.warning("Không có dữ liệu để ghi.")
    else:
        try:
            write_to_sheet(st.session_state.df.copy())
            st.success("Đã ghi dữ liệu lên Google Sheet.")
        except Exception as e:
            st.error(f"Lỗi khi ghi Sheets: {e}")

if clear_btn:
    # xác nhận làm mới
    st.session_state.confirm_clear = True

if st.session_state.confirm_clear:
    with st.expander("❓ Xác nhận làm mới bảng CSV? (Sẽ mất thay đổi chưa ghi)", expanded=True):
        c1,c2 = st.columns(2)
        if c1.button("Có, làm mới ngay", type="primary"):
            st.session_state.df = pd.DataFrame()
            st.session_state.confirm_clear = False
            st.success("Đã làm mới bảng CSV.")
        if c2.button("Không, giữ nguyên"):
            st.session_state.confirm_clear = False

if export_btn:
    if st.session_state.df.empty:
        st.warning("Không có dữ liệu để xuất.")
    else:
        try:
            out = io.BytesIO()
            with pd.ExcelWriter(out, engine="xlsxwriter") as writer:
                st.session_state.df.to_excel(writer, sheet_name="KPI", index=False)
            st.download_button("⬇️ Tải Excel", data=out.getvalue(),
                               file_name=f"KPI_{datetime.now():%Y%m%d_%H%M}.xlsx",
                               mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")
            st.success("Đã tạo file Excel.")
        except Exception as e:
            st.error(f"Lỗi xuất: {e}")

if drive_btn:
    # Hiện tại: ghi lại lên Sheet như một cách "lưu" an toàn (nếu cần upload file Drive riêng, có thể bổ sung sau).
    if st.session_state.df.empty:
        st.warning("Không có dữ liệu để lưu.")
    else:
        try:
            write_to_sheet(st.session_state.df.copy())
            st.success("Đã lưu (ghi lại) dữ liệu lên Google Sheet.")
        except Exception as e:
            st.error(f"Lỗi lưu: {e}")
