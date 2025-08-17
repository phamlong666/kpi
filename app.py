# -*- coding: utf-8 -*-
# KPI – Đội quản lý Điện lực khu vực Định Hóa
# Bản đã tích hợp RULES (BONUS/MANUAL/PENALTY_ERR), sticky form, nút nhiều màu, tính Tổng điểm.

import re
import json
import io
import math
from datetime import datetime
from pathlib import Path

import pandas as pd
import numpy as np
import streamlit as st

# ==== CẤU HÌNH TRANG ====
st.set_page_config(
    page_title="KPI – ĐLKV Định Hóa",
    page_icon="📊",
    layout="wide",
    initial_sidebar_state="expanded",
)

# ==== CSS: tiêu đề nhỏ, sticky form, màu nút ====
st.markdown(
    """
    <style>
      /* Thu nhỏ title */
      .app-title h1 {font-size: 1.6rem !important; margin-bottom: 0.25rem;}
      .app-subnote {color:#666; font-size:0.9rem; margin-bottom:1rem}

      /* Form sticky */
      .sticky-box {
         position: sticky; top: 0; z-index: 20;
         background: var(--background-color);
         border: 1px solid #eee; border-radius: 12px; padding: 12px;
         box-shadow: 0 2px 8px rgba(0,0,0,0.04);
         margin-bottom: 10px;
      }

      /* Màu nút riêng */
      .btn-green button{background:#0ea5e9 !important;color:white !important}
      .btn-blue button{background:#10b981 !important;color:white !important}
      .btn-orange button{background:#f59e0b !important;color:white !important}
      .btn-purple button{background:#8b5cf6 !important;color:white !important}
      .btn-red button{background:#ef4444 !important;color:white !important}

      /* Ô tổng điểm */
      .total-box {
        background:#f0f9ff; border:1px dashed #7dd3fc;
        padding:10px 14px;border-radius:10px;color:#0c4a6e;
        font-weight:600;margin-top:8px;
      }

      /* Data editor chiều cao dễ xem */
      section.main > div.block-container{padding-top: 0.8rem;}
    </style>
    """,
    unsafe_allow_html=True,
)

# ==== LOGO ====
LOGO_URL = "https://raw.githubusercontent.com/phamlong666/kpi/main/logo_hinh_tron.png"
col_logo, col_title = st.columns([1,10], vertical_alignment="center")
with col_logo:
    st.image(LOGO_URL, width=60)
with col_title:
    st.markdown('<div class="app-title"><h1>KPI – Đội quản lý Điện lực khu vực Định Hóa</h1></div>', unsafe_allow_html=True)
    st.markdown('<div class="app-subnote">Biểu mẫu nhập tay + chấm điểm theo RULES (cấu hình trong Sheets)</div>', unsafe_allow_html=True)

# ----------------- Helpers -----------------
def parse_float(x):
    try:
        if x is None or (isinstance(x, float) and math.isnan(x)):
            return None
        if isinstance(x, (int, float)):
            return float(x)
        s = str(x).strip()
        if s == "" or s.lower() in ("none", "nan"):
            return None
        # Việt Nam number "1.234.567,89"
        s = s.replace(" ", "")
        s = s.replace(".", "").replace(",", ".")
        return float(s)
    except Exception:
        return None

def parse_vn_number(s):
    return parse_float(s)

def format_vn_number(v, nd=2):
    if v is None: return ""
    try:
        q = round(float(v), nd)
    except Exception:
        return str(v)
    s = f"{q:,.{nd}f}"
    # chuyển 1,234,567.89 -> 1.234.567,89
    s = s.replace(",", "X").replace(".", ",").replace("X", ".")
    return s

def _coerce_weight(w):
    v = parse_float(w)
    if v is None: return 0.0
    return max(0.0, float(v))

# -------------- Google Sheets ----------------
# Cho phép chạy khi không có gspread
try:
    import gspread
    from google.oauth2.service_account import Credentials
    HAS_GSPREAD = True
except Exception:
    HAS_GSPREAD = False

SCOPES = [
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive",
    "https://www.googleapis.com/auth/drive.file",
]

def get_sheet_and_name():
    """Mở spreadsheet theo sidebar input. Trả (spreadsheet, sheet_name)"""
    url_or_id = st.session_state.get("sheet_url_or_id", "").strip()
    sheet_name = st.session_state.get("kpi_sheet_name", "KPI")
    if not HAS_GSPREAD: raise RuntimeError("Thiếu gspread (môi trường chưa cài).")
    if not url_or_id:
        raise RuntimeError("Chưa nhập ID/URL Google Sheet.")
    # Service account credentials: Streamlit secrets hoặc file credentials.json nếu có
    creds = None
    if "gcp_service_account" in st.secrets:
        info = dict(st.secrets["gcp_service_account"])
        creds = Credentials.from_service_account_info(info, scopes=SCOPES)
    else:
        # thử file credentials.json
        if Path("credentials.json").exists():
            creds = Credentials.from_service_account_file("credentials.json", scopes=SCOPES)
        else:
            raise RuntimeError("Không tìm thấy thông tin Service Account (secrets['gcp_service_account'] hoặc credentials.json).")
    gc = gspread.authorize(creds)
    sh = gc.open_by_key(extract_sheet_id(url_or_id))
    return sh, sheet_name

def extract_sheet_id(url_or_id: str) -> str:
    """Lấy spreadsheetId từ URL hoặc trả luôn id"""
    if "/d/" in url_or_id:
        # URL dạng https://docs.google.com/spreadsheets/d/<ID>/edit...
        return url_or_id.split("/d/")[1].split("/")[0]
    return url_or_id

# -------------- RULES Loader ----------------
@st.cache_data(ttl=600)
def load_rules_registry(spreadsheet_id_or_url: str):
    """Đọc RULES từ Google Sheet (sheet RULES). Nếu không có/ lỗi → trả default."""
    try:
        if not HAS_GSPREAD or not spreadsheet_id_or_url:
            return default_rules()
        sh, _ = get_sheet_and_name()
        try:
            ws = sh.worksheet("RULES")
        except Exception:
            return default_rules()

        df = pd.DataFrame(ws.get_all_records())
        reg = {}
        for _, r in df.iterrows():
            code = str(r.get("Code") or "").strip()
            if not code: continue
            rule = {k: r.get(k) for k in df.columns}
            # Chuẩn hoá
            rule["Type"] = str(rule.get("Type") or "").strip().upper()
            rule["keywords"] = str(rule.get("keywords") or "").lower()
            rule["thr"] = parse_float(rule.get("thr"))
            rule["step"] = parse_float(rule.get("step"))
            rule["pen"] = parse_float(rule.get("pen"))
            rule["cap"] = parse_float(rule.get("cap"))
            rule["op"] = str(rule.get("op") or "").strip()
            rule["lo"] = parse_float(rule.get("lo"))
            rule["hi"] = parse_float(rule.get("hi"))
            rule["metric"] = str(rule.get("metric") or "").strip().lower()
            rule["apply_weight"] = str(rule.get("apply_weight") or "").strip()
            rule["points_json"] = rule.get("points_json")
            rule["expr"] = rule.get("expr")
            reg[code.upper()] = rule
        if not reg:
            return default_rules()
        return reg
    except Exception:
        return default_rules()

def default_rules():
    """Bộ rule mặc định nếu không có RULES sheet."""
    reg = {
        "RATIO_UP_DEFAULT": {"Type":"RATIO_UP"},
        "RATIO_DOWN_DEFAULT": {"Type":"RATIO_DOWN"},
        "PASS_FAIL_DEFAULT": {"Type":"PASS_FAIL"},
        "RANGE_DEFAULT": {"Type":"RANGE"},
        "PENALTY_ERR_004": {"Type":"PENALTY_ERR","thr":1.5,"step":0.1,"pen":0.04,"cap":3,"apply_weight":"false"},
        "PENALTY_ERR_002": {"Type":"PENALTY_ERR","thr":1.5,"step":0.1,"pen":0.02,"cap":3,"apply_weight":"false"},
        "BONUS_RATIO_TIER": {"Type":"BONUS","metric":"ratio","apply_weight":"false",
            "points_json": json.dumps({"tiers":[{"gte":0.98,"lte":0.99,"point":0.5},{"gt":0.99,"lt":1.0,"point":0.7},{"gte":1.0,"point":1.0}]})
        },
        "MANUAL_POINT": {"Type":"MANUAL","apply_weight":"false"},
    }
    return reg

def ensure_rules_template(sh):
    headers = ["Code","Type","keywords","thr","step","pen","cap","op","lo","hi","metric","apply_weight","points_json","expr","description"]
    try:
        ws = sh.worksheet("RULES"); ws.clear()
    except Exception:
        ws = sh.add_worksheet(title="RULES", rows=100, cols=len(headers))
    rows = [
        ["RATIO_UP_DEFAULT","RATIO_UP","tăng tốt hơn; ≥","","","","","","","","","TRUE","","","Tăng đạt/vượt KH – điểm = min(ACTUAL/PLAN,2)*10*W"],
        ["RATIO_DOWN_DEFAULT","RATIO_DOWN","giảm tốt hơn; ≤","","","","","","","","","TRUE","","","Giảm càng tốt"],
        ["PASS_FAIL_DEFAULT","PASS_FAIL","đạt/không đạt","","","","","","","","","TRUE","","","Đạt = 10*W"],
        ["RANGE_DEFAULT","RANGE","khoảng; range","","","","","","","","","TRUE","","","LO≤ACTUAL≤HI ⇒ 10*W"],
        ["PENALTY_ERR_004","PENALTY_ERR","sai số ±1,5%; trừ 0,04","1.5","0.1","0.04","3","","","","","FALSE","","","Dự báo – trừ tối đa 3đ"],
        ["PENALTY_ERR_002","PENALTY_ERR","sai số ±1,5%; trừ 0,02","1.5","0.1","0.02","3","","","","","FALSE","","","Dự báo – trừ tối đa 3đ"],
        ["BONUS_RATIO_TIER","BONUS","cộng điểm theo tỷ lệ","","","","","","","","ratio","FALSE",
         '{"tiers":[{"gte":0.98,"lte":0.99,"point":0.5},{"gt":0.99,"lt":1.0,"point":0.7},{"gte":1.0,"point":1.0}]}',"","98–99% +0.5; 99–<100% +0.7; ≥100% +1.0"],
        ["MANUAL_POINT","MANUAL","nhập tay","","","","","","","","","FALSE","","","Nhập tay điểm ở cột Điểm KPI hoặc Ghi chú: point=..."],
    ]
    ws.update([headers] + rows, value_input_option="USER_ENTERED")
    return True

# -------------- Scoring core ----------------
def find_rule_for_row(row, registry):
    """Tìm rule theo [CODE] trong 'Phương pháp đo kết quả' hoặc theo từ khóa"""
    method = str(row.get("Phương pháp đo kết quả") or "")
    # Ưu tiên [CODE] trong method
    m = re.search(r"\[(?P<code>[A-Za-z0-9_]+)\]", method)
    overrides = {}
    if m:
        code = m.group("code").upper()
        # đọc tham số "k=v; ..."
        kvs = re.findall(r"([a-zA-Z_]+)\s*=\s*([^;,\]]+)", method)
        for k, v in kvs:
            k=k.strip().lower(); overrides[k]=v.strip()
        rule = registry.get(code)
        if rule: return rule, overrides
    # Không có CODE: tìm theo keywords
    method_l = method.lower()
    for code, rule in registry.items():
        kw = str(rule.get("keywords") or "").lower().strip()
        if kw and kw in method_l:
            return rule, {}
    # Fallback
    if "đạt" in method_l:
        return registry.get("PASS_FAIL_DEFAULT"), {}
    return registry.get("RATIO_UP_DEFAULT"), {}  # mặc định

def _score_ratio_up(row):
    W = _coerce_weight(row.get("Trọng số"))
    plan = parse_float(row.get("Kế hoạch"))
    actual = parse_float(row.get("Thực hiện"))
    if plan in (None,0) or actual is None:
        return None
    ratio = max(0.0, min(actual/plan, 2.0)) # chặn 200%
    return round(ratio * 10.0 * W, 2)

def _score_ratio_down(row):
    W = _coerce_weight(row.get("Trọng số"))
    plan = parse_float(row.get("Kế hoạch"))
    actual = parse_float(row.get("Thực hiện"))
    if plan is None or actual is None:
        return None
    # Thực hiện <= KH: điểm tối đa
    if actual <= plan:
        return round(10.0 * W, 2)
    # Vượt KH: suy giảm tuyến tính tới 0 (tuỳ chỉnh nếu cần)
    # Ví dụ: vượt 50% -> 0 điểm
    over = (actual - plan) / plan
    score = max(0.0, (1 - over*2) * 10.0 * W)
    return round(score, 2)

def _score_pass_fail(row):
    W = _coerce_weight(row.get("Trọng số"))
    method = str(row.get("Phương pháp đo kết quả") or "").lower()
    # “Đạt/Không đạt”
    if "không" in method and "đạt" in method:
        # Lấy từ Ghi chú: pass=true/false hoặc dựa ACTUAL vs PLAN
        note = str(row.get("Ghi chú") or "").lower()
        flag = None
        m = re.search(r"pass\s*=\s*(true|false|1|0)", note)
        if m:
            flag = m.group(1) in ("true","1")
        else:
            plan = parse_float(row.get("Kế hoạch"))
            actual = parse_float(row.get("Thực hiện"))
            flag = (plan is not None and actual is not None and actual >= plan)
        return round((10.0 * W) if flag else 0.0, 2)
    return None

def _score_range(row, rule, overrides):
    W = _coerce_weight(row.get("Trọng số"))
    lo = parse_float(overrides.get("lo")) if overrides.get("lo") else parse_float(rule.get("lo"))
    hi = parse_float(overrides.get("hi")) if overrides.get("hi") else parse_float(rule.get("hi"))
    actual = parse_float(row.get("Thực hiện"))
    if lo is None or hi is None or actual is None:
        return None
    return round(10.0 * W if (lo <= actual <= hi) else 0.0, 2)

def _score_penalty_err(row, rule, overrides):
    """Phạt theo sai số % vượt ngưỡng, trừ step->pen mỗi 0.1% (hoặc step) – cap tối đa điểm trừ."""
    apply_weight = str(overrides.get("apply_weight", rule.get("apply_weight", "false"))).lower() in ("1","true","yes")
    thr  = parse_float(overrides.get("thr")  or rule.get("thr")  or 1.5)
    step = parse_float(overrides.get("step") or rule.get("step") or 0.1)
    pen  = parse_float(overrides.get("pen")  or rule.get("pen")  or 0.04)
    cap  = parse_float(overrides.get("cap")  or rule.get("cap")  or 3.0)

    plan = parse_float(row.get("Kế hoạch"))
    actual = parse_float(row.get("Thực hiện"))
    W = _coerce_weight(row.get("Trọng số"))

    if plan in (None,0) or actual is None:
        return None

    err_pct = abs(actual - plan) / plan * 100.0
    if err_pct <= thr:
        score = 10.0 * W if apply_weight else 0.0  # không trừ gì
        return round(score,2)

    over = err_pct - thr
    # mỗi 'step'% vượt trừ 'pen' điểm
    times = math.floor(over / step + 1e-9)
    minus = min(cap, times * pen)
    score = (10.0 * W) - (minus * (W if apply_weight else 1.0))
    # Lưu ý: nhóm này là 'chỉ trừ', nếu muốn chỉ trả "điểm trừ" thì để apply_weight=False và đọc giá trị âm
    return round(score, 2)

def _score_bonus(row, rule, overrides):
    """Cộng điểm theo bậc (tiers). Mặc định không nhân trọng số."""
    metric = (str(overrides.get("metric") or rule.get("metric") or "ratio")).lower()
    apply_weight = str(overrides.get("apply_weight", rule.get("apply_weight", "false"))).lower() in ("1","true","yes")

    pts = overrides.get("points_json") or rule.get("points_json")
    tiers = None
    if isinstance(pts, (dict, list)):
        tiers = pts
    elif isinstance(pts, str) and pts.strip():
        try:
            j = json.loads(pts)
            tiers = j.get("tiers", j)
        except Exception:
            tiers = None

    plan   = parse_float(row.get("Kế hoạch"))
    actual = parse_float(row.get("Thực hiện"))
    if metric == "ratio":
        x = (actual/plan) if (plan not in (None,0) and actual is not None) else None
    else:
        x = actual

    if x is None: return None

    point = 0.0
    if tiers:
        for t in tiers:
            ok = True
            if "gt" in t:  ok &= (x > float(t["gt"]))
            if "gte" in t: ok &= (x >= float(t["gte"]))
            if "lt" in t:  ok &= (x < float(t["lt"]))
            if "lte" in t: ok &= (x <= float(t["lte"]))
            if ok:
                point = float(t.get("point",0))
                break

    if apply_weight:
        point *= _coerce_weight(row.get("Trọng số"))
    return round(point, 2)

def _score_manual(row, overrides):
    v = parse_float(row.get("Điểm KPI"))
    if v is None:
        m = re.search(r"point\s*=\s*([0-9\.,\-]+)", str(row.get("Ghi chú") or ""))
        if m: v = parse_vn_number(m.group(1))
    return None if v is None else float(v)

def compute_score_with_method(row, registry):
    rule, overrides = find_rule_for_row(row, registry)
    if not rule: return None
    t = (rule.get("Type") or "").upper()
    if   t=="RATIO_UP":      return _score_ratio_up(row)
    elif t=="RATIO_DOWN":    return _score_ratio_down(row)
    elif t=="PASS_FAIL":     return _score_pass_fail(row)
    elif t=="RANGE":         return _score_range(row, rule, overrides)
    elif t=="PENALTY_ERR":   return _score_penalty_err(row, rule, overrides)
    elif t=="BONUS":         return _score_bonus(row, rule, overrides)
    elif t=="MANUAL":        return _score_manual(row, overrides)
    else:                    return _score_ratio_up(row)

# -------------- STATE --------------
if "df" not in st.session_state:
    st.session_state.df = pd.DataFrame()
if "sheet_url_or_id" not in st.session_state:
    st.session_state.sheet_url_or_id = ""
if "kpi_sheet_name" not in st.session_state:
    st.session_state.kpi_sheet_name = "KPI"

# -------------- SIDEBAR --------------
with st.sidebar:
    st.header("🔐 Đăng nhập")
    st.text_input("USE (vd: PCTN\\KVDHA)", key="use_username")
    st.text_input("Mật khẩu", type="password", key="use_password")
    st.divider()

    st.header("🔗 Kết nối Google Sheets")
    st.text_input("ID/URL Google Sheet", key="sheet_url_or_id", placeholder="Dán URL hoặc ID")
    st.text_input("Tên sheet KPI", key="kpi_sheet_name", value="KPI")

    colA, colB = st.columns(2)
    with colA:
        if st.button("📄 Tạo/ cập nhật RULES (mẫu)", use_container_width=True):
            try:
                sh,_ = get_sheet_and_name()
                ensure_rules_template(sh)
                st.success("Đã tạo RULES mẫu (sheet RULES).")
            except Exception as e:
                st.error(f"Lỗi tạo RULES: {e}")
    with colB:
        if st.button("🔁 Làm mới RULES", use_container_width=True):
            load_rules_registry.clear()
            st.success("Đã tải lại RULES")

# -------------- FILE CSV --------------
st.subheader("📥 Nhập CSV vào KPI")
up = st.file_uploader("Tải file CSV (mẫu: KPI_Input_template.csv)", type=["csv"], accept_multiple_files=False, label_visibility="collapsed")
if up is not None:
    try:
        df = pd.read_csv(up)
        # bảo đảm một số cột tồn tại
        for col in ["Chọn","Kế hoạch","Thực hiện","Trọng số","Điểm KPI","Tháng","Năm","Ghi chú","Phương pháp đo kết quả"]:
            if col not in df.columns:
                if col=="Chọn":
                    df[col] = False
                else:
                    df[col] = ""
        st.session_state.df = df
        st.success("Đã nạp CSV.")
    except Exception as e:
        st.error(f"Lỗi đọc CSV: {e}")

# -------------- FORM NHẬP (sticky) --------------
with st.container():
    st.markdown('<div class="sticky-box">', unsafe_allow_html=True)

    colF1, colF2, colF3, colF4 = st.columns([3,1,1,2])
    with colF1:
        kpi_name = st.text_input("Tên chỉ tiêu (KPI)", value=st.session_state.get("form_kpi_name",""))
    with colF2:
        unit_txt = st.text_input("Đơn vị tính", value=st.session_state.get("unit_txt",""))
    with colF3:
        dept_txt = st.text_input("Bộ phận/người phụ trách", value=st.session_state.get("dept_txt",""))
    with colF4:
        unit_owner = st.text_input("Tên đơn vị", value=st.session_state.get("unit_owner",""))

    colP1, colP2, colP3 = st.columns([1,1,1])
    with colP1:
        plan_txt = st.text_input("Kế hoạch", value=st.session_state.get("plan_txt","0,00"))
    with colP2:
        actual_txt = st.text_input("Thực hiện", value=st.session_state.get("actual_txt","0,00"))
    with colP3:
        weight_txt = st.text_input("Trọng số (%)", value=st.session_state.get("weight_txt","100,00"))

    method = st.text_input("Phương pháp đo kết quả (có thể ghi [CODE]...)", value=st.session_state.get("method_txt","Đạt/Không đạt"))

    colT1, colT2, colT3 = st.columns([1,1,2])
    with colT1:
        month_txt = st.text_input("Tháng", value=st.session_state.get("month_txt","7"))
    with colT2:
        year_txt = st.text_input("Năm", value=st.session_state.get("year_txt", str(datetime.now().year)))
    with colT3:
        note_txt = st.text_input("Ghi chú", value=st.session_state.get("note_txt",""))

    # tính điểm thử theo RULES
    registry = load_rules_registry(st.session_state.get("sheet_url_or_id",""))
    demo_row = {
        "Phương pháp đo kết quả": method,
        "Kế hoạch": plan_txt,
        "Thực hiện": actual_txt,
        "Trọng số": weight_txt,
        "Ghi chú": note_txt
    }
    score_preview = compute_score_with_method(demo_row, registry)
    colS1, colS2 = st.columns([1,2])
    with colS1:
        st.number_input("Điểm KPI (tự tính)", value=0.0 if (score_preview is None) else float(score_preview), step=0.01, key="calc_point", disabled=True)
    with colS2:
        st.markdown(f"<div class='total-box'>Điểm xem trước: <b>{score_preview if score_preview is not None else '—'}</b></div>", unsafe_allow_html=True)

    c1, c2, c3, c4, c5 = st.columns(5)
    with c1:
        st.write("")  # spacer
        if st.container().button("Áp dụng vào bảng CSV tạm", use_container_width=True, key="btn_apply", help="Đổ các ô trên vào dòng đã tích Chọn", type="primary"):
            if st.session_state.df.empty:
                st.warning("Chưa có bảng CSV.")
            else:
                df = st.session_state.df.copy()
                mask = df["Chọn"] == True if "Chọn" in df.columns else pd.Series([False]*len(df))
                if mask.sum()==0:
                    st.warning("Hãy tích chọn ít nhất 1 dòng trong bảng bên dưới.")
                else:
                    df.loc[mask, "Phương pháp đo kết quả"] = method
                    df.loc[mask, "Kế hoạch"] = plan_txt
                    df.loc[mask, "Thực hiện"] = actual_txt
                    df.loc[mask, "Trọng số"] = weight_txt
                    df.loc[mask, "Điểm KPI"] = score_preview if score_preview is not None else df.loc[mask, "Điểm KPI"]
                    df.loc[mask, "Tháng"] = month_txt
                    df.loc[mask, "Năm"] = year_txt
                    df.loc[mask, "Ghi chú"] = note_txt
                    st.session_state.df = df
                    st.success(f"Đã áp dụng vào {mask.sum()} dòng.")

    with c2:
        btn1 = st.container().button("💾 Ghi CSV tạm vào sheet KPI", use_container_width=True, key="btn_write", help="Ghi DataFrame hiện tại lên Sheet", type="secondary")
        st.markdown('<div class="btn-green"></div>', unsafe_allow_html=True)
    with c3:
        btn2 = st.container().button("🔄 Làm mới bảng CSV", use_container_width=True, key="btn_refresh", help="Xoá bảng hiện tại")
        st.markdown('<div class="btn-blue"></div>', unsafe_allow_html=True)
    with c4:
        btn3 = st.container().button("📤 Xuất báo cáo (Excel)", use_container_width=True, key="btn_export")
        st.markdown('<div class="btn-orange"></div>', unsafe_allow_html=True)
    with c5:
        btn4 = st.container().button("☁️ Lưu dữ liệu vào Google Drive", use_container_width=True, key="btn_drive")
        st.markdown('<div class="btn-purple"></div>', unsafe_allow_html=True)

    st.markdown('</div>', unsafe_allow_html=True)

# -------------- BẢNG CHÍNH --------------
st.subheader("📋 Bảng KPI (CSV tạm)")

if not st.session_state.df.empty:
    df = st.session_state.df.copy()

    # Hiển thị Data Editor có checkbox 'Chọn'
    if "Chọn" not in df.columns:
        df.insert(0, "Chọn", False)

    # Tính lại điểm theo RULES cho dòng không phải MANUAL (hoặc MANUAL mà không có điểm nhập)
    registry = load_rules_registry(st.session_state.get("sheet_url_or_id",""))
    scores = []
    for _, r in df.iterrows():
        s = compute_score_with_method(r, registry)
        scores.append(s)
    df["Điểm KPI"] = [s if s is not None else r for s, r in zip(scores, df.get("Điểm KPI", [None]*len(df)))]

    # Tổng điểm KPI (cộng các giá trị số)
    total_point = 0.0
    for v in df["Điểm KPI"].tolist():
        vv = parse_float(v)
        if vv is not None: total_point += vv
    st.markdown(f"<div class='total-box'>Tổng điểm KPI (tạm tính): <b>{format_vn_number(total_point,2)}</b></div>", unsafe_allow_html=True)

    edited = st.data_editor(
        df,
        use_container_width=True,
        num_rows="dynamic",
        disabled=[],
        column_config={
            "Chọn": st.column_config.CheckboxColumn("Chọn"),
        },
        height=460
    )
    st.session_state.df = edited
else:
    st.info("Chưa có dữ liệu CSV – vui lòng tải file mẫu rồi chỉnh sửa.")

# -------------- SỰ KIỆN NÚT --------------
def write_to_google_sheet(df: pd.DataFrame):
    sh, sheet_name = get_sheet_and_name()
    try:
        ws = sh.worksheet(sheet_name)
        ws.clear()
    except Exception:
        ws = sh.add_worksheet(title=sheet_name, rows=1000, cols=max(26, len(df.columns)))
    # update header + data
    ws.update([df.columns.tolist()] + df.fillna("").astype(str).values.tolist(), value_input_option="USER_ENTERED")
    return True

if 'btn_write' in st.session_state and st.session_state['btn_write']:
    if st.session_state.df.empty:
        st.warning("Không có dữ liệu để ghi.")
    else:
        try:
            ok = write_to_google_sheet(st.session_state.df.copy())
            st.success("Đã ghi dữ liệu lên Google Sheet.")
        except Exception as e:
            st.error(f"Lỗi khi ghi Sheets: {e}")

if 'btn_refresh' in st.session_state and st.session_state['btn_refresh']:
    st.session_state.df = pd.DataFrame()
    st.success("Đã làm mới bảng CSV tạm.")

if 'btn_export' in st.session_state and st.session_state['btn_export']:
    if st.session_state.df.empty:
        st.warning("Không có dữ liệu để xuất.")
    else:
        try:
            out = io.BytesIO()
            with pd.ExcelWriter(out, engine="xlsxwriter") as writer:
                st.session_state.df.to_excel(writer, sheet_name="KPI", index=False)
            st.download_button("⬇️ Tải file Excel", data=out.getvalue(), file_name=f"KPI_{datetime.now():%Y%m%d_%H%M}.xlsx", mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")
            st.success("Đã tạo file Excel.")
        except Exception as e:
            st.error(f"Lỗi xuất báo cáo: {e}")

if 'btn_drive' in st.session_state and st.session_state['btn_drive']:
    # Tùy môi trường/quyền Drive. Ở bản này, nếu đã ghi lên Sheet, coi như lưu xong.
    try:
        if st.session_state.df.empty:
            st.warning("Không có dữ liệu để lưu.")
        else:
            ok = write_to_google_sheet(st.session_state.df.copy())
            st.success("Đã lưu dữ liệu lên Google Sheets (coi như lưu Drive).")
    except Exception as e:
        st.error(f"Lỗi lưu Drive: {e}")
