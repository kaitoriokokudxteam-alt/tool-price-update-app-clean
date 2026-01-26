import datetime
import re
import time
import random
from io import BytesIO
from typing import Dict, List, Tuple, Optional, Callable, Any

import pandas as pd
import streamlit as st
import gspread
from gspread.exceptions import APIError
from google.oauth2.service_account import Credentials


# =========================
# 1. シート名および定数定義
# =========================
SHEET_CATALOG = "T_catalog"
SHEET_MAKER = "Tメーカー"
SHEET_ITEM = "Tアイテム"
SHEET_MAKER_COEF = "メーカー倍率"
SHEET_ITEM_COEF = "アイテム倍率"
SHEET_RULES = "T_rules"  # 正本

SHEET_TMP_CATALOG = "カタログデータ出力"
SHEET_TMP_RULES = "売買価格ルール設定出力"
SHEET_LOG_CATALOG = "カタログログ"
SHEET_LOG_RULES = "価格ログ"

# マスターの列定義（app20260126-01.py準拠）
MAKER_HEADERS = ["メーカー名", "揺らぎ", "メーカーランク"]
ITEM_HEADERS = ["アイテム名", "アイテムランク", "揺らぎ"]

# カタログ保存用（32列） - app20260126-01.pyを完全維持
CATALOG_STORE_HEADERS_32 = [
    "商品ID",
    "商品コード",
    "商品代替コード",
    "ステータス",
    "ステータス名",
    "商品名",
    "カテゴリID",
    "カテゴリ名",
    "完全カテゴリID",
    "完全カテゴリ名",
    "グロスモード",
    "量り買い",
    "量り買い単位",
    "税率タイプ",
    "免税区分",
    "画像URL",
    "商品スペック(商品属性.custom_additional1)",
    "EC用商品スペック(商品属性.custom_spec)",
    "プライスカード印刷用商品名(商品属性.custom_additional2)",
    "自由項目3(商品属性.custom_additional3)",
    "ASIN(商品属性.asin)",
    "JANコード(商品属性.jan)",
    "メーカー(商品属性.manufacturer)",
    "型番(商品属性.mpn)",
    "ブランド(商品属性.brand)",
    "色(商品属性.color)",
    "定価 (円)(商品属性.custom_list_price)",
    "付属品(商品属性.custom_accessory)",
    "TAYS ID(商品属性.tays_id)",
    "商品作成日",
    "商品更新日",
    "ハッシュ",
]

# 出力A（25列） - app20260126-01.pyを完全維持
CATALOG_EXPORT_HEADERS_25 = [
    "商品ID",
    "商品コード",
    "商品代替コード",
    "ステータス",
    "商品名",
    "カテゴリID",
    "グロスモード",
    "量り買い",
    "量り買い単位",
    "税率タイプ",
    "免税区分",
    "画像URL",
    "商品スペック(商品属性.custom_additional1)",
    "EC用商品スペック(商品属性.custom_spec)",
    "プライスカード印刷用商品名(商品属性.custom_additional2)",
    "自由項目3(商品属性.custom_additional3)",
    "ASIN(商品属性.asin)",
    "JANコード(商品属性.jan)",
    "メーカー(商品属性.manufacturer)",
    "型番(商品属性.mpn)",
    "ブランド(商品属性.brand)",
    "色(商品属性.color)",
    "定価 (円)(商品属性.custom_list_price)",
    "付属品(商品属性.custom_accessory)",
    "TAYS ID(商品属性.tays_id)",
]

# ルールB（74列） - app20260126-01.pyを完全維持
RULE_EXPORT_HEADERS_74 = (
    ["商品ID", "商品コード", "画像URL", "メモ"]
    + sum(
        (
            [
                f"設定.{i}.対象グレードID",
                f"設定.{i}.買取価格モード",
                f"設定.{i}.買取価格設定値",
                f"設定.{i}.買取価格対象モール",
                f"設定.{i}.販売価格モード",
                f"設定.{i}.販売価格設定値",
                f"設定.{i}.販売価格対象モール",
            ]
            for i in range(1, 11)
        ),
        [],
    )
)

PRICE_RANKS = ["未使用", "A", "B", "C", "D"]
GRADE_ID_BY_RANK = {"未使用": "6", "A": "2", "B": "3", "C": "4", "D": "5"}
SETTING_INDEX_BY_RANK = {"未使用": 1, "A": 2, "B": 3, "C": 4, "D": 5}

BASE_OPTIONS = [
    "未使用 売価",
    "A 売価",
    "B 売価",
    "C 売価",
    "D 売価",
    "未使用 買取",
    "A 買取",
    "B 買取",
    "C 買取",
    "D 買取",
]


# =========================
# 2. ヘルパー・共通関数
# =========================
def _is_quota_error(e: Exception) -> bool:
    if not isinstance(e, APIError):
        return False
    s = str(e)
    return "[429]" in s or "Quota exceeded" in s


def call_with_retry(fn: Callable[[], Any], tries: int = 9, base_sleep: float = 1.5) -> Any:
    for i in range(tries):
        try:
            return fn()
        except Exception as e:
            if _is_quota_error(e) and i < tries - 1:
                sleep = base_sleep * (2 ** i) + random.uniform(0.0, 1.0)
                time.sleep(min(sleep, 60))
                continue
            raise


@st.cache_resource
def get_gspread_client() -> gspread.Client:
    sa_info = dict(st.secrets["gcp_service_account"])
    scopes = [
        "https://www.googleapis.com/auth/spreadsheets",
        "https://www.googleapis.com/auth/drive",
    ]
    creds = Credentials.from_service_account_info(sa_info, scopes=scopes)
    return gspread.authorize(creds)


@st.cache_resource
def open_spreadsheet() -> gspread.Spreadsheet:
    gc = get_gspread_client()
    spreadsheet_id = st.secrets["app"]["spreadsheet_id"]
    return call_with_retry(lambda: gc.open_by_key(spreadsheet_id))


def normalize_text(x) -> str:
    if pd.isna(x):
        return ""
    return str(x).strip()


def to_text_keep_zeros(x) -> str:
    if pd.isna(x):
        return ""
    s = str(x).strip()
    if re.fullmatch(r"\d+\.0", s):
        s = s[:-2]
    return s


def safe_to_number(s: Any) -> Optional[float]:
    if s is None or s == "":
        return None
    s = str(s).strip().replace(",", "").replace("¥", "")
    try:
        return float(s)
    except Exception:
        return None


def make_excel_bytes(df: pd.DataFrame, sheet_name: str) -> bytes:
    bio = BytesIO()
    with pd.ExcelWriter(bio, engine="openpyxl") as writer:
        df.to_excel(writer, index=False, sheet_name=sheet_name)
    return bio.getvalue()


def ensure_worksheet(ss: gspread.Spreadsheet, title: str, headers: List[str]) -> gspread.Worksheet:
    try:
        ws = call_with_retry(lambda: ss.worksheet(title))
    except gspread.WorksheetNotFound:
        ws = call_with_retry(lambda: ss.add_worksheet(title=title, rows=6000, cols=max(26, len(headers) + 5)))
        call_with_retry(lambda: ws.update(values=[headers], range_name="A1"))
        return ws

    header_row = call_with_retry(lambda: ws.get("1:1"))
    if not header_row or all(str(x).strip() == "" for x in header_row[0]):
        call_with_retry(lambda: ws.update(values=[headers], range_name="A1"))
    return ws


def ensure_headers_append(ws: gspread.Worksheet, desired_headers: List[str]) -> List[str]:
    header_row = call_with_retry(lambda: ws.get("1:1"))
    current = []
    if header_row and len(header_row) > 0:
        current = [str(c).strip() for c in header_row[0] if c is not None]

    if len([x for x in current if x]) == 0:
        call_with_retry(lambda: ws.update(values=[desired_headers], range_name="A1"))
        return desired_headers

    missing = [h for h in desired_headers if h not in current]
    if missing:
        new_headers = current + missing
        call_with_retry(lambda: ws.update(values=[new_headers], range_name="A1"))
        return new_headers

    return current


def get_headers(ws: gspread.Worksheet) -> List[str]:
    row = call_with_retry(lambda: ws.get("1:1"))
    if not row:
        return []
    return [str(x).strip() for x in row[0]]


def col_values_fast(ws: gspread.Worksheet, col_index_1based: int) -> List[str]:
    return call_with_retry(lambda: ws.col_values(col_index_1based))


def find_row_number_by_key(ws: gspread.Worksheet, key_col_name: str, key_value: str) -> Optional[int]:
    headers = get_headers(ws)
    if key_col_name not in headers:
        return None
    col_idx = headers.index(key_col_name) + 1
    vals = col_values_fast(ws, col_idx)
    key_value = str(key_value).strip()
    for i, v in enumerate(vals, start=1):
        if i == 1:
            continue
        if str(v).strip() == key_value:
            return i
    return None


def read_row_as_dict(ws: gspread.Worksheet, row_no: int) -> Dict[str, str]:
    headers = get_headers(ws)
    if row_no is None or row_no < 2:
        return {}
    row_vals = call_with_retry(lambda: ws.row_values(row_no))
    out = {}
    for i, h in enumerate(headers):
        out[h] = row_vals[i].strip() if i < len(row_vals) else ""
    return out


def update_cells_by_headers(ws: gspread.Worksheet, row_no: int, updates: Dict[str, str]) -> None:
    headers = get_headers(ws)
    data = []
    for k, v in updates.items():
        if k not in headers:
            continue
        col_no = headers.index(k) + 1
        a1 = gspread.utils.rowcol_to_a1(row_no, col_no)
        data.append({"range": f"{ws.title}!{a1}", "values": [[normalize_text(v)]]})
    if not data:
        return
    body = {"valueInputOption": "RAW", "data": data}
    call_with_retry(lambda: ws.spreadsheet.values_batch_update(body))
    time.sleep(0.1)


# =========================
# 3. インポート・バリデーション（完全維持）
# =========================
def load_catalog_excel(uploaded_file) -> pd.DataFrame:
    df = pd.read_excel(uploaded_file, sheet_name="Sheet1", engine="openpyxl", dtype=str)
    return df.fillna("")


def map_columns_for_store(df: pd.DataFrame) -> pd.DataFrame:
    out = pd.DataFrame()
    for h in CATALOG_STORE_HEADERS_32:
        if h in df.columns:
            if h in ["商品ID", "商品コード", "JANコード(商品属性.jan)"]:
                out[h] = df[h].apply(to_text_keep_zeros)
            else:
                out[h] = df[h].apply(normalize_text)
        else:
            out[h] = ""
    return out[CATALOG_STORE_HEADERS_32]


def validate_rows(df: pd.DataFrame) -> Tuple[pd.DataFrame, pd.DataFrame]:
    errors = []
    for i, row in df.iterrows():
        if normalize_text(row["商品ID"]) == "":
            errors.append({"行番号": int(i) + 2, "商品ID": "", "理由": "商品IDが空です"})

    pid_series = df["商品ID"].apply(normalize_text)
    dup_mask = pid_series.duplicated(keep=False) & (pid_series != "")
    if dup_mask.any():
        for i, row in df[dup_mask].iterrows():
            errors.append({"行番号": int(i) + 2, "商品ID": normalize_text(row["商品ID"]), "理由": "ファイル内で商品IDが重複しています"})

    err_df = pd.DataFrame(errors)
    if len(err_df) == 0:
        return df.copy(), err_df

    bad_rows = set(err_df["行番号"].tolist())
    ok_indices = [i for i in df.index if (i + 2) not in bad_rows]
    return df.loc[ok_indices].copy(), err_df


# =========================
# 4. 判定・紐付けメンテナンス（新規追加ロジック）
# =========================
def split_yuragi_cell(cell: str) -> List[str]:
    s = normalize_text(cell)
    if s == "":
        return []
    return [p.strip() for p in re.split(r"[,\n、]", s) if p.strip()]


def find_best_match_in_name(
    product_name: str,
    df: pd.DataFrame,
    name_col: str,
    rank_col: str,
    yuragi_col: str
) -> Tuple[str, str, str]:
    pn = normalize_text(product_name).lower()
    if pn == "" or df.empty:
        return "", "", ""
    best_name, best_rank, best_hit, best_len = "", "", "", 0
    for _, r in df.iterrows():
        name = normalize_text(r.get(name_col, ""))
        rank = normalize_text(r.get(rank_col, ""))
        yuragi = normalize_text(r.get(yuragi_col, ""))
        keywords = [name] + split_yuragi_cell(yuragi)
        for y in keywords:
            if y and y.lower() in pn and len(y) > best_len:
                best_name, best_rank, best_hit, best_len = name, rank, y, len(y)
    return best_name, best_rank, best_hit


def move_yuragi_link(sh, sheet_name, name_col, yuragi_col, rank_col, old_name, new_name, keyword, new_rank):
    """間違った紐づけを消して、新しい紐づけをする"""
    ws = sh.worksheet(sheet_name)
    data = ws.get_all_values()
    headers = data[0]
    df = pd.DataFrame(data[1:], columns=headers)
    
    # 1. 旧マスターからキーワードを削除
    if old_name and old_name in df[name_col].values:
        idx = df[df[name_col] == old_name].index[0]
        current_y = split_yuragi_cell(df.at[idx, yuragi_col])
        if keyword in current_y:
            current_y.remove(keyword)
            ws.update_cell(idx + 2, headers.index(yuragi_col) + 1, ",".join(current_y))
            
    # 2. 新マスターへ追加
    if new_name in df[name_col].values:
        idx = df[df[name_col] == new_name].index[0]
        current_y = split_yuragi_cell(df.at[idx, yuragi_col])
        if keyword not in current_y:
            current_y.append(keyword)
        ws.update_cell(idx + 2, headers.index(yuragi_col) + 1, ",".join(current_y))
        ws.update_cell(idx + 2, headers.index(rank_col) + 1, new_rank)
    else:
        # 新規登録
        new_row = [""] * len(headers)
        new_row[headers.index(name_col)] = new_name
        new_row[headers.index(yuragi_col)] = keyword
        new_row[headers.index(rank_col)] = new_rank
        ws.append_row(new_row)


# =========================
# 5. 価格計算ロジック（多聞様指定切り下げ）
# =========================
def floor_price_custom(price: Optional[float]) -> Optional[int]:
    """多聞様指定：桁数に応じた切り下げルール"""
    if price is None or price <= 0:
        return 0
    p = int(price)
    length = len(str(p))
    if length >= 5:
        return (p // 1000) * 1000  # 12345 -> 12000
    elif length == 4 or length == 3:
        return (p // 100) * 100    # 1234 -> 1200, 123 -> 100
    elif length == 2:
        return (p // 10) * 10      # 12 -> 10
    else:
        return p


def get_item_buy_percent(df_item_coef: pd.DataFrame, item_rank: str) -> Optional[float]:
    if df_item_coef.empty:
        return None
    sub = df_item_coef[df_item_coef["アイテムランク"].astype(str).str.strip() == str(item_rank).strip()]
    if sub.empty:
        return None
    return safe_to_number(sub.iloc[0].get("買取係数", ""))


def get_maker_percent(df_maker_coef: pd.DataFrame, maker_rank: str, item_name: str, price_rank: str) -> Optional[float]:
    if df_maker_coef.empty:
        return None
    mr = str(maker_rank).strip()
    it = str(item_name).strip()
    sub = df_maker_coef[
        (df_maker_coef["メーカーランク"].astype(str).str.strip() == mr)
        & (df_maker_coef["項目"].astype(str).str.contains(re.escape(it), na=False))
    ]
    if sub.empty:
        return None
    return safe_to_number(sub.iloc[0].get(price_rank, ""))


def derive_base_x(base_option, base_price, maker_rank, df_maker_coef, item_buy_percent):
    if base_price is None or maker_rank == "" or item_buy_percent is None:
        return None
    rank, kind = base_option.split()
    pct = get_maker_percent(df_maker_coef, maker_rank, "売価" if kind == "売価" else "買取", rank)
    if not pct:
        return None
    if kind == "売価":
        return base_price / (pct / 100.0)
    return base_price / (pct / 100.0) / (item_buy_percent / 100.0)


def calc_all_prices(base_x, maker_rank, df_maker_coef, item_buy_percent):
    out = {r: {"売価": None, "買取": None} for r in PRICE_RANKS}
    if base_x is None or maker_rank == "" or item_buy_percent is None:
        return out
    for r in PRICE_RANKS:
        sell_p = get_maker_percent(df_maker_coef, maker_rank, "売価", r)
        buy_p = get_maker_percent(df_maker_coef, maker_rank, "買取", r)
        if sell_p:
            out[r]["売価"] = floor_price_custom(base_x * (sell_p / 100.0))
        if buy_p:
            out[r]["買取"] = floor_price_custom(base_x * (buy_p / 100.0) * (item_buy_percent / 100.0))
    return out


def build_rule_row_from_editor(pid, code, img, edited_df, memo="") -> Dict[str, str]:
    """編集後のデータから74列データを構築（仕様：価格モード FIXED, モール空文字）"""
    row = {h: "" for h in RULE_EXPORT_HEADERS_74}
    row["商品ID"] = normalize_text(pid)
    row["商品コード"] = normalize_text(code)
    row["画像URL"] = normalize_text(img)
    row["メモ"] = memo or "手動微調整保存"
    for r in PRICE_RANKS:
        idx = SETTING_INDEX_BY_RANK[r]
        sub_list = edited_df[edited_df["価格ランク"] == r]
        if sub_list.empty:
            continue
        sub = sub_list.iloc[0]
        row[f"設定.{idx}.対象グレードID"] = GRADE_ID_BY_RANK[r]
        row[f"設定.{idx}.買取価格モード"] = "FIXED"
        row[f"設定.{idx}.買取価格対象モール"] = ""
        row[f"設定.{idx}.販売価格モード"] = "FIXED"
        row[f"設定.{idx}.販売価格対象モール"] = ""
        buy_val = safe_to_number(sub["買取"])
        sell_val = safe_to_number(sub["売価"])
        row[f"設定.{idx}.販売価格設定値"] = str(int(sell_val)) if sell_val is not None else ""
        row[f"設定.{idx}.買取価格設定値"] = str(int(buy_val)) if buy_val is not None else ""
    return row


# =========================
# 6. シート準備・キャッシュ
# =========================
@st.cache_resource
def prepare_sheets_cached() -> Dict[str, Any]:
    ss = open_spreadsheet()
    ws_catalog = ensure_worksheet(ss, SHEET_CATALOG, CATALOG_STORE_HEADERS_32)
    ws_rules = ensure_worksheet(ss, SHEET_RULES, RULE_EXPORT_HEADERS_74)
    ws_maker = ensure_worksheet(ss, SHEET_MAKER, MAKER_HEADERS)
    ws_item = ensure_worksheet(ss, SHEET_ITEM, ITEM_HEADERS)
    ws_tmp_cat = ensure_worksheet(ss, SHEET_TMP_CATALOG, CATALOG_EXPORT_HEADERS_25)
    ws_tmp_rules = ensure_worksheet(ss, SHEET_TMP_RULES, RULE_EXPORT_HEADERS_74)
    ws_log_cat = ensure_worksheet(ss, SHEET_LOG_CATALOG, ["日付", "商品ID", "種別"])
    ws_log_rules = ensure_worksheet(ss, SHEET_LOG_RULES, ["日付", "商品ID", "種別"])

    ensure_headers_append(ws_catalog, CATALOG_STORE_HEADERS_32)
    ensure_headers_append(ws_rules, RULE_EXPORT_HEADERS_74)
    ensure_headers_append(ws_maker, MAKER_HEADERS)
    ensure_headers_append(ws_item, ITEM_HEADERS)
    ensure_headers_append(ws_tmp_cat, CATALOG_EXPORT_HEADERS_25)
    ensure_headers_append(ws_tmp_rules, RULE_EXPORT_HEADERS_74)

    return {
        "ss": ss,
        "ws_catalog": ws_catalog,
        "ws_rules": ws_rules,
        "ws_maker": ws_maker,
        "ws_item": ws_item,
        "ws_tmp_cat": ws_tmp_cat,
        "ws_tmp_rules": ws_tmp_rules,
        "ws_log_cat": ws_log_cat,
        "ws_log_rules": ws_log_rules,
    }


@st.cache_data(ttl=120)
def load_master_tables() -> Tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    ss = open_spreadsheet()
    def ws_to_df(name: str) -> pd.DataFrame:
        ws = call_with_retry(lambda: ss.worksheet(name))
        vals = call_with_retry(lambda: ws.get_all_values())
        if not vals or len(vals) <= 1: return pd.DataFrame()
        return pd.DataFrame(vals[1:], columns=vals[0])
    return ws_to_df(SHEET_MAKER), ws_to_df(SHEET_ITEM), ws_to_df(SHEET_MAKER_COEF), ws_to_df(SHEET_ITEM_COEF)


def refresh_master_tables():
    st.cache_data.clear()
    st.rerun()


def reset_current_edit_state():
    """保存後に初期入力待ち状態へリセットする"""
    st.session_state["loaded"] = False
    st.session_state["current_pid"] = ""


# =========================
# 7. メインUI
# =========================
st.set_page_config(page_title="工具価格更新アプリ", layout="wide")
st.title("🛠 工具価格更新アプリ")

if "current_pid" not in st.session_state: st.session_state["current_pid"] = ""
if "loaded" not in st.session_state: st.session_state["loaded"] = False

with st.sidebar:
    if st.button("マスター再読み込み"):
        refresh_master_tables()

page = st.sidebar.radio("メニュー", ["インポート", "既存商品（価格決定・編集）", "出力（ダウンロード）"], index=1)

env = prepare_sheets_cached()
df_maker, df_item, df_maker_coef, df_item_coef = load_master_tables()

# ---------------------------------------------------------
# 1) インポート（Excel読み込みバリデーション含む - 完全維持）
# ---------------------------------------------------------
if page == "インポート":
    st.header("1) インポート")
    uploaded = st.file_uploader("カタログExcel（.xlsx）", type=["xlsx"])
    if uploaded:
        raw_df = load_catalog_excel(uploaded)
        st.write(f"読み込み行数: {len(raw_df)}")
        if st.button("スプレッドシートへ取り込む", type="primary"):
            with st.spinner("取り込み中..."):
                ok_df, err_df = validate_rows(raw_df)
                if not err_df.empty:
                    st.error("エラーがありました。")
                    st.dataframe(err_df)
                if not ok_df.empty:
                    mapped = map_columns_for_store(ok_df)
                    vals = [CATALOG_STORE_HEADERS_32] + mapped.values.tolist()
                    call_with_retry(lambda: env["ws_catalog"].clear())
                    call_with_retry(lambda: env["ws_catalog"].update(values=vals, range_name="A1"))
                    st.success(f"{len(mapped)}件のカタログを取り込みました。")

# ---------------------------------------------------------
# 2) 既存商品（価格決定・編集）
# ---------------------------------------------------------
elif page == "既存商品（価格決定・編集）":
    st.header("2) 既存商品（価格決定・編集）")

    with st.form("load_form", clear_on_submit=False):
        pid_input = st.text_input("商品IDを入力してください", value=st.session_state["current_pid"])
        if st.form_submit_button("呼び出す"):
            st.session_state["current_pid"] = pid_input.strip()
            st.session_state["loaded"] = True
            st.rerun()

    if not st.session_state.get("loaded", False) or st.session_state.get("current_pid", "") == "":
        st.stop()

    pid = st.session_state["current_pid"]
    row_no = find_row_number_by_key(env["ws_catalog"], "商品ID", pid)
    if not row_no:
        st.error("その商品IDは見ろかりませんでした。")
        st.stop()
    
    row = read_row_as_dict(env["ws_catalog"], row_no)
    product_name = normalize_text(row.get("商品名"))
    product_code = normalize_text(row.get("商品コード"))
    image_url = normalize_text(row.get("画像URL"))
    st.subheader(f"対象：{product_name}")
    
    col_img, col_txt = st.columns([1, 4])
    with col_img:
        if image_url: st.image(image_url, width=150)
    with col_txt:
        edit_pname = st.text_input("商品名（判定に影響）", value=product_name)
        edit_mpn = st.text_input("型番", value=normalize_text(row.get("型番(商品属性.mpn)")))

    # 自動判定の実行
    m_auto_n, m_auto_r, m_hit = find_best_match_in_name(edit_pname, df_maker, "メーカー名", "メーカーランク", "揺らぎ")
    i_auto_n, i_auto_r, i_hit = find_best_match_in_name(edit_pname, df_item, "アイテム名", "アイテムランク", "揺らぎ")

    st.divider()
    st.write("### メーカー・アイテム確定（修正可能）")
    cm, ci = st.columns(2)
    
    with cm:
        st.write("#### メーカー設定")
        st.caption(f"自動判定：{m_auto_n or '未検知'} ({m_auto_r or '-'})")
        m_list = [""] + df_maker["メーカー名"].unique().tolist() + ["(新規登録)"]
        default_m_idx = m_list.index(m_auto_n) if (m_auto_n in m_list) else 0
        sel_m = st.selectbox("メーカー選択", m_list, index=default_m_idx)
        fin_m_name = st.text_input("確定メーカー名", value=sel_m if sel_m != "(新規登録)" else "")
        
        m_ranks = ["A", "B", "C", "D", "E"]
        default_mr_idx = m_ranks.index(m_auto_r) if (m_auto_r in m_ranks) else 2
        fin_m_rank = st.selectbox("メーカーランク修正", m_ranks, index=default_mr_idx)
        
        yuragi_m_target = st.text_input("メーカー揺らぎ登録用KW", value=m_hit or "", help="修正可能です。")
        update_m_yuragi = st.checkbox("メーカーの紐づけ（揺らぎ）を修正登録する", value=False)

    with ci:
        st.write("#### アイテム設定")
        st.caption(f"自動判定：{i_auto_n or '未検知'} ({i_auto_r or '-'})")
        i_list = [""] + df_item["アイテム名"].unique().tolist() + ["(新規登録)"]
        default_i_idx = i_list.index(i_auto_n) if (i_auto_n in i_list) else 0
        sel_i = st.selectbox("アイテム選択", i_list, index=default_i_idx)
        fin_i_name = st.text_input("確定アイテム名", value=sel_i if sel_i != "(新規登録)" else "")
        
        default_ir_idx = m_ranks.index(i_auto_r) if (i_auto_r in m_ranks) else 2
        fin_i_rank = st.selectbox("アイテムランク修正", m_ranks, index=default_ir_idx)

        yuragi_i_target = st.text_input("アイテム揺らぎ登録用KW", value=i_hit or "")
        update_i_yuragi = st.checkbox("アイテムの紐づけ（揺らぎ）を修正登録する", value=False)

    st.divider()
    item_pct = get_item_buy_percent(df_item_coef, fin_i_rank)
    if item_pct is not None:
        st.subheader("価格決定・利益微調整")
        b_opt = st.selectbox("計算基準", BASE_OPTIONS, index=1)
        b_val = safe_to_number(st.text_input("基準金額（円）", value=row.get("定価 (円)(商品属性.custom_list_price)", "0")))
        
        bx = derive_base_x(b_opt, b_val, fin_m_rank, df_maker_coef, item_pct)
        prices = calc_all_prices(bx, fin_m_rank, df_maker_coef, item_pct)
        
        # エディタの構築
        init_rows = [{"価格ランク": r, "売価": int(prices[r]["売価"] or 0), "買取": int(prices[r]["買取"] or 0)} for r in PRICE_RANKS]
        edited_df = st.data_editor(
            pd.DataFrame(init_rows),
            column_config={
                "価格ランク": st.column_config.TextColumn("価格ランク", disabled=True),
                "売価": st.column_config.NumberColumn("売価 (切下済)", min_value=0, step=10, format="%d"),
                "買取": st.column_config.NumberColumn("買取 (切下済)", min_value=0, step=10, format="%d"),
            },
            use_container_width=True, hide_index=True, key="price_editor"
        )
        
        # 利益のリアルタイム表示
        st.write("📈 **現在の設定での利益確認**")
        p_rows = []
        for _, r_data in edited_df.iterrows():
            s, b = safe_to_number(r_data["売価"]) or 0, safe_to_number(r_data["買取"]) or 0
            profit = s - b
            rate = (profit / s * 100) if s > 0 else 0
            p_rows.append({"ランク": r_data["価格ランク"], "売価": f"¥{int(s):,}", "買取": f"¥{int(b):,}", "値入額": f"¥{int(profit):,}", "値入率": f"{rate:.1f}%"})
        st.table(pd.DataFrame(p_rows))
        
        st.divider()
        if st.button("⑥ 保存（T_rulesへ保存＆出力対象に追加）", type="primary"):
            with st.spinner("保存中..."):
                # 1. カタログ更新
                update_cells_by_headers(env["ws_catalog"], row_no, {
                    "商品名": edit_pname,
                    "型番(商品属性.mpn)": edit_mpn,
                    "メーカー(商品属性.manufacturer)": fin_m_name
                })
                
                # 2. 紐づけ修正（揺らぎメンテナンス）
                if update_m_yuragi and yuragi_m_target:
                    move_yuragi_link(env["ss"], SHEET_MAKER, "メーカー名", "揺らぎ", "メーカーランク", m_auto_n, fin_m_name, yuragi_m_target, fin_m_rank)
                if update_i_yuragi and yuragi_i_target:
                    move_yuragi_link(env["ss"], SHEET_ITEM, "アイテム名", "揺らぎ", "アイテムランク", i_auto_n, fin_i_name, yuragi_i_target, fin_i_rank)
                
                # 3. ルール保存 (74列) - メモ形式厳守
                memo_str = f"maker={fin_m_name}, item={fin_i_name}"
                rule_row = build_rule_row_from_editor(pid, product_code, image_url, edited_df, memo=memo_str)
                rno = find_row_number_by_key(env["ws_rules"], "商品ID", pid)
                if rno:
                    update_cells_by_headers(env["ws_rules"], rno, rule_row)
                else:
                    call_with_retry(lambda: env["ws_rules"].append_row([rule_row.get(h, "") for h in get_headers(env["ws_rules"])], value_input_option="RAW"))
                
                # 4. 出力用tmpシート反映
                t_cat_row = {h: normalize_text(row.get(h, "")) for h in CATALOG_EXPORT_HEADERS_25}
                t_cat_row.update({"商品名": edit_pname, "型番(商品属性.mpn)": edit_mpn, "メーカー(商品属性.manufacturer)": fin_m_name})
                tno_cat = find_row_number_by_key(env["ws_tmp_cat"], "商品ID", pid)
                if tno_cat:
                    update_cells_by_headers(env["ws_tmp_cat"], tno_cat, t_cat_row)
                else:
                    call_with_retry(lambda: env["ws_tmp_cat"].append_row([t_cat_row.get(h, "") for h in CATALOG_EXPORT_HEADERS_25], value_input_option="RAW"))
                
                tno_rule = find_row_number_by_key(env["ws_tmp_rules"], "商品ID", pid)
                if tno_rule:
                    update_cells_by_headers(env["ws_tmp_rules"], tno_rule, rule_row)
                else:
                    call_with_retry(lambda: env["ws_tmp_rules"].append_row([rule_row.get(h, "") for h in RULE_EXPORT_HEADERS_74], value_input_option="RAW"))
                
                st.success("保存完了しました。ID入力画面に戻ります。")
                refresh_master_tables()
                reset_current_edit_state()
                st.rerun()
                
    if st.button("この商品を閉じる（クリア）"):
        reset_current_edit_state()
        st.rerun()

# ---------------------------------------------------------
# 3) 出力（ダウンロード）（完全維持）
# ---------------------------------------------------------
else:
    st.header("3) 出力（ダウンロード）")
    def ws_to_df(ws: gspread.Worksheet) -> pd.DataFrame:
        vals = call_with_retry(lambda: ws.get_all_values())
        if not vals or len(vals) <= 1: return pd.DataFrame()
        return pd.DataFrame(vals[1:], columns=vals[0])

    df_tmp_cat = ws_to_df(env["ws_tmp_cat"])
    df_tmp_rules = ws_to_df(env["ws_tmp_rules"])

    if df_tmp_cat.empty and df_tmp_rules.empty:
        st.info("出力対象がありません。")
        st.stop()

    if not df_tmp_cat.empty:
        st.write(f"カタログ対象: {len(df_tmp_cat)}件")
        a_bytes = make_excel_bytes(df_tmp_cat.reindex(columns=CATALOG_EXPORT_HEADERS_25).fillna(""), "カタログデータ出力")
        st.download_button("Aをダウンロード", a_bytes, "カタログデータ出力.xlsx")

    if not df_tmp_rules.empty:
        st.write(f"ルール対象: {len(df_tmp_rules)}件")
        b_bytes = make_excel_bytes(df_tmp_rules.reindex(columns=RULE_EXPORT_HEADERS_74).fillna(""), "売買価格ルール設定出力")
        st.download_button("Bをダウンロード", b_bytes, "売買価格ルール設定出力.xlsx")

    st.divider()
    if st.button("出力完了にする（ログ登録＆一時シート削除）", type="primary"):
        with st.spinner("同期中..."):
            today = datetime.date.today().strftime("%Y-%m-%d")
            
            def get_log_set(ws: gspread.Worksheet) -> set:
                vals = call_with_retry(lambda: ws.get_all_values())
                if not vals or len(vals) <= 1: return set()
                idx = vals[0].index("商品ID") if "商品ID" in vals[0] else -1
                return set(str(r[idx]).strip() for r in vals[1:] if idx != -1 and len(r) > idx)

            existed_cat = get_log_set(env["ws_log_cat"])
            existed_rule = get_log_set(env["ws_log_rules"])

            for df, ws, existed in [(df_tmp_cat, env["ws_log_cat"], existed_cat), (df_tmp_rules, env["ws_log_rules"], existed_rule)]:
                if not df.empty and "商品ID" in df.columns:
                    log_rows = [[today, pid_log, "更新" if str(pid_log) in existed else "新規"] for pid_log in df["商品ID"].astype(str).tolist()]
                    call_with_retry(lambda: ws.append_rows(log_rows, value_input_option="RAW"))

            call_with_retry(lambda: env["ws_tmp_cat"].batch_clear(["A2:Z"]))
            call_with_retry(lambda: env["ws_tmp_rules"].batch_clear(["A2:ZZ"]))
            st.success("完了しました。")
            st.rerun()
