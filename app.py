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
# シート名（Google Sheets）
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


# =========================
# マスターの正しい列順（重要）
# =========================
MAKER_HEADERS = ["メーカー名", "揺らぎ", "メーカーランク"]
ITEM_HEADERS = ["アイテム名", "アイテムランク", "揺らぎ"]


# =========================
# カタログ：保存用（32列）
# =========================
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

# 出力A（25列）
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

# ルールB（74列）
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
# 429対策
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


# =========================
# Google Sheets 接続
# =========================
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


def batch_update_rows(
    ss: gspread.Spreadsheet,
    ws_title: str,
    headers: List[str],
    updates: List[Tuple[int, Dict[str, str]]],
    chunk: int = 50
) -> None:
    if not updates:
        return
    for i in range(0, len(updates), chunk):
        part = updates[i:i + chunk]
        data = []
        for row_no, row_dict in part:
            row_values = [normalize_text(row_dict.get(h, "")) for h in headers]
            end_a1 = gspread.utils.rowcol_to_a1(row_no, len(headers))
            rng = f"{ws_title}!A{row_no}:{end_a1}"
            data.append({"range": rng, "values": [row_values]})
        body = {"valueInputOption": "RAW", "data": data}
        call_with_retry(lambda: ss.values_batch_update(body))
        time.sleep(0.2)


# =========================
# インポート
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
# 揺らぎ・判定
# =========================
def split_yuragi_cell(cell: str) -> List[str]:
    s = normalize_text(cell)
    if s == "":
        return []
    return [p.strip() for p in re.split(r"[,\n、]", s) if p.strip()]


def join_yuragi(existing: str, add_text: str) -> str:
    ex = split_yuragi_cell(existing)
    adds = split_yuragi_cell(add_text)
    for a in adds:
        if a not in ex:
            ex.append(a)
    return ",".join(ex)


def find_best_match_in_name(
    product_name: str,
    df: pd.DataFrame,
    name_col: str,
    rank_col: str,
    yuragi_col: str
) -> Tuple[str, str, str]:
    pn = normalize_text(product_name)
    if pn == "" or df.empty:
        return "", "", ""
    best_name, best_rank, best_hit, best_len = "", "", "", 0
    for _, r in df.iterrows():
        name = normalize_text(r.get(name_col, ""))
        rank = normalize_text(r.get(rank_col, ""))
        yuragi = normalize_text(r.get(yuragi_col, ""))
        for y in split_yuragi_cell(yuragi) or ([] if yuragi == "" else [yuragi]):
            if y and y in pn and len(y) > best_len:
                best_name, best_rank, best_hit, best_len = name, rank, y, len(y)
    return best_name, best_rank, best_hit


# =========================
# 価格ロジック（メーカー倍率×アイテム倍率）
# =========================
def get_item_buy_percent(df_item_coef: pd.DataFrame, item_rank: str) -> Optional[float]:
    if df_item_coef.empty:
        return None
    need = {"アイテムランク", "買取係数"}
    if not need.issubset(set(df_item_coef.columns)):
        return None
    sub = df_item_coef[df_item_coef["アイテムランク"].astype(str).str.strip() == str(item_rank).strip()]
    if sub.empty:
        return None
    return safe_to_number(sub.iloc[0].get("買取係数", ""))


def get_maker_percent(df_maker_coef: pd.DataFrame, maker_rank: str, item_name: str, price_rank: str) -> Optional[float]:
    if df_maker_coef.empty:
        return None
    need = {"メーカーランク", "項目", "未使用", "A", "B", "C", "D"}
    if not need.issubset(set(df_maker_coef.columns)):
        return None
    mr = str(maker_rank).strip()
    it = str(item_name).strip()

    sub = df_maker_coef[
        (df_maker_coef["メーカーランク"].astype(str).str.strip() == mr)
        & (df_maker_coef["項目"].astype(str).str.strip() == it)
    ]

    if sub.empty:
        col_item = df_maker_coef["項目"].astype(str).str.strip()
        sub = df_maker_coef[
            (df_maker_coef["メーカーランク"].astype(str).str.strip() == mr)
            & (col_item.str.contains(re.escape(it), na=False))
        ]

    if sub.empty and it in ("売価", "買取"):
        synonyms = {
            "売価": ["売価", "販売", "販売価格", "売価率", "売価(%)", "売価％"],
            "買取": ["買取", "買取価格", "買取率", "買取(%)", "買取％"],
        }[it]
        col_item = df_maker_coef["項目"].astype(str).str.strip()
        mask_syn = False
        for s in synonyms:
            mask_syn = mask_syn | col_item.str.contains(re.escape(s), na=False)
        sub = df_maker_coef[
            (df_maker_coef["メーカーランク"].astype(str).str.strip() == mr) & mask_syn
        ]

    if sub.empty:
        return None

    return safe_to_number(sub.iloc[0].get(price_rank, ""))


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


def derive_base_x_from_selected_price(
    base_option: str,
    base_price: Optional[float],
    maker_rank: str,
    df_maker_coef: pd.DataFrame,
    item_buy_percent: Optional[float],
) -> Optional[float]:
    if base_price is None or maker_rank == "" or item_buy_percent is None:
        return None
    rank, kind = base_option.split()
    if rank not in PRICE_RANKS:
        return None

    if kind == "売価":
        sell_percent = get_maker_percent(df_maker_coef, maker_rank, "売価", rank)
        if sell_percent is None or sell_percent == 0:
            return None
        return base_price / (sell_percent / 100.0)

    buy_percent = get_maker_percent(df_maker_coef, maker_rank, "買取", rank)
    if buy_percent is None or buy_percent == 0:
        return None
    return base_price / (buy_percent / 100.0) / (item_buy_percent / 100.0)


def calc_all_prices_from_base_x(
    base_x: Optional[float],
    maker_rank: str,
    df_maker_coef: pd.DataFrame,
    item_buy_percent: Optional[float],
) -> Dict[str, Dict[str, Optional[int]]]:
    out = {r: {"売価": None, "買取": None} for r in PRICE_RANKS}
    if base_x is None or maker_rank == "" or item_buy_percent is None:
        return out
    for r in PRICE_RANKS:
        sell_percent = get_maker_percent(df_maker_coef, maker_rank, "売価", r)
        buy_percent = get_maker_percent(df_maker_coef, maker_rank, "買取", r)
        if sell_percent is not None:
            # 売価も切り下げ適用
            out[r]["売価"] = floor_price_custom(base_x * (sell_percent / 100.0))
        if buy_percent is not None:
            # 買取も切り下げ適用
            out[r]["買取"] = floor_price_custom(base_x * (buy_percent / 100.0) * (item_buy_percent / 100.0))
    return out


def build_rule_row_from_editor(
    product_id: str,
    product_code: str,
    image_url: str,
    edited_df: pd.DataFrame,
    memo: str = ""
) -> Dict[str, str]:
    out = {h: "" for h in RULE_EXPORT_HEADERS_74}
    out["商品ID"] = normalize_text(product_id)
    out["商品コード"] = normalize_text(product_code)
    out["画像URL"] = normalize_text(image_url)
    out["メモ"] = memo or ""
    for r in PRICE_RANKS:
        idx = SETTING_INDEX_BY_RANK[r]
        out[f"設定.{idx}.対象グレードID"] = GRADE_ID_BY_RANK[r]
        # data_editorから値を取得
        sub = edited_df[edited_df["価格ランク"] == r].iloc[0]
        buy = safe_to_number(sub["買取"])
        sell = safe_to_number(sub["売価"])
        if buy is not None:
            out[f"設定.{idx}.買取価格モード"] = "FIXED"
            out[f"設定.{idx}.買取価格設定値"] = str(int(buy))
        if sell is not None:
            out[f"設定.{idx}.販売価格モード"] = "FIXED"
            out[f"設定.{idx}.販売価格設定値"] = str(int(sell))
    return out


# =========================
# 紐づけ修正・メンテナンス
# =========================
def move_yuragi_link(sh, sheet_name, name_col, yuragi_col, rank_col, old_name, new_name, keyword, new_rank):
    """間違った紐づけを消して、新しい紐づけをする"""
    ws = sh.worksheet(sheet_name)
    data = ws.get_all_values()
    headers = data[0]
    df = pd.DataFrame(data[1:], columns=headers)
    
    # 1. 旧マスターから間違った揺らぎを消す
    if old_name and old_name in df[name_col].values:
        idx = df[df[name_col] == old_name].index[0]
        current_y = split_yuragi_cell(df.at[idx, yuragi_col])
        if keyword in current_y:
            current_y.remove(keyword)
            ws.update_cell(idx + 2, headers.index(yuragi_col) + 1, ",".join(current_y))
            
    # 2. 新マスターへ新しい揺らぎを登録する（または更新）
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
# 起動を軽くする：シート準備をキャッシュ
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

    catalog_headers = ensure_headers_append(ws_catalog, CATALOG_STORE_HEADERS_32)
    rules_headers = ensure_headers_append(ws_rules, RULE_EXPORT_HEADERS_74)

    ensure_headers_append(ws_maker, MAKER_HEADERS)
    ensure_headers_append(ws_item, ITEM_HEADERS)

    ensure_headers_append(ws_tmp_cat, CATALOG_EXPORT_HEADERS_25)
    ensure_headers_append(ws_tmp_rules, RULE_EXPORT_HEADERS_74)
    ensure_headers_append(ws_log_cat, ["日付", "商品ID", "種別"])
    ensure_headers_append(ws_log_rules, ["日付", "商品ID", "種別"])

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
        "catalog_headers": catalog_headers,
        "rules_headers": rules_headers,
    }


@st.cache_data(ttl=120)
def load_master_tables() -> Tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    ss = open_spreadsheet()
    def ws_to_df(name: str) -> pd.DataFrame:
        ws = call_with_retry(lambda: ss.worksheet(name))
        vals = call_with_retry(lambda: ws.get_all_values())
        if not vals or len(vals) <= 1:
            return pd.DataFrame()
        return pd.DataFrame(vals[1:], columns=vals[0])
    return ws_to_df(SHEET_MAKER), ws_to_df(SHEET_ITEM), ws_to_df(SHEET_MAKER_COEF), ws_to_df(SHEET_ITEM_COEF)


def refresh_master_tables():
    st.cache_data.clear()
    st.rerun()


def reset_current_edit_state():
    for k in list(st.session_state.keys()):
        if k not in ["current_pid", "loaded"]:
            st.session_state.pop(k, None)
    st.session_state["loaded"] = False
    st.session_state["current_pid"] = ""


# =========================
# 画面維持（session_state）
# =========================
def set_current_pid(pid: str):
    st.session_state["current_pid"] = pid
    st.session_state["loaded"] = True


if "current_pid" not in st.session_state:
    st.session_state["current_pid"] = ""
if "loaded" not in st.session_state:
    st.session_state["loaded"] = False


# =========================
# UI
# =========================
st.set_page_config(page_title="工具価格更新アプリ", layout="wide")
st.title("🛠 工具価格更新アプリ")

with st.sidebar:
    if st.button("マスター再読み込み（キャッシュクリア）"):
        refresh_master_tables()

page = st.sidebar.radio("メニュー", ["インポート", "既存商品（価格決定・編集）", "出力（ダウンロード）"], index=1)

env = prepare_sheets_cached()
ss = env["ss"]
ws_catalog = env["ws_catalog"]
ws_rules = env["ws_rules"]
ws_maker = env["ws_maker"]
ws_item = env["ws_item"]
ws_tmp_cat = env["ws_tmp_cat"]
ws_tmp_rules = env["ws_tmp_rules"]
ws_log_cat = env["ws_log_cat"]
ws_log_rules = env["ws_log_rules"]
catalog_headers = env["catalog_headers"]
rules_headers = env["rules_headers"]

df_maker, df_item, df_maker_coef, df_item_coef = load_master_tables()


# =========================
# 1) インポート
# =========================
if page == "インポート":
    st.header("1) インポート")
    uploaded = st.file_uploader("カタログExcel（.xlsx）", type=["xlsx"])
    if uploaded:
        raw_df = pd.read_excel(uploaded, sheet_name="Sheet1", engine="openpyxl", dtype=str).fillna("")
        st.write(f"読み込み行数: {len(raw_df)}")
        if st.button("スプレッドシートへ取り込む", type="primary"):
            with st.spinner("取り込み中..."):
                mapped = pd.DataFrame()
                for h in CATALOG_STORE_HEADERS_32:
                    mapped[h] = raw_df[h] if h in raw_df.columns else ""
                vals = [CATALOG_STORE_HEADERS_32] + mapped.values.tolist()
                call_with_retry(lambda: ws_catalog.clear())
                call_with_retry(lambda: ws_catalog.update(values=vals, range_name="A1"))
                st.success("カタログ取り込み完了")


# =========================
# 2) 既存商品（価格決定・編集）
# =========================
elif page == "既存商品（価格決定・編集）":
    st.header("2) 既存商品（価格決定・編集）")

    with st.form("load_form", clear_on_submit=False):
        pid_input = st.text_input("商品ID（文字列）", value=st.session_state.get("current_pid", ""))
        submitted = st.form_submit_button("呼び出す")
        if submitted:
            pid_input = normalize_text(pid_input)
            if pid_input == "":
                st.warning("商品IDを入れてください。")
            else:
                set_current_pid(pid_input)

    if not st.session_state.get("loaded", False) or st.session_state.get("current_pid", "") == "":
        st.stop()

    pid = st.session_state["current_pid"]
    row_no = find_row_number_by_key(ws_catalog, "商品ID", pid)
    if not row_no:
        st.error("その商品IDは見つかりませんでした。")
        st.stop()

    row = read_row_as_dict(ws_catalog, row_no)
    product_name = normalize_text(row.get("商品名", ""))
    product_code = normalize_text(row.get("商品コード", ""))
    image_url = normalize_text(row.get("画像URL", ""))

    st.subheader("商品情報修正")
    col1, col2 = st.columns(2)
    with col1:
        edit_product_name = st.text_input("商品名（保存時に反映）", value=product_name, key="edit_product_name")
        edit_mpn = st.text_input("型番", value=normalize_text(row.get("型番(商品属性.mpn)", "")), key="edit_mpn")
    with col2:
        st.text_input("商品コード", value=product_code, disabled=True)
        if image_url: st.image(image_url, width=100)

    # 自動判定（揺らぎチェック）
    maker_n_auto, maker_r_auto, maker_hit = find_best_match_in_name(edit_product_name, df_maker, "メーカー名", "メーカーランク", "揺らぎ")
    item_n_auto, item_r_auto, item_hit = find_best_match_in_name(edit_product_name, df_item, "アイテム名", "アイテムランク", "揺らぎ")

    st.divider()
    st.subheader("メーカー・アイテム確定（修正可能）")
    
    col_m, col_i = st.columns(2)
    
    with col_m:
        st.write("#### メーカー設定")
        st.caption(f"自動判定：{maker_n_auto or 'なし'} ({maker_r_auto or '-'}) ヒット：{maker_hit or '-'}")
        m_list = [""] + df_maker["メーカー名"].unique().tolist() + ["(新規登録)"]
        selected_m = st.selectbox("メーカーを選択", options=m_list, index=m_list.index(maker_n_auto) if maker_n_auto in m_list else 0)
        
        final_m_name = selected_m
        if selected_m == "(新規登録)":
            final_m_name = st.text_input("新規メーカー名を入力")
            
        m_ranks = ["A", "B", "C", "D", "E"]
        # 多聞様要望：メーカーランクを変更できるようにする
        final_m_rank = st.selectbox("メーカーランク修正", options=m_ranks, index=m_ranks.index(maker_r_auto) if maker_r_auto in m_ranks else 2)

    with col_i:
        st.write("#### アイテム設定")
        st.caption(f"自動判定：{item_n_auto or 'なし'} ({item_r_auto or '-'}) ヒット：{item_hit or '-'}")
        i_list = [""] + df_item["アイテム名"].unique().tolist() + ["(新規登録)"]
        selected_i = st.selectbox("アイテムを選択", options=i_list, index=i_list.index(item_n_auto) if item_n_auto in i_list else 0)
        
        final_i_name = selected_i
        if selected_i == "(新規登録)":
            final_i_name = st.text_input("新規アイテム名を入力")
        
        final_i_rank = st.selectbox("アイテムランク修正", options=m_ranks, index=m_ranks.index(item_r_auto) if item_r_auto in m_ranks else 2)

    st.divider()
    st.subheader("価格・利益微調整（売価・買取ともに修正可能）")
    
    item_buy_percent = get_item_buy_percent(df_item_coef, final_i_rank)
    if item_buy_percent is not None:
        base_option = st.selectbox("計算基準", BASE_OPTIONS, index=1)
        base_price = safe_to_number(st.text_input("基準金額（円）", value=row.get("定価 (円)(商品属性.custom_list_price)", "0")))
        
        base_x = derive_base_x_from_selected_price(base_option, base_price, final_m_rank, df_maker_coef, item_buy_percent)
        prices = calc_all_prices_from_base_x(base_x, final_m_rank, df_maker_coef, item_buy_percent)
        
        # 編集用のデータフレーム構築（多聞さんがいじれるように）
        init_rows = []
        for rk in PRICE_RANKS:
            init_rows.append({"価格ランク": rk, "売価": int(prices[rk]["売価"] or 0), "買取": int(prices[rk]["買取"] or 0)})
        
        # リアルタイム微調整エディタ
        edited_df = st.data_editor(
            pd.DataFrame(init_rows),
            column_config={
                "価格ランク": st.column_config.TextColumn("価格ランク", disabled=True),
                "売価": st.column_config.NumberColumn("売価 (切下済)", min_value=0, step=10, format="%d"),
                "買取": st.column_config.NumberColumn("買取 (切下済)", min_value=0, step=10, format="%d"),
            },
            use_container_width=True, hide_index=True, key="price_editor"
        )
        
        # 値入計算の表示
        st.write("📈 **現在の利益確認**")
        p_rows = []
        for _, r_data in edited_df.iterrows():
            s, b = safe_to_number(r_data["売価"]) or 0, safe_to_number(r_data["買取"]) or 0
            profit = s - b
            rate = (profit / s * 100) if s > 0 else 0
            p_rows.append({"ランク": r_data["価格ランク"], "売価": f"¥{int(s):,}", "買取": f"¥{int(b):,}", "値入額": f"¥{int(profit):,}", "値入率": f"{rate:.1f}%"})
        st.table(pd.DataFrame(p_rows))
        
        st.divider()
        yuragi_fix = st.checkbox("マスターの紐づけ（揺らぎ）を修正する", value=False, help="間違ったメーカーからヒットしたキーワードを消し、新しいメーカーへ移します。")
        
        if st.button("⑥ 保存（T_rulesへ保存＆出力対象に追加）", type="primary"):
            with st.spinner("保存中..."):
                # 1. カタログ更新
                update_cells_by_headers(ws_catalog, row_no, {
                    "商品名": edit_product_name, "型番(商品属性.mpn)": edit_mpn, "メーカー(商品属性.manufacturer)": final_m_name
                })
                
                # 2. 多聞様要望：間違った紐づけを消して、新しい紐づけをする
                if yuragi_fix:
                    if maker_hit and maker_n_auto != final_m_name:
                        move_yuragi_link(ss, SHEET_MAKER, "メーカー名", "揺らぎ", "メーカーランク", maker_n_auto, final_m_name, maker_hit, final_m_rank)
                    if item_hit and item_n_auto != final_i_name:
                        move_yuragi_link(ss, SHEET_ITEM, "アイテム名", "揺らぎ", "アイテムランク", item_n_auto, final_i_name, item_hit, final_i_rank)

                # 3. ルール保存 (74列展開) - app20260126-01.py のロジック準拠
                rule_row = build_rule_row_from_editor(pid, product_code, image_url, edited_df, memo=f"maker={final_m_name}")
                rno = find_row_number_by_key(ws_rules, "商品ID", pid)
                if rno:
                    update_cells_by_headers(ws_rules, rno, rule_row)
                else:
                    call_with_retry(lambda: ws_rules.append_row([rule_row.get(h, "") for h in rules_headers], value_input_option="RAW"))
                
                # 4. 一時シート（tmp）更新
                tno = find_row_number_by_key(ws_tmp_cat, "商品ID", pid)
                tmp_cat_vals = {h: normalize_text(row.get(h, "")) for h in CATALOG_EXPORT_HEADERS_25}
                tmp_cat_vals.update({"商品名": edit_product_name, "型番(商品属性.mpn)": edit_mpn, "メーカー(商品属性.manufacturer)": final_m_name})
                if tno:
                    update_cells_by_headers(ws_tmp_cat, tno, tmp_cat_vals)
                else:
                    call_with_retry(lambda: ws_tmp_cat.append_row([tmp_cat_vals.get(h, "") for h in CATALOG_EXPORT_HEADERS_25], value_input_option="RAW"))
                
                trno = find_row_number_by_key(ws_tmp_rules, "商品ID", pid)
                if trno:
                    update_cells_by_headers(ws_tmp_rules, trno, rule_row)
                else:
                    call_with_retry(lambda: ws_tmp_rules.append_row([rule_row.get(h, "") for h in RULE_EXPORT_HEADERS_74], value_input_option="RAW"))

                st.success("保存完了しました。")
                refresh_master_tables()
                
    if st.button("この商品を閉じる（入力をクリア）"):
        reset_current_edit_state()
        st.rerun()


# =========================
# 3) 出力（ダウンロード） - オリジナルを完全維持
# =========================
else:
    st.header("3) 出力（ダウンロード）")
    def ws_to_df(ws: gspread.Worksheet) -> pd.DataFrame:
        vals = call_with_retry(lambda: ws.get_all_values())
        if not vals or len(vals) <= 1: return pd.DataFrame()
        return pd.DataFrame(vals[1:], columns=vals[0])

    df_t_cat = ws_to_df(ws_tmp_cat)
    df_t_rule = ws_to_df(ws_tmp_rules)

    if df_t_cat.empty and df_t_rule.empty:
        st.info("出力対象がありません。")
        st.stop()

    if not df_t_cat.empty:
        a_bytes = make_excel_bytes(df_t_cat.reindex(columns=CATALOG_EXPORT_HEADERS_25).fillna(""), "カタログデータ出力")
        st.download_button("Aをダウンロード（カタログデータ出力.xlsx）", a_bytes, "カタログデータ出力.xlsx")

    if not df_t_rule.empty:
        b_bytes = make_excel_bytes(df_t_rule.reindex(columns=RULE_EXPORT_HEADERS_74).fillna(""), "売買価格ルール設定出力")
        st.download_button("Bをダウンロード（売買価格ルール設定出力.xlsx）", b_bytes, "売買価格ルール設定出力.xlsx")

    st.divider()
    if st.button("出力完了にする（ログ登録＆一時シート削除）", type="primary"):
        today = datetime.date.today().strftime("%Y-%m-%d")
        def log_sync(ws_log, df_src):
            if df_src.empty: return
            vals = call_with_retry(lambda: ws_log.get_all_values())
            existed = set(str(r[1]).strip() for r in vals[1:] if len(r) > 1) if vals else set()
            new_rows = [[today, pid_log, "更新" if pid_log in existed else "新規"] for pid_log in df_src["商品ID"].astype(str).unique()]
            if new_rows: call_with_retry(lambda: ws_log.append_rows(new_rows))

        log_sync(ws_log_cat, df_t_cat)
        log_sync(ws_log_rules, df_t_rule)
        call_with_retry(lambda: ws_tmp_cat.batch_clear(["A2:Z"]))
        call_with_retry(lambda: ws_tmp_rules.batch_clear(["A2:ZZ"]))
        st.success("完了しました。")
        st.rerun()
