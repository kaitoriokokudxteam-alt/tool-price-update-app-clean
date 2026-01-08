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


def safe_to_number(s: str) -> Optional[float]:
    if s is None:
        return None
    s = str(s).strip()
    if s == "":
        return None
    s = s.replace(",", "")
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
    return [p.strip() for p in re.split(r"[,\n]", s) if p.strip()]


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

    # まずは完全一致
    sub = df_maker_coef[
        (df_maker_coef["メーカーランク"].astype(str).str.strip() == mr)
        & (df_maker_coef["項目"].astype(str).str.strip() == it)
    ]

    # 見つからない場合：表記ゆれ（例：売価(%) / 買取率 など）を吸収
    if sub.empty:
        col_item = df_maker_coef["項目"].astype(str).str.strip()
        # item_name が含まれる行（部分一致）
        sub = df_maker_coef[
            (df_maker_coef["メーカーランク"].astype(str).str.strip() == mr)
            & (col_item.str.contains(re.escape(it), na=False))
        ]

    # さらに見つからない場合：売価/買取の同義語も試す
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
            out[r]["売価"] = int(round(base_x * (sell_percent / 100.0)))
        if buy_percent is not None:
            out[r]["買取"] = int(round(base_x * (buy_percent / 100.0) * (item_buy_percent / 100.0)))
    return out


def calc_margin_table(prices: Dict[str, Dict[str, Optional[int]]]) -> pd.DataFrame:
    rows = []
    for r in PRICE_RANKS:
        sell = prices.get(r, {}).get("売価")
        buy = prices.get(r, {}).get("買取")
        if sell is None or buy is None or sell == 0:
            margin = "" if (sell is None or buy is None) else (sell - buy)
            rate = ""
        else:
            margin = sell - buy
            rate = f"{(margin / sell) * 100.0:.1f}%"
        rows.append({
            "価格ランク": r,
            "売価": "" if sell is None else sell,
            "買取": "" if buy is None else buy,
            "値入額": margin,
            "値入率": rate
        })
    return pd.DataFrame(rows)


def build_rule_row(
    product_id: str,
    product_code: str,
    image_url: str,
    prices: Dict[str, Dict[str, Optional[int]]],
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
        buy = prices.get(r, {}).get("買取")
        sell = prices.get(r, {}).get("売価")
        if buy is not None:
            out[f"設定.{idx}.買取価格モード"] = "FIXED"
            out[f"設定.{idx}.買取価格設定値"] = str(int(buy))
        if sell is not None:
            out[f"設定.{idx}.販売価格モード"] = "FIXED"
            out[f"設定.{idx}.販売価格設定値"] = str(int(sell))
    return out


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
    ws_maker = call_with_retry(lambda: ss.worksheet(SHEET_MAKER))
    ws_item = call_with_retry(lambda: ss.worksheet(SHEET_ITEM))
    ws_maker_coef = call_with_retry(lambda: ss.worksheet(SHEET_MAKER_COEF))
    ws_item_coef = call_with_retry(lambda: ss.worksheet(SHEET_ITEM_COEF))

    def ws_to_df(ws: gspread.Worksheet) -> pd.DataFrame:
        vals = call_with_retry(lambda: ws.get_all_values())
        if not vals or len(vals) <= 1:
            return pd.DataFrame()
        return pd.DataFrame(vals[1:], columns=vals[0])

    return ws_to_df(ws_maker), ws_to_df(ws_item), ws_to_df(ws_maker_coef), ws_to_df(ws_item_coef)


def refresh_master_tables():
    st.cache_data.clear()
    st.rerun()


def reset_current_edit_state():
    for k in [
        "current_pid",
        "loaded",
        "edit_product_name",
        "edit_mpn",
        "final_maker_name",
        "final_maker_rank",
        "final_item_name",
        "final_item_rank",
        "maker_add_yuragi_auto",
        "maker_add_yuragi_pick",
        "maker_pick",
        "maker_search",
        "maker_new_name",
        "maker_new_yuragi",
        "maker_new_rank",
        "item_add_yuragi_auto",
        "item_add_yuragi_pick",
        "item_pick",
        "item_search",
        "item_new_name",
        "item_new_rank",
        "item_new_yuragi",
        "base_option",
        "base_price",
        "memo",
        "edited_price_table",
        "edited_prices",
    ]:
        if k in st.session_state:
            st.session_state.pop(k, None)


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
# マスター登録ヘルパー
# =========================
def upsert_yuragi_existing(
    ws: gspread.Worksheet,
    name_col: str,
    yuragi_col: str,
    key_name: str,
    add_yuragi_text: str
) -> None:
    row_no = find_row_number_by_key(ws, name_col, key_name)
    if not row_no:
        raise ValueError("指定した名称が見つかりませんでした。")
    row = read_row_as_dict(ws, row_no)
    current = normalize_text(row.get(yuragi_col, ""))
    new_val = join_yuragi(current, add_yuragi_text)
    update_cells_by_headers(ws, row_no, {yuragi_col: new_val})


def append_new_master(ws: gspread.Worksheet, row_dict: Dict[str, str]) -> None:
    headers = get_headers(ws)
    if not headers:
        raise ValueError("マスターシートのヘッダーが空です。")
    row = [normalize_text(row_dict.get(h, "")) for h in headers]
    call_with_retry(lambda: ws.append_row(row, value_input_option="RAW"))
    time.sleep(0.2)


# =========================
# UI
# =========================
st.set_page_config(page_title="工具価格更新アプリ", layout="wide")
st.title("工具価格更新アプリ")

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
# 1) インポート（軽量）
# =========================
if page == "インポート":
    st.header("1) カタログデータ取り込み（軽量化）")
    uploaded = st.file_uploader("取り込みたいカタログExcel（.xlsx）", type=["xlsx"])
    if uploaded is None:
        st.stop()

    raw_df = load_catalog_excel(uploaded)
    mapped_df = map_columns_for_store(raw_df)
    ok_df, err_df = validate_rows(mapped_df)

    st.write(f"**OK行：{len(ok_df)} / エラー行：{len(err_df) if err_df is not None else 0}**")
    st.dataframe(ok_df.head(20), use_container_width=True)

    if err_df is not None and len(err_df) > 0:
        st.subheader("エラー一覧（この行は取り込まれません）")
        st.dataframe(err_df, use_container_width=True)

    if not st.button("取り込み実行", type="primary"):
        st.stop()

    try:
        headers = get_headers(ws_catalog)
        if "商品ID" not in headers:
            raise ValueError("T_catalog に「商品ID」列がありません。")
        pid_col = headers.index("商品ID") + 1
        pid_col_vals = col_values_fast(ws_catalog, pid_col)

        existing_map: Dict[str, int] = {}
        for row_no in range(2, len(pid_col_vals) + 1):
            pid = str(pid_col_vals[row_no - 1]).strip()
            if pid:
                existing_map[pid] = row_no

        to_append = ok_df[~ok_df["商品ID"].isin(existing_map.keys())].copy()
        to_update = ok_df[ok_df["商品ID"].isin(existing_map.keys())].copy()

        if len(to_append) > 0:
            rows = []
            for _, r in to_append.iterrows():
                row_dict = {h: normalize_text(r.get(h, "")) for h in catalog_headers}
                rows.append([row_dict.get(h, "") for h in catalog_headers])
            call_with_retry(lambda: ws_catalog.append_rows(rows, value_input_option="RAW"))
            time.sleep(0.3)

        updates = []
        for _, r in to_update.iterrows():
            pid = normalize_text(r.get("商品ID", ""))
            row_no = existing_map.get(pid)
            if not row_no:
                continue
            row_dict = {h: "" for h in catalog_headers}
            for h in CATALOG_STORE_HEADERS_32:
                row_dict[h] = normalize_text(r.get(h, ""))
            updates.append((row_no, row_dict))

        batch_update_rows(ss, ws_catalog.title, catalog_headers, updates, chunk=60)
        st.success("取り込み完了！")

    except Exception as e:
        st.error("取り込み中にエラーが出ました。")
        st.exception(e)


# =========================
# 2) 既存商品（本丸）
# =========================
elif page == "既存商品（価格決定・編集）":
    st.header("2) 既存商品（価格決定・編集）")

    with st.form("load_form", clear_on_submit=False):
        pid_input = st.text_input("商品ID（文字列）", value=st.session_state.get("current_pid", ""))
        submitted = st.form_submit_button("この商品IDを呼び出す")
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
        st.error("その商品IDは T_catalog に見つかりませんでした。")
        reset_current_edit_state()
        st.rerun()

    row = read_row_as_dict(ws_catalog, row_no)
    product_name_original = normalize_text(row.get("商品名", ""))
    product_code = normalize_text(row.get("商品コード", ""))
    image_url = normalize_text(row.get("画像URL", ""))

    display_name = product_name_original if product_name_original.startswith("★") else f"★{product_name_original}"

    st.subheader("商品情報")
    col1, col2 = st.columns(2)
    with col1:
        edit_product_name = st.text_input("商品名（保存時に反映）", value=display_name, key="edit_product_name")
        edit_mpn = st.text_input("型番", value=normalize_text(row.get("型番(商品属性.mpn)", "")), key="edit_mpn")
    with col2:
        st.text_input("商品コード", value=product_code, disabled=True)
        st.text_input("画像URL", value=image_url, disabled=True)

    maker_name_auto, maker_rank_auto, maker_hit = find_best_match_in_name(
        edit_product_name, df_maker, "メーカー名", "メーカーランク", "揺らぎ"
    )
    item_name_auto, item_rank_auto, item_hit = find_best_match_in_name(
        edit_product_name, df_item, "アイテム名", "アイテムランク", "揺らぎ"
    )

    st.divider()
    st.subheader("④ 最終確定（見つからない時は登録）")

    if "final_maker_name" not in st.session_state:
        st.session_state["final_maker_name"] = maker_name_auto
    if "final_maker_rank" not in st.session_state:
        st.session_state["final_maker_rank"] = maker_rank_auto
    if "final_item_name" not in st.session_state:
        st.session_state["final_item_name"] = item_name_auto
    if "final_item_rank" not in st.session_state:
        st.session_state["final_item_rank"] = item_rank_auto

    st.markdown("### **メーカー確定**")
    st.write(f"自動判定：**{maker_name_auto or '未判定'}** / ランク：**{maker_rank_auto or '未判定'}** / ヒット：**{maker_hit or 'なし'}**")

    maker_ok = maker_name_auto != ""
    maker_block = st.container(border=True)
    with maker_block:
        if maker_ok:
            c1, c2, c3 = st.columns([2, 2, 2])
            with c1:
                st.text_input("メーカー名（最終）", value=st.session_state["final_maker_name"], key="final_maker_name", disabled=True)
            with c2:
                st.text_input("メーカーランク（最終）", value=st.session_state["final_maker_rank"], key="final_maker_rank", disabled=True)
            with c3:
                add_y = st.text_input("揺らぎ追加（任意）", value="", key="maker_add_yuragi_auto")
                if st.button("このメーカーに揺らぎを追加"):
                    if add_y.strip() == "":
                        st.warning("揺らぎが空です。")
                    else:
                        try:
                            upsert_yuragi_existing(ws_maker, "メーカー名", "揺らぎ", st.session_state["final_maker_name"], add_y)
                            st.success("揺らぎを追加しました。")
                            refresh_master_tables()
                        except Exception as e:
                            st.error("揺らぎ追加に失敗しました。")
                            st.exception(e)
        else:
            tabs = st.tabs(["既存メーカーから選ぶ＋揺らぎ追加", "新規メーカー登録"])
            with tabs[0]:
                search = st.text_input("メーカー検索（部分一致）", value="", key="maker_search")
                df = df_maker.copy() if not df_maker.empty else pd.DataFrame(columns=MAKER_HEADERS)
                if search.strip():
                    s = search.strip()
                    df = df[df["メーカー名"].astype(str).str.contains(s, na=False)]
                names = df["メーカー名"].astype(str).tolist() if "メーカー名" in df.columns else []
                pick = st.selectbox("既存メーカーを選択", options=[""] + names, index=0, key="maker_pick")
                add_y = st.text_input("このメーカーに追加する揺らぎ（必須）", value="", key="maker_add_yuragi_pick")
                if st.button("確定（既存メーカー＋揺らぎ追加）", type="primary"):
                    if pick.strip() == "":
                        st.warning("メーカーを選んでください。")
                    elif add_y.strip() == "":
                        st.warning("揺らぎが必須です。")
                    else:
                        try:
                            upsert_yuragi_existing(ws_maker, "メーカー名", "揺らぎ", pick, add_y)
                            rank = ""
                            if not df_maker.empty:
                                sub = df_maker[df_maker["メーカー名"].astype(str).str.strip() == pick.strip()]
                                if not sub.empty:
                                    rank = normalize_text(sub.iloc[0].get("メーカーランク", ""))
                            st.session_state["final_maker_name"] = pick
                            st.session_state["final_maker_rank"] = rank
                            st.success("メーカーを確定しました。")
                            refresh_master_tables()
                        except Exception as e:
                            st.error("確定に失敗しました。")
                            st.exception(e)

            with tabs[1]:
                nm = st.text_input("新規メーカー名", value="", key="maker_new_name")
                yu = st.text_input("揺らぎ（必須・カンマ区切りOK）", value="", key="maker_new_yuragi")
                rk = st.selectbox("メーカーランク", options=["A", "B", "C", "D", "E"], index=0, key="maker_new_rank")
                if st.button("新規メーカー登録して確定", type="primary"):
                    if nm.strip() == "":
                        st.warning("新規メーカー名が空です。")
                    elif yu.strip() == "":
                        st.warning("揺らぎが必須です。")
                    else:
                        try:
                            append_new_master(ws_maker, {"メーカー名": nm, "揺らぎ": yu, "メーカーランク": rk})
                            st.session_state["final_maker_name"] = nm.strip()
                            st.session_state["final_maker_rank"] = rk.strip()
                            st.success("新規メーカーを登録して確定しました。")
                            refresh_master_tables()
                        except Exception as e:
                            st.error("新規登録に失敗しました。")
                            st.exception(e)

    st.markdown("### **アイテム確定**")
    st.write(f"自動判定：**{item_name_auto or '未判定'}** / ランク：**{item_rank_auto or '未判定'}** / ヒット：**{item_hit or 'なし'}**")

    item_ok = item_name_auto != ""
    item_block = st.container(border=True)
    with item_block:
        if item_ok:
            c1, c2, c3 = st.columns([2, 2, 2])
            with c1:
                st.text_input("アイテム名（最終）", value=st.session_state["final_item_name"], key="final_item_name", disabled=True)
            with c2:
                st.text_input("アイテムランク（最終）", value=st.session_state["final_item_rank"], key="final_item_rank", disabled=True)
            with c3:
                add_y = st.text_input("揺らぎ追加（任意）", value="", key="item_add_yuragi_auto")
                if st.button("このアイテムに揺らぎを追加"):
                    if add_y.strip() == "":
                        st.warning("揺らぎが空です。")
                    else:
                        try:
                            upsert_yuragi_existing(ws_item, "アイテム名", "揺らぎ", st.session_state["final_item_name"], add_y)
                            st.success("揺らぎを追加しました。")
                            refresh_master_tables()
                        except Exception as e:
                            st.error("揺らぎ追加に失敗しました。")
                            st.exception(e)
        else:
            tabs = st.tabs(["既存アイテムから選ぶ＋揺らぎ追加", "新規アイテム登録"])
            with tabs[0]:
                search = st.text_input("アイテム検索（部分一致）", value="", key="item_search")
                df = df_item.copy() if not df_item.empty else pd.DataFrame(columns=ITEM_HEADERS)
                if search.strip():
                    s = search.strip()
                    df = df[df["アイテム名"].astype(str).str.contains(s, na=False)]
                names = df["アイテム名"].astype(str).tolist() if "アイテム名" in df.columns else []
                pick = st.selectbox("既存アイテムを選択", options=[""] + names, index=0, key="item_pick")
                add_y = st.text_input("このアイテムに追加する揺らぎ（必須）", value="", key="item_add_yuragi_pick")
                if st.button("確定（既存アイテム＋揺らぎ追加）", type="primary"):
                    if pick.strip() == "":
                        st.warning("アイテムを選んでください。")
                    elif add_y.strip() == "":
                        st.warning("揺らぎが必須です。")
                    else:
                        try:
                            upsert_yuragi_existing(ws_item, "アイテム名", "揺らぎ", pick, add_y)
                            rank = ""
                            if not df_item.empty:
                                sub = df_item[df_item["アイテム名"].astype(str).str.strip() == pick.strip()]
                                if not sub.empty:
                                    rank = normalize_text(sub.iloc[0].get("アイテムランク", ""))
                            st.session_state["final_item_name"] = pick
                            st.session_state["final_item_rank"] = rank
                            st.success("アイテムを確定しました。")
                            refresh_master_tables()
                        except Exception as e:
                            st.error("確定に失敗しました。")
                            st.exception(e)

            with tabs[1]:
                nm = st.text_input("新規アイテム名", value="", key="item_new_name")
                rk = st.selectbox("アイテムランク", options=["A", "B", "C", "D", "E"], index=0, key="item_new_rank")
                yu = st.text_input("揺らぎ（必須・カンマ区切りOK）", value="", key="item_new_yuragi")
                if st.button("新規アイテム登録して確定", type="primary"):
                    if nm.strip() == "":
                        st.warning("新規アイテム名が空です。")
                    elif yu.strip() == "":
                        st.warning("揺らぎが必須です。")
                    else:
                        try:
                            append_new_master(ws_item, {"アイテム名": nm, "アイテムランク": rk, "揺らぎ": yu})
                            st.session_state["final_item_name"] = nm.strip()
                            st.session_state["final_item_rank"] = rk.strip()
                            st.success("新規アイテムを登録して確定しました。")
                            refresh_master_tables()
                        except Exception as e:
                            st.error("新規登録に失敗しました。")
                            st.exception(e)

    # ------------------------
    # ⑤ 基準値入力
    # ------------------------
    st.divider()
    st.subheader("⑤ 基準値入力 → 自動計算（本丸）")

    # ★表示は出ているのに⑤が止まる対策（自動判定 → 強制確定）
    if normalize_text(st.session_state.get("final_maker_name", "")) == "" and maker_name_auto:
        st.session_state["final_maker_name"] = maker_name_auto
    if normalize_text(st.session_state.get("final_maker_rank", "")) == "" and maker_rank_auto:
        st.session_state["final_maker_rank"] = maker_rank_auto
    if normalize_text(st.session_state.get("final_item_name", "")) == "" and item_name_auto:
        st.session_state["final_item_name"] = item_name_auto
    if normalize_text(st.session_state.get("final_item_rank", "")) == "" and item_rank_auto:
        st.session_state["final_item_rank"] = item_rank_auto

    # ★強制確定の後に取り直す（重要）
    final_maker_name = normalize_text(st.session_state.get("final_maker_name", ""))
    final_maker_rank = normalize_text(st.session_state.get("final_maker_rank", ""))
    final_item_name = normalize_text(st.session_state.get("final_item_name", ""))
    final_item_rank = normalize_text(st.session_state.get("final_item_rank", ""))

    if final_maker_name == "" or final_maker_rank == "" or final_item_name == "" or final_item_rank == "":
        st.warning("**メーカー/アイテムがまだ確定できていません。** 上の確定を完了してください。")
        st.stop()

    item_buy_percent = get_item_buy_percent(df_item_coef, final_item_rank)
    if item_buy_percent is None:
        st.error("アイテム倍率（買取係数）が見つかりません。アイテム倍率シートを確認してください。")
        st.stop()

    base_option = st.selectbox("基準値（10種）", BASE_OPTIONS, index=1, key="base_option")
    base_price = safe_to_number(st.text_input("基準値に入れる金額（円）", value="", key="base_price"))

    # base_price が未入力のタイミングで編集テーブルを初期化してしまうと、
    # その後に金額を入れても None のまま固定されてしまうため、
    # "基準値が確定したとき" だけ初期化／再初期化する。
    base_key = (
        normalize_text(final_maker_rank),
        normalize_text(final_item_rank),
        normalize_text(base_option),
        base_price,
    )

    base_x = derive_base_x_from_selected_price(
        base_option, base_price, final_maker_rank, df_maker_coef, item_buy_percent
    )
    prices = calc_all_prices_from_base_x(base_x, final_maker_rank, df_maker_coef, item_buy_percent)

    st.write(f"メーカー倍率：**{final_maker_rank}** / アイテム買取係数：**{item_buy_percent}%**")

        # ====== 価格表：売価/買取だけ編集 → 値入額/値入率は自動再計算して同じ表に表示 ======
    # 方針：
    # - data_editor で編集できるのは「売価」「買取」だけ（価格ランクは固定）
    # - 編集が入るたびに「値入額」「値入率」を再計算し、同じ表の列として表示する
    # - 買取は「10円単位で切り上げ（1桁目繰り上げ）」して表示・保存する

    def ceil_to_10(yen: int) -> int:
        """10円単位で切り上げ（例：4375→4380）"""
        try:
            v = int(yen)
        except Exception:
            return 0
        if v <= 0:
            return 0
        return ((v + 9) // 10) * 10

    def _to_int_or_none(x):
        s = str(x).strip()
        if s == "" or s.lower() == "none":
            return None
        try:
            return int(float(str(x).replace(",", "")))
        except Exception:
            return None

    def build_price_table_for_editor(prices_dict: dict) -> "pd.DataFrame":
        """prices(辞書) -> editor用DataFrame（計算列付き、列順固定）"""
        rows = []
        for rk in PRICE_RANKS:
            p = prices_dict.get(rk, {})
            sell = _to_int_or_none(p.get("売価"))
            buy = _to_int_or_none(p.get("買取"))
            if buy is not None:
                buy = ceil_to_10(buy)

            margin = None
            rate = None
            if sell is not None and buy is not None:
                margin = sell - buy
                if sell > 0:
                    rate = round((margin / sell) * 100, 1)

            rows.append({
                "価格ランク": rk,
                "売価": sell,
                "買取": buy,
                "値入額": margin,
                "値入率": rate,  # %の数値（例：69.4）
            })
        df = pd.DataFrame(rows)
        # 表示上の列順を固定
        df = df[["価格ランク", "売価", "買取", "値入額", "値入率"]]
        return df

    # 初回、または基準値が変わった時だけ、基準計算結果(prices)を「編集用の元データ」として保持
    # ※ base_price が None のときは初期化しない（None固定事故を防ぐ）
    if base_price is not None:
        if (
            "price_table_edit_base" not in st.session_state
            or st.session_state.get("price_table_base_key") != base_key
        ):
            st.session_state["price_table_edit_base"] = {
                rk: {"売価": prices[rk]["売価"], "買取": prices[rk]["買取"]} for rk in PRICE_RANKS
            }
            st.session_state["price_table_base_key"] = base_key
    else:
        st.info("**基準値に入れる金額（円）** を入力すると、売価/買取が自動計算されます。")
        # 表は出すが、編集元は作らない（入力後に改めて初期化される）
        if "price_table_edit_base" not in st.session_state:
            st.session_state["price_table_edit_base"] = {rk: {"売価": None, "買取": None} for rk in PRICE_RANKS}

    # editor表示用（計算列付き）
    df_prices_for_editor = build_price_table_for_editor(st.session_state["price_table_edit_base"])

    edited_df = st.data_editor(
        df_prices_for_editor,
        use_container_width=True,
        hide_index=True,
        key="edited_price_table",
        column_config={
            "価格ランク": st.column_config.TextColumn("価格ランク"),
            "売価": st.column_config.NumberColumn("売価", step=10, format="%d"),
            "買取": st.column_config.NumberColumn("買取", step=10, format="%d"),
            "値入額": st.column_config.NumberColumn("値入額", format="%d"),
            "値入率": st.column_config.NumberColumn("値入率", format="%.1f"),
        },
        disabled=["価格ランク", "値入額", "値入率"],
    )

    # ここで「売価/買取」の編集結果を session_state に反映
    # → 値入額/値入率は次のrerunで再計算され、同じ表に即反映される
    edited_prices_base = st.session_state["price_table_edit_base"]
    changed = False

    for _, rr in edited_df.iterrows():
        rk = str(rr.get("価格ランク", "")).strip()
        if rk not in edited_prices_base:
            continue

        new_sell = _to_int_or_none(rr.get("売価"))
        new_buy = _to_int_or_none(rr.get("買取"))
        if new_buy is not None:
            new_buy = ceil_to_10(new_buy)

        old_sell = _to_int_or_none(edited_prices_base[rk].get("売価"))
        old_buy = _to_int_or_none(edited_prices_base[rk].get("買取"))
        if old_buy is not None:
            old_buy = ceil_to_10(old_buy)

        if new_sell != old_sell or new_buy != old_buy:
            edited_prices_base[rk]["売価"] = new_sell
            edited_prices_base[rk]["買取"] = new_buy
            changed = True

    # ⑥保存が編集後を使えるように session_state["edited_prices"] を作る（従来の辞書形式）
    edited_prices = {}
    for rk in PRICE_RANKS:
        p = edited_prices_base.get(rk, {})
        edited_prices[rk] = {"売価": _to_int_or_none(p.get("売価")), "買取": _to_int_or_none(p.get("買取"))}
        if edited_prices[rk]["買取"] is not None:
            edited_prices[rk]["買取"] = ceil_to_10(edited_prices[rk]["買取"])

    st.session_state["edited_prices"] = edited_prices

    # 変更があった場合、即 rerun して計算列を最新化（同じ表に反映）
    if changed:
        st.rerun()

    memo = st.text_input("メモ（任意）", value=f"maker={final_maker_name}, item={final_item_name}", key="memo")

    st.divider()
    colA, colB = st.columns([1, 1])

    with colA:
        if st.button("⑥ 保存（T_rulesへ保存＆出力対象に追加）", type="primary"):
            save_prices = st.session_state.get("edited_prices", prices)

            any_price = any(
                (save_prices[r]["売価"] is not None or save_prices[r]["買取"] is not None)
                for r in PRICE_RANKS
            )
            if not any_price:
                st.error("基準値の金額が未入力、または倍率が不足していて計算できません。")
                st.stop()

            # カタログ側は編集内容を反映
            update_cells_by_headers(ws_catalog, row_no, {
                "商品名": edit_product_name,
                "型番(商品属性.mpn)": edit_mpn,
                "メーカー(商品属性.manufacturer)": final_maker_name,
            })

            # ★ T_rules：メモあり（残してOK）
            rule_row = build_rule_row(pid, product_code, image_url, save_prices, memo=memo)

            rno = find_row_number_by_key(ws_rules, "商品ID", pid)
            if rno:
                full = {h: rule_row.get(h, "") for h in rules_headers}
                batch_update_rows(ss, ws_rules.title, rules_headers, [(rno, full)], chunk=1)
            else:
                call_with_retry(lambda: ws_rules.append_row(
                    [rule_row.get(h, "") for h in rules_headers],
                    value_input_option="RAW"
                ))

            # ★ 一時出力B：メモは必ず空
            tmp_rule_row = dict(rule_row)
            tmp_rule_row["メモ"] = ""

            # 一時出力A（カタログデータ出力）更新/追加
            tno = find_row_number_by_key(ws_tmp_cat, "商品ID", pid)
            tmp_cat_row = {h: "" for h in CATALOG_EXPORT_HEADERS_25}
            for h in CATALOG_EXPORT_HEADERS_25:
                if h == "商品名":
                    tmp_cat_row[h] = edit_product_name
                elif h == "型番(商品属性.mpn)":
                    tmp_cat_row[h] = edit_mpn
                elif h == "メーカー(商品属性.manufacturer)":
                    tmp_cat_row[h] = final_maker_name
                else:
                    tmp_cat_row[h] = normalize_text(row.get(h, ""))

            if tno:
                full = {h: tmp_cat_row.get(h, "") for h in CATALOG_EXPORT_HEADERS_25}
                batch_update_rows(ss, ws_tmp_cat.title, CATALOG_EXPORT_HEADERS_25, [(tno, full)], chunk=1)
            else:
                call_with_retry(lambda: ws_tmp_cat.append_row(
                    [tmp_cat_row.get(h, "") for h in CATALOG_EXPORT_HEADERS_25],
                    value_input_option="RAW"
                ))

            # 一時出力B（売買価格ルール設定出力）更新/追加
            trno = find_row_number_by_key(ws_tmp_rules, "商品ID", pid)
            if trno:
                full = {h: tmp_rule_row.get(h, "") for h in RULE_EXPORT_HEADERS_74}
                batch_update_rows(ss, ws_tmp_rules.title, RULE_EXPORT_HEADERS_74, [(trno, full)], chunk=1)
            else:
                call_with_retry(lambda: ws_tmp_rules.append_row(
                    [tmp_rule_row.get(h, "") for h in RULE_EXPORT_HEADERS_74],
                    value_input_option="RAW"
                ))

            st.success("保存しました。次の商品へ進みます。")
            reset_current_edit_state()
            st.rerun()

    with colB:
        if st.button("この商品を閉じる（入力をクリア）"):
            reset_current_edit_state()
            st.rerun()


# =========================
# 3) 出力（tmpだけ）
# =========================
else:
    st.header("3) 出力（ダウンロード）")

    def ws_to_df(ws: gspread.Worksheet) -> pd.DataFrame:
        vals = call_with_retry(lambda: ws.get_all_values())
        if not vals or len(vals) <= 1:
            return pd.DataFrame()
        return pd.DataFrame(vals[1:], columns=vals[0])

    df_tmp_cat = ws_to_df(ws_tmp_cat)
    df_tmp_rules = ws_to_df(ws_tmp_rules)

    if df_tmp_cat.empty and df_tmp_rules.empty:
        st.info("出力対象がありません。")
        st.stop()

    if not df_tmp_cat.empty:
        for h in CATALOG_EXPORT_HEADERS_25:
            if h not in df_tmp_cat.columns:
                df_tmp_cat[h] = ""
        a_bytes = make_excel_bytes(df_tmp_cat[CATALOG_EXPORT_HEADERS_25].copy(), "カタログデータ出力")
        st.download_button(
            "Aをダウンロード（カタログデータ出力.xlsx）",
            a_bytes,
            "カタログデータ出力.xlsx",
            "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        )

    if not df_tmp_rules.empty:
        for h in RULE_EXPORT_HEADERS_74:
            if h not in df_tmp_rules.columns:
                df_tmp_rules[h] = ""
        b_bytes = make_excel_bytes(df_tmp_rules[RULE_EXPORT_HEADERS_74].copy(), "売買価格ルール設定出力")
        st.download_button(
            "Bをダウンロード（売買価格ルール設定出力.xlsx）",
            b_bytes,
            "売買価格ルール設定出力.xlsx",
            "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        )

    st.divider()
    st.subheader("出力完了（ログ登録→一時シートをクリア）")

    if st.button("出力完了にする（ログ登録＆一時シート削除）", type="primary"):
        today = datetime.date.today().strftime("%Y-%m-%d")

        def log_ws_to_set(ws: gspread.Worksheet) -> set:
            vals = call_with_retry(lambda: ws.get_all_values())
            if not vals or len(vals) <= 1:
                return set()
            headers = vals[0]
            if "商品ID" not in headers:
                return set()
            idx = headers.index("商品ID")
            return set(str(r[idx]).strip() for r in vals[1:] if idx < len(r) and str(r[idx]).strip())

        existed_cat = log_ws_to_set(ws_log_cat)
        existed_rule = log_ws_to_set(ws_log_rules)

        if not df_tmp_cat.empty and "商品ID" in df_tmp_cat.columns:
            rows = []
            for pid in df_tmp_cat["商品ID"].astype(str).tolist():
                kind = "更新" if pid in existed_cat else "新規"
                rows.append([today, pid, kind])
            call_with_retry(lambda: ws_log_cat.append_rows(rows, value_input_option="RAW"))

        if not df_tmp_rules.empty and "商品ID" in df_tmp_rules.columns:
            rows = []
            for pid in df_tmp_rules["商品ID"].astype(str).tolist():
                kind = "更新" if pid in existed_rule else "新規"
                rows.append([today, pid, kind])
            call_with_retry(lambda: ws_log_rules.append_rows(rows, value_input_option="RAW"))

        call_with_retry(lambda: ws_tmp_cat.batch_clear(["A2:Z"]))
        call_with_retry(lambda: ws_tmp_rules.batch_clear(["A2:ZZ"]))

        st.success("ログ登録＆一時シートをクリアしました。")
        st.rerun()
