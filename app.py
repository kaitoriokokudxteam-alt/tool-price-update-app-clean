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


# ==========================================
# シート名および定数定義
# ==========================================
SHEET_CATALOG = "T_catalog"
SHEET_MAKER = "Tメーカー"
SHEET_ITEM = "Tアイテム"
SHEET_MAKER_COEF = "メーカー倍率"
SHEET_ITEM_COEF = "アイテム倍率"
SHEET_RULES = "T_rules"

SHEET_TMP_CATALOG = "カタログデータ出力"
SHEET_TMP_RULES = "売買価格ルール設定出力"
SHEET_LOG_CATALOG = "カタログログ"
SHEET_LOG_RULES = "価格ログ"

# カタログ保存用（32列）
CATALOG_STORE_HEADERS_32 = [
    "商品ID", "商品コード", "商品代替コード", "ステータス", "ステータス名", "商品名",
    "カテゴリID", "カテゴリ名", "完全カテゴリID", "完全カテゴリ名", "グロスモード",
    "量り買い", "量り買い単位", "税率タイプ", "免税区分", "画像URL",
    "商品スペック(商品属性.custom_additional1)", "EC用商品スペック(商品属性.custom_spec)",
    "プライスカード印刷用商品名(商品属性.custom_additional2)", "自由項目3(商品属性.custom_additional3)",
    "ASIN(商品属性.asin)", "JANコード(商品属性.jan)", "メーカー(商品属性.manufacturer)",
    "型番(商品属性.mpn)", "ブランド(商品属性.brand)", "色(商品属性.color)",
    "定価 (円)(商品属性.custom_list_price)", "付属品(商品属性.custom_accessory)",
    "TAYS ID(商品属性.tays_id)", "商品作成日", "商品更新日", "ハッシュ",
]

# カタログ出力用（25列）
CATALOG_EXPORT_HEADERS_25 = [
    "商品ID", "商品コード", "商品代替コード", "ステータス", "商品名", "カテゴリID",
    "グロスモード", "量り買い", "量り買い単位", "税率タイプ", "免税区分", "画像URL",
    "商品スペック(商品属性.custom_additional1)", "EC用商品スペック(商品属性.custom_spec)",
    "プライスカード印刷用商品名(商品属性.custom_additional2)", "自由項目3(商品属性.custom_additional3)",
    "ASIN(商品属性.asin)", "JANコード(商品属性.jan)", "メーカー(商品属性.manufacturer)",
    "型番(商品属性.mpn)", "ブランド(商品属性.brand)", "色(商品属性.color)",
    "定価 (円)(商品属性.custom_list_price)", "付属品(商品属性.custom_accessory)",
    "TAYS ID(商品属性.tays_id)",
]

# 売買価格ルール用（74列）
RULE_EXPORT_HEADERS_74 = (
    ["商品ID", "商品コード", "画像URL", "メモ"]
    + sum(([f"設定.{i}.対象グレードID", f"設定.{i}.買取価格モード", f"設定.{i}.買取価格設定値", f"設定.{i}.買取価格対象モール", f"設定.{i}.販売価格モード", f"設定.{i}.販売価格設定値", f"設定.{i}.販売価格対象モール"] for i in range(1, 11)), [])
)

PRICE_RANKS = ["未使用", "A", "B", "C", "D"]
GRADE_ID_BY_RANK = {"未使用": "6", "A": "2", "B": "3", "C": "4", "D": "5"}
SETTING_INDEX_BY_RANK = {"未使用": 1, "A": 2, "B": 3, "C": 4, "D": 5}

BASE_OPTIONS = [
    "未使用 売価", "A 売価", "B 売価", "C 売価", "D 売価",
    "未使用 買取", "A 買取", "B 買取", "C 買取", "D 買取",
]

# ==========================================
# ロジック：切り下げ関数（多聞様指定ルール）
# ==========================================
def floor_price_custom(price: float) -> int:
    if price <= 0:
        return 0
    p = int(price)
    length = len(str(p))
    if length >= 5:
        return (p // 1000) * 1000  # 12345 -> 12000
    elif length >= 3:
        return (p // 100) * 100    # 1234 -> 1200, 123 -> 100
    elif length == 2:
        return (p // 10) * 10      # 12 -> 10
    else:
        return p                   # 1 -> 1

# ==========================================
# Google Sheets 接続およびリトライ処理
# ==========================================
def _is_quota_error(e: Exception) -> bool:
    if not isinstance(e, APIError): return False
    return "[429]" in str(e) or "Quota exceeded" in str(e)

def call_with_retry(fn: Callable[[], Any], tries: int = 9, base_sleep: float = 1.5) -> Any:
    for i in range(tries):
        try:
            return fn()
        except Exception as e:
            if _is_quota_error(e) and i < tries - 1:
                time.sleep(base_sleep * (2 ** i) + random.uniform(0.0, 1.0))
                continue
            raise

@st.cache_resource
def get_gspread_client() -> gspread.Client:
    sa_info = dict(st.secrets["gcp_service_account"])
    scopes = ["https://www.googleapis.com/auth/spreadsheets", "https://www.googleapis.com/auth/drive"]
    creds = Credentials.from_service_account_info(sa_info, scopes=scopes)
    return gspread.authorize(creds)

@st.cache_resource
def open_spreadsheet() -> gspread.Spreadsheet:
    gc = get_gspread_client()
    spreadsheet_id = st.secrets["app"]["spreadsheet_id"]
    return call_with_retry(lambda: gc.open_by_key(spreadsheet_id))

# ==========================================
# 共通ユーティリティ
# ==========================================
def normalize_text(x) -> str:
    return str(x).strip() if pd.notna(x) else ""

def safe_to_number(s: str) -> Optional[float]:
    if not s: return None
    try:
        return float(str(s).replace(",", "").replace("¥", ""))
    except:
        return None

def ensure_worksheet(ss: gspread.Spreadsheet, title: str, headers: List[str]) -> gspread.Worksheet:
    try:
        ws = call_with_retry(lambda: ss.worksheet(title))
    except gspread.WorksheetNotFound:
        ws = call_with_retry(lambda: ss.add_worksheet(title=title, rows=6000, cols=max(26, len(headers) + 5)))
        call_with_retry(lambda: ws.update(values=[headers], range_name="A1"))
    return ws

def get_headers(ws: gspread.Worksheet) -> List[str]:
    row = call_with_retry(lambda: ws.get("1:1"))
    return [str(x).strip() for x in row[0]] if row else []

def find_row_number_by_key(ws: gspread.Worksheet, key_col_name: str, key_value: str) -> Optional[int]:
    headers = get_headers(ws)
    if key_col_name not in headers: return None
    col_idx = headers.index(key_col_name) + 1
    vals = call_with_retry(lambda: ws.col_values(col_idx))
    val_s = str(key_value).strip()
    for i, v in enumerate(vals, start=1):
        if i > 1 and str(v).strip() == val_s:
            return i
    return None

def read_row_as_dict(ws: gspread.Worksheet, row_no: int) -> Dict[str, str]:
    headers = get_headers(ws)
    row_vals = call_with_retry(lambda: ws.row_values(row_no))
    return {h: (row_vals[i].strip() if i < len(row_vals) else "") for i, h in enumerate(headers)}

def update_cells_by_headers(ws: gspread.Worksheet, row_no: int, updates: Dict[str, str]) -> None:
    headers = get_headers(ws)
    data = []
    for k, v in updates.items():
        if k in headers:
            a1 = gspread.utils.rowcol_to_a1(row_no, headers.index(k) + 1)
            data.append({"range": f"{ws.title}!{a1}", "values": [[normalize_text(v)]]})
    if data:
        call_with_retry(lambda: ws.spreadsheet.values_batch_update({"valueInputOption": "RAW", "data": data}))

# ==========================================
# マスター判定・メンテナンスロジック
# ==========================================
def split_yuragi_cell(cell: str) -> List[str]:
    s = normalize_text(cell)
    return [p.strip() for p in re.split(r"[,\n、]", s) if p.strip()]

def find_best_match_in_name(product_name: str, df: pd.DataFrame, name_col: str, rank_col: str, yuragi_col: str) -> Tuple[str, str, str]:
    pn = normalize_text(product_name).lower()
    if not pn or df.empty: return "", "", ""
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

def move_yuragi_keyword(sh, sheet_name, name_col, yuragi_col, rank_col, old_name, new_name, keyword, new_rank):
    """揺らぎの付け替えロジック"""
    ws = sh.worksheet(sheet_name)
    data = ws.get_all_values()
    headers = data[0]
    df = pd.DataFrame(data[1:], columns=headers)
    
    # 1. 旧マスターからキーワードを削除
    if old_name and old_name in df[name_col].values:
        idx = df[df[name_col] == old_name].index[0]
        current_yuragi = split_yuragi_cell(df.at[idx, yuragi_col])
        if keyword in current_yuragi:
            current_yuragi.remove(keyword)
            ws.update_cell(idx + 2, headers.index(yuragi_col) + 1, ",".join(current_yuragi))
            
    # 2. 新マスターへ追加または更新
    if new_name in df[name_col].values:
        idx = df[df[name_col] == new_name].index[0]
        current_yuragi = split_yuragi_cell(df.at[idx, yuragi_col])
        if keyword not in current_yuragi:
            current_yuragi.append(keyword)
        ws.update_cell(idx + 2, headers.index(yuragi_col) + 1, ",".join(current_yuragi))
        ws.update_cell(idx + 2, headers.index(rank_col) + 1, new_rank)
    else:
        # 新規メーカーとして追加
        new_row = [""] * len(headers)
        new_row[headers.index(name_col)] = new_name
        new_row[headers.index(yuragi_col)] = keyword
        new_row[headers.index(rank_col)] = new_rank
        ws.append_row(new_row)

# ==========================================
# 価格計算ロジック
# ==========================================
def get_item_buy_percent(df_item_coef, item_rank):
    if df_item_coef.empty: return None
    sub = df_item_coef[df_item_coef["アイテムランク"].astype(str).str.strip() == str(item_rank).strip()]
    return safe_to_number(sub.iloc[0].get("買取係数", "")) if not sub.empty else None

def get_maker_percent(df_maker_coef, maker_rank, item_name, price_rank):
    if df_maker_coef.empty: return None
    mr = str(maker_rank).strip()
    it = str(item_name).strip()
    sub = df_maker_coef[(df_maker_coef["メーカーランク"].astype(str).str.strip() == mr) & (df_maker_coef["項目"].astype(str).str.contains(re.escape(it), na=False))]
    return safe_to_number(sub.iloc[0].get(price_rank, "")) if not sub.empty else None

def derive_base_x(base_option, base_price, maker_rank, df_maker_coef, item_buy_percent):
    if base_price is None or not maker_rank or item_buy_percent is None: return None
    rank, kind = base_option.split()
    pct = get_maker_percent(df_maker_coef, maker_rank, "売価" if kind == "売価" else "買取", rank)
    if not pct: return None
    if kind == "売価": return base_price / (pct / 100.0)
    return base_price / (pct / 100.0) / (item_buy_percent / 100.0)

def calc_all_prices(base_x, maker_rank, df_maker_coef, item_buy_percent):
    out = {r: {"売価": None, "買取": None} for r in PRICE_RANKS}
    if base_x is None: return out
    for r in PRICE_RANKS:
        s_pct = get_maker_percent(df_maker_coef, maker_rank, "売価", r)
        b_pct = get_maker_percent(df_maker_coef, maker_rank, "買取", r)
        if s_pct: out[r]["売価"] = int(round(base_x * (s_pct / 100.0)))
        if b_pct: out[r]["買取"] = int(round(base_x * (b_pct / 100.0) * (item_buy_percent / 100.0)))
    return out

def build_rule_row(pid, code, img, prices_dict) -> Dict[str, str]:
    """T_rules用の74列データを構築"""
    row = {"商品ID": pid, "商品コード": code, "画像URL": img, "メモ": "価格更新アプリにて自動計算"}
    for rk in PRICE_RANKS:
        idx = SETTING_INDEX_BY_RANK[rk]
        row[f"設定.{idx}.対象グレードID"] = GRADE_ID_BY_RANK[rk]
        row[f"設定.{idx}.買取価格モード"] = "2"
        row[f"設定.{idx}.買取価格対象モール"] = "0"
        row[f"設定.{idx}.販売価格モード"] = "2"
        row[f"設定.{idx}.販売価格対象モール"] = "0"
        v_s = prices_dict[rk]["売価"]
        v_b = prices_dict[rk]["買取"]
        row[f"設定.{idx}.販売価格設定値"] = str(v_s) if v_s else ""
        row[f"設定.{idx}.買取価格設定値"] = str(floor_price_custom(v_b)) if v_b else ""
    return row

# ==========================================
# 画面構成
# ==========================================
@st.cache_resource
def prepare_sheets_cached():
    ss = open_spreadsheet()
    return {
        "ss": ss,
        "ws_catalog": ensure_worksheet(ss, SHEET_CATALOG, CATALOG_STORE_HEADERS_32),
        "ws_rules": ensure_worksheet(ss, SHEET_RULES, RULE_EXPORT_HEADERS_74),
        "ws_maker": ensure_worksheet(ss, SHEET_MAKER, MAKER_HEADERS),
        "ws_item": ensure_worksheet(ss, SHEET_ITEM, ITEM_HEADERS),
        "ws_tmp_cat": ensure_worksheet(ss, SHEET_TMP_CATALOG, CATALOG_EXPORT_HEADERS_25),
        "ws_tmp_rules": ensure_worksheet(ss, SHEET_TMP_RULES, RULE_EXPORT_HEADERS_74),
        "ws_log_cat": ensure_worksheet(ss, SHEET_LOG_CATALOG, ["日付", "商品ID", "種別"]),
        "ws_log_rules": ensure_worksheet(ss, SHEET_LOG_RULES, ["日付", "商品ID", "種別"]),
    }

@st.cache_data(ttl=120)
def load_master_tables():
    ss = open_spreadsheet()
    def ws_to_df(name):
        vals = call_with_retry(lambda: ss.worksheet(name).get_all_values())
        return pd.DataFrame(vals[1:], columns=vals[0]) if len(vals) > 1 else pd.DataFrame()
    return ws_to_df(SHEET_MAKER), ws_to_df(SHEET_ITEM), ws_to_df(SHEET_MAKER_COEF), ws_to_df(SHEET_ITEM_COEF)

if "current_pid" not in st.session_state: st.session_state["current_pid"] = ""
if "loaded" not in st.session_state: st.session_state["loaded"] = False

st.set_page_config(page_title="工具価格更新アプリ", layout="wide")
st.title("🛠 工具価格更新アプリ")

page = st.sidebar.radio("メニュー", ["1) インポート", "2) 既存商品（価格決定・編集）", "3) 出力（ダウンロード）"], index=1)
env = prepare_sheets_cached()
df_maker, df_item, df_maker_coef, df_item_coef = load_master_tables()

# ---------------------------------------------------------
# Page 1: インポート
# ---------------------------------------------------------
if page == "1) インポート":
    st.header("1) インポート")
    up_cat = st.file_uploader("商品カタログCSV", type=["csv"])
    up_rule = st.file_uploader("売買価格ルール設定CSV", type=["csv"])
    if st.button("スプレッドシートへ取り込む"):
        if up_cat:
            df = pd.read_csv(up_cat, dtype=str).reindex(columns=CATALOG_STORE_HEADERS_32).fillna("")
            call_with_retry(lambda: env["ws_catalog"].clear())
            call_with_retry(lambda: env["ws_catalog"].update(values=[CATALOG_STORE_HEADERS_32] + df.values.tolist(), range_name="A1"))
            st.success("カタログ取り込み完了")
        if up_rule:
            df = pd.read_csv(up_rule, dtype=str).reindex(columns=RULE_EXPORT_HEADERS_74).fillna("")
            call_with_retry(lambda: env["ws_rules"].clear())
            call_with_retry(lambda: env["ws_rules"].update(values=[RULE_EXPORT_HEADERS_74] + df.values.tolist(), range_name="A1"))
            st.success("ルール取り込み完了")

# ---------------------------------------------------------
# Page 2: 既存商品（価格決定・編集）
# ---------------------------------------------------------
elif page == "2) 既存商品（価格決定・編集）":
    st.header("2) 既存商品（価格決定・編集）")
    with st.form("load_form"):
        pid_input = st.text_input("商品IDまたは型番", value=st.session_state["current_pid"])
        if st.form_submit_button("呼び出す"):
            st.session_state["current_pid"] = pid_input.strip()
            st.session_state["loaded"] = True
            st.rerun()

    if st.session_state["loaded"] and st.session_state["current_pid"]:
        pid = st.session_state["current_pid"]
        row_no = find_row_number_by_key(env["ws_catalog"], "商品ID", pid)
        if not row_no:
            st.error("商品が見つかりません。")
            st.stop()
        row = read_row_as_dict(env["ws_catalog"], row_no)
        
        c1, c2 = st.columns(2)
        with c1:
            edit_name = st.text_input("商品名", value=row.get("商品名", ""))
            edit_mpn = st.text_input("型番", value=row.get("型番(商品属性.mpn)", ""))
        with c2:
            st.text_input("JANコード", value=row.get("JANコード(商品属性.jan)", ""), disabled=True)
            if row.get("画像URL"): st.image(row["画像URL"], width=150)

        # 自動判定ロジック
        ma_n, ma_r, ma_h = find_best_match_in_name(edit_name, df_maker, "メーカー名", "メーカーランク", "揺らぎ")
        ia_n, ia_r, ia_h = find_best_match_in_name(edit_name, df_item, "アイテム名", "アイテムランク", "揺らぎ")

        st.divider()
        col_m, col_i = st.columns(2)
        with col_m:
            st.subheader("メーカー設定")
            st.caption(f"自動判定: {ma_n or 'なし'} ({ma_r or '-'})")
            m_list = [""] + df_maker["メーカー名"].unique().tolist() + ["(新規登録)"]
            sel_m = st.selectbox("メーカー選択", options=m_list, index=m_list.index(ma_n) if ma_n in m_list else 0)
            fin_m_name = st.text_input("メーカー名（確定）", value=sel_m if sel_m != "(新規登録)" else "")
            m_ranks = ["A", "B", "C", "D", "E"]
            fin_m_rank = st.selectbox("メーカーランク修正", options=m_ranks, index=m_ranks.index(ma_r) if ma_r in m_ranks else 2)
        with col_i:
            st.subheader("アイテム設定")
            st.caption(f"自動判定: {ia_n or 'なし'} ({ia_r or '-'})")
            i_list = [""] + df_item["アイテム名"].unique().tolist() + ["(新規登録)"]
            sel_i = st.selectbox("アイテム選択", options=i_list, index=i_list.index(ia_n) if ia_n in i_list else 0)
            fin_i_name = st.text_input("アイテム名（確定）", value=sel_i if sel_i != "(新規登録)" else "")
            fin_i_rank = st.selectbox("アイテムランク修正", options=m_ranks, index=m_ranks.index(ia_r) if ia_r in m_ranks else 2)

        st.divider()
        item_pct = get_item_buy_percent(df_item_coef, fin_i_rank)
        if item_pct is not None:
            st.subheader("価格計算結果 (値入確認)")
            b_opt = st.selectbox("計算基準", BASE_OPTIONS, index=1)
            b_val = safe_to_number(st.text_input("基準金額", value=row.get("定価 (円)(商品属性.custom_list_price)", "0")))
            
            bx = derive_base_x(b_opt, b_val, fin_m_rank, df_maker_coef, item_pct)
            calc_p = calc_all_prices(bx, fin_m_rank, df_maker_coef, item_pct)
            
            # 多聞様向けの利益・値入計算
            disp_rows = []
            for rk in PRICE_RANKS:
                s = calc_p[rk]["売価"] or 0
                b = floor_price_custom(calc_p[rk]["買取"]) if calc_p[rk]["買取"] else 0
                profit = s - b
                profit_rate = (profit / s * 100) if s > 0 else 0
                disp_rows.append({
                    "ランク": rk,
                    "推奨売価": f"¥{s:,}",
                    "推奨買取(切下)": f"¥{b:,}",
                    "値入額": f"¥{profit:,}",
                    "値入率": f"{profit_rate:.1f}%"
                })
            
            st.table(pd.DataFrame(disp_rows))
            
            update_yuragi = st.checkbox("この紐づけ(揺らぎ)をマスターに永久保存する", value=False)
            if st.button("価格を保存する", type="primary"):
                with st.spinner("スプレッドシートを更新中..."):
                    # 1. カタログ本体の更新
                    update_cells_by_headers(env["ws_catalog"], row_no, {
                        "商品名": edit_name,
                        "型番(商品属性.mpn)": edit_mpn,
                        "メーカー(商品属性.manufacturer)": fin_m_name
                    })
                    
                    # 2. マスターのメンテナンス（揺らぎ付け替え）
                    if update_yuragi:
                        if ma_h and ma_n != fin_m_name:
                            move_yuragi_keyword(env["ss"], SHEET_MAKER, "メーカー名", "揺らぎ", "メーカーランク", ma_n, fin_m_name, ma_h, fin_m_rank)
                        if ia_h and ia_n != fin_i_name:
                            move_yuragi_keyword(env["ss"], SHEET_ITEM, "アイテム名", "揺らぎ", "アイテムランク", ia_n, fin_i_name, ia_h, fin_i_rank)
                    
                    # 3. ルール保存 (74列展開)
                    r_row_dict = build_rule_row(pid, row.get("商品コード", ""), row.get("画像URL", ""), calc_p)
                    r_no = find_row_number_by_key(env["ws_rules"], "商品ID", pid)
                    if r_no:
                        update_cells_by_headers(env["ws_rules"], r_no, r_row_dict)
                    else:
                        rule_headers = get_headers(env["ws_rules"])
                        vals = [r_row_dict.get(h, "") for h in rule_headers]
                        call_with_retry(lambda: env["ws_rules"].append_row(vals))
                    
                    st.success("価格情報を保存しました。")
                    st.cache_data.clear()
                    st.rerun()
        else:
            st.error("アイテム倍率マスタからランク情報が取得できません。ランク設定を確認してください。")

# ---------------------------------------------------------
# Page 3: 出力 & 本来のログ更新機能
# ---------------------------------------------------------
elif page == "3) 出力（ダウンロード）":
    st.header("3) 出力（ダウンロード）")
    if st.button("出力用タブおよびログを最新に更新する"):
        with st.spinner("全データを同期中..."):
            # カタログ出力用の一時タブ更新
            df_cat_full = pd.DataFrame(env["ws_catalog"].get_all_values())
            df_tmp_cat = pd.DataFrame()
            if not df_cat_full.empty:
                df_cat_full.columns = df_cat_full.iloc[0]; df_cat_full = df_cat_full[1:]
                df_tmp_cat = df_cat_full.reindex(columns=CATALOG_EXPORT_HEADERS_25).fillna("")
                call_with_retry(lambda: env["ws_tmp_cat"].clear())
                call_with_retry(lambda: env["ws_tmp_cat"].update(values=[CATALOG_EXPORT_HEADERS_25] + df_tmp_cat.values.tolist(), range_name="A1"))
            
            # ルール出力用の一時タブ更新
            df_rule_full = pd.DataFrame(env["ws_rules"].get_all_values())
            df_tmp_rules = pd.DataFrame()
            if not df_rule_full.empty:
                df_rule_full.columns = df_rule_full.iloc[0]; df_rule_full = df_rule_full[1:]
                df_tmp_rules = df_rule_full.reindex(columns=RULE_EXPORT_HEADERS_74).fillna("")
                call_with_retry(lambda: env["ws_tmp_rules"].clear())
                call_with_retry(lambda: env["ws_tmp_rules"].update(values=[RULE_EXPORT_HEADERS_74] + df_tmp_rules.values.tolist(), range_name="A1"))

            # ログ記録機能（詳細版の復元）
            today = datetime.date.today().strftime("%Y-%m-%d")
            
            def perform_detailed_logging(ws_log, df_source):
                if df_source.empty or "商品ID" not in df_source.columns: return
                # 既存ログの取得
                log_data = call_with_retry(lambda: ws_log.get_all_values())
                existed_ids = set()
                if len(log_data) > 1:
                    existed_ids = set(str(r[1]).strip() for r in log_data[1:] if len(r) > 1)
                
                new_entries = []
                for current_id in df_source["商品ID"].astype(str).unique():
                    kind = "更新" if current_id in existed_ids else "新規"
                    new_entries.append([today, current_id, kind])
                
                if new_entries:
                    call_with_retry(lambda: ws_log.append_rows(new_entries))

            perform_detailed_logging(env["ws_log_cat"], df_tmp_cat)
            perform_detailed_logging(env["ws_log_rules"], df_tmp_rules)

            st.success("出力データの準備が整いました。ログにも反映済みです。")
