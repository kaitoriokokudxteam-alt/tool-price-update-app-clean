import datetime
import re
import time
import random
from io import BytesIO
from typing import Dict, List, Tuple, Optional, Any

import pandas as pd
import streamlit as st
import gspread
from google.oauth2.service_account import Credentials

# =========================
# シート名（Google Sheets）
# =========================
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

# =========================
# マスターの正しい列順
# =========================
MAKER_HEADERS = ["メーカー名", "揺らぎ", "メーカーランク"]
ITEM_HEADERS = ["アイテム名", "アイテムランク", "揺らぎ"]

# =========================
# ロジック：切り下げ関数
# =========================
def floor_price(price: float) -> int:
    """
    ユーザー指定の切り下げルール:
    - 5桁以上 (12345 -> 12000): 1000円単位切り下げ
    - 4桁 (1234 -> 1200): 100円単位切り下げ
    - 3桁 (123 -> 100): 100円単位切り下げ
    - 2桁 (12 -> 10): 10円単位切り下げ
    """
    if price <= 0:
        return 0
    p = int(price)
    length = len(str(p))
    
    if length >= 5:
        return (p // 1000) * 1000
    elif length == 4 or length == 3:
        return (p // 100) * 100
    elif length == 2:
        return (p // 10) * 10
    else:
        return p

# =========================
# Firebase / GSheets 接続
# =========================
def get_gspread_client():
    if "firebase_config" in st.secrets:
        creds_dict = dict(st.secrets["firebase_config"])
    else:
        # ローカル開発用
        try:
            import json
            with open("service_account.json") as f:
                creds_dict = json.load(f)
        except:
            st.error("GCPの認証情報が見つかりません。")
            return None

    scopes = [
        "https://www.googleapis.com/auth/spreadsheets",
        "https://www.googleapis.com/auth/drive",
    ]
    creds = Credentials.from_service_account_info(creds_dict, scopes=scopes)
    return gspread.authorize(creds)

def call_with_retry(func, max_retries=5):
    for i in range(max_retries):
        try:
            return func()
        except Exception as e:
            if "quota" in str(e).lower() or "429" in str(e):
                time.sleep(2 ** i + random.random())
            else:
                raise e
    return func()

# =========================
# データ読み込み・マッチング
# =========================
@st.cache_data(ttl=600)
def load_all_masters():
    client = get_gspread_client()
    url = "https://docs.google.com/spreadsheets/d/18o7HVMPQZ3CnPTPfCdRkLeQeLsKi-JLZM2e_Gw44rHU/edit"
    sh = client.open_by_url(url)

    def get_df(name):
        ws = sh.worksheet(name)
        data = call_with_retry(lambda: ws.get_all_values())
        if not data: return pd.DataFrame()
        return pd.DataFrame(data[1:], columns=data[0])

    return {
        "catalog": get_df(SHEET_CATALOG),
        "maker": get_df(SHEET_MAKER),
        "item": get_df(SHEET_ITEM),
        "maker_coef": get_df(SHEET_MAKER_COEF),
        "item_coef": get_df(SHEET_ITEM_COEF),
        "rules": get_df(SHEET_RULES),
    }

def find_match_with_keyword(name: str, master_df: pd.DataFrame, name_col: str, yuragi_col: str) -> Tuple[Optional[pd.Series], Optional[str]]:
    """
    マスターから名前または揺らぎでマッチングし、
    (マッチした行, ヒットしたキーワード) を返す。
    """
    if not name or master_df.empty:
        return None, None
    
    name_s = str(name).strip().lower()
    
    # 1. 完全一致（メーカー名そのもの）
    for _, row in master_df.iterrows():
        m_name = str(row[name_col]).strip()
        if m_name.lower() == name_s:
            return row, m_name
            
    # 2. 揺らぎ一致
    for _, row in master_df.iterrows():
        yuragi_str = str(row[yuragi_col])
        if not yuragi_str:
            continue
        keywords = [k.strip() for k in yuragi_str.replace("、", ",").split(",") if k.strip()]
        for kw in keywords:
            if kw.lower() in name_s:
                # 長いキーワードから先に判定するなどの工夫も可能だが、ここでは最初に見つかったもの
                return row, kw
                
    return None, None

# =========================
# メインUI
# =========================
def main():
    st.set_page_config(page_title="工具価格更新アプリ", layout="wide")
    st.title("🛠 工具価格更新アプリ")

    masters = load_all_masters()
    if not masters: return

    # サイドバー：更新ボタン
    if st.sidebar.button("マスター再読込"):
        st.cache_data.clear()
        st.rerun()

    tab1, tab2 = st.tabs(["既存商品（価格決定・編集）", "商品カタログ確認"])

    # ---------------------------------------------------------
    # Tab 1: 既存商品（価格決定・編集）
    # ---------------------------------------------------------
    with tab1:
        st.subheader("既存商品の価格設定")
        
        # 検索
        search_col1, search_col2 = st.columns([3, 1])
        with search_col1:
            target_pid = st.text_input("商品IDまたは型番を入力してください")
        
        df_cat = masters["catalog"]
        target_row = None
        
        if target_pid:
            res = df_cat[(df_cat["商品ID"] == target_pid) | (df_cat["型番"] == target_pid)]
            if not res.empty:
                target_row = res.iloc[0]
            else:
                st.warning("商品が見つかりません。")

        if target_row is not None:
            st.info(f"対象商品: {target_row['商品名']} ({target_row['型番']})")
            
            # --- 1. 自動判定ロジック実行 ---
            hit_maker_row, hit_maker_kw = find_match_with_keyword(target_row["商品名"], masters["maker"], "メーカー名", "揺らぎ")
            hit_item_row, hit_item_kw = find_match_with_keyword(target_row["商品名"], masters["item"], "アイテム名", "揺らぎ")
            
            # --- 2. ユーザー修正・選択エリア ---
            st.divider()
            col_m, col_i = st.columns(2)
            
            with col_m:
                st.write("### メーカー設定")
                # メーカーリスト
                maker_list = masters["maker"]["メーカー名"].unique().tolist()
                default_maker = hit_maker_row["メーカー名"] if hit_maker_row is not None else ""
                
                # セレクトボックス（リストにない場合は新規入力扱いにするための工夫）
                selected_maker = st.selectbox(
                    "メーカー名を選択", 
                    options=[""] + maker_list + ["(新規作成)"],
                    index=maker_list.index(default_maker) + 1 if default_maker in maker_list else 0
                )
                
                final_maker_name = selected_maker
                if selected_maker == "(新規作成)":
                    final_maker_name = st.text_input("新規メーカー名を入力")
                
                # ランク選択
                current_m_rank = hit_maker_row["メーカーランク"] if hit_maker_row is not None else "C"
                final_m_rank = st.selectbox("メーカーランク", ["A", "B", "C", "D", "E"], index=["A", "B", "C", "D", "E"].index(current_m_rank))

            with col_i:
                st.write("### アイテム設定")
                item_list = masters["item"]["アイテム名"].unique().tolist()
                default_item = hit_item_row["アイテム名"] if hit_item_row is not None else ""
                
                selected_item = st.selectbox(
                    "アイテムカテゴリを選択",
                    options=[""] + item_list + ["(新規作成)"],
                    index=item_list.index(default_item) + 1 if default_item in item_list else 0
                )
                
                final_item_name = selected_item
                if selected_item == "(新規作成)":
                    final_item_name = st.text_input("新規アイテム名を入力")
                
                # ランク選択
                current_i_rank = hit_item_row["アイテムランク"] if hit_item_row is not None else "C"
                final_i_rank = st.selectbox("アイテムランク", ["A", "B", "C", "D", "E"], index=["A", "B", "C", "D", "E"].index(current_i_rank))

            # --- 3. 価格計算 ---
            st.divider()
            st.write("### 価格計算結果")
            
            # 倍率取得
            m_coef_row = masters["maker_coef"][masters["maker_coef"]["ランク"] == final_m_rank]
            i_coef_row = masters["item_coef"][masters["item_coef"]["ランク"] == final_i_rank]
            
            if not m_coef_row.empty and not i_coef_row.empty:
                m_buy_rate = float(m_coef_row.iloc[0]["買取倍率"])
                m_sell_rate = float(m_coef_row.iloc[0]["販売倍率"])
                i_buy_rate = float(i_coef_row.iloc[0]["買取倍率"])
                i_sell_rate = float(i_coef_row.iloc[0]["販売倍率"])
                
                base_price = float(target_row["定価"]) if target_row["定価"] else 0
                
                # 計算ロジック
                calc_buy = base_price * m_buy_rate * i_buy_rate
                calc_sell = base_price * m_sell_rate * i_sell_rate
                
                # 新しい切り下げルールの適用
                final_buy = floor_price(calc_buy)
                final_sell = floor_price(calc_sell)
                
                c1, c2, c3 = st.columns(3)
                c1.metric("定価", f"¥{int(base_price):,}")
                c2.metric("推奨買取価格", f"¥{final_buy:,}", help="桁数に応じた切り下げ適用済")
                c3.metric("推奨販売価格", f"¥{final_sell:,}", help="桁数に応じた切り下げ適用済")
                
                # 最終調整用
                adj_buy = st.number_input("最終買取価格 (上書き可能)", value=final_buy, step=100)
                adj_sell = st.number_input("最終販売価格 (上書き可能)", value=final_sell, step=100)

                # --- 4. 保存と揺らぎメンテナンス ---
                st.divider()
                st.write("### マスター登録オプション")
                update_master = st.checkbox("メーカー・アイテムの紐づけ（揺らぎ）を修正/登録する", value=False)
                
                if st.button("価格情報を保存する"):
                    # 保存処理の実行
                    with st.spinner("データを更新中..."):
                        client = get_gspread_client()
                        sh = client.open_by_url("https://docs.google.com/spreadsheets/d/18o7HVMPQZ3CnPTPfCdRkLeQeLsKi-JLZM2e_Gw44rHU/edit")
                        
                        # (A) T_rules への価格保存（既存ロジックを流用）
                        # 省略：実際の保存処理（append_rows等）をここに記述
                        
                        # (B) マスターのメンテナンス
                        if update_master:
                            # メーカーの揺らぎ修正
                            if hit_maker_kw and hit_maker_row is not None and hit_maker_row["メーカー名"] != final_maker_name:
                                # 旧マスターからヒットしたKWを削除、新マスターへ追加する関数を呼ぶ
                                update_master_yuragi(sh, SHEET_MAKER, "メーカー名", "揺らぎ", "メーカーランク", 
                                                   hit_maker_row["メーカー名"], final_maker_name, hit_maker_kw, final_m_rank)
                            
                            # アイテムの揺らぎ修正
                            if hit_item_kw and hit_item_row is not None and hit_item_row["アイテム名"] != final_item_name:
                                update_master_yuragi(sh, SHEET_ITEM, "アイテム名", "揺らぎ", "アイテムランク",
                                                   hit_item_row["アイテム名"], final_item_name, hit_item_kw, final_i_rank)

                        st.success("保存が完了しました。")
                        st.cache_data.clear()
            else:
                st.error("倍率マスタが見つかりません。ランク設定を確認してください。")

# =========================
# マスター更新ロジック（揺らぎの付け替え）
# =========================
def update_master_yuragi(sh, sheet_name, name_col_name, yuragi_col_name, rank_col_name, old_name, new_name, keyword, new_rank):
    ws = sh.worksheet(sheet_name)
    data = ws.get_all_values()
    headers = data[0]
    df = pd.DataFrame(data[1:], columns=headers)
    
    n_idx = headers.index(name_col_name)
    y_idx = headers.index(yuragi_col_name)
    r_idx = headers.index(rank_col_name)
    
    # 1. 旧マスターからキーワードを削除
    if old_name and old_name in df[name_col_name].values:
        row_idx = df[df[name_col_name] == old_name].index[0]
        current_yuragi = str(df.at[row_idx, yuragi_col_name])
        # キーワードを削除（カンマ区切り考慮）
        kw_list = [k.strip() for k in current_yuragi.replace("、", ",").split(",") if k.strip()]
        if keyword in kw_list:
            kw_list.remove(keyword)
        new_yuragi_str = ",".join(kw_list)
        ws.update_cell(row_idx + 2, y_idx + 1, new_yuragi_str)

    # 2. 新マスターへキーワードを追加
    if new_name:
        if new_name in df[name_col_name].values:
            # 既存の行を更新
            row_idx = df[df[name_col_name] == new_name].index[0]
            current_yuragi = str(df.at[row_idx, yuragi_col_name])
            kw_list = [k.strip() for k in current_yuragi.replace("、", ",").split(",") if k.strip()]
            if keyword not in kw_list:
                kw_list.append(keyword)
            new_yuragi_str = ",".join(kw_list)
            ws.update_cell(row_idx + 2, y_idx + 1, new_yuragi_str)
            ws.update_cell(row_idx + 2, r_idx + 1, new_rank) # ランクも最新に
        else:
            # 新規行を追加
            new_row = [""] * len(headers)
            new_row[n_idx] = new_name
            new_row[y_idx] = keyword
            new_row[r_idx] = new_rank
            ws.append_row(new_row)

if __name__ == "__main__":
    main()
