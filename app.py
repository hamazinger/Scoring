import streamlit as st
from google.cloud import bigquery
from google.oauth2 import service_account
import matplotlib.pyplot as plt
import japanize_matplotlib
from wordcloud import WordCloud
from janome.tokenizer import Tokenizer
import re
from datetime import datetime, timedelta
from st_aggrid import AgGrid, GridOptionsBuilder, GridUpdateMode, DataReturnMode
import pandas as pd

# Streamlitアプリのタイトルを設定
st.title("マジセミリードスコアリング＆ワードクラウド")

# テーブルIDを直接指定 (Secretsの問題が解決したら、Secretsから取得するように戻してください)
destination_table = "mythical-envoy-386309.majisemi.majisemi_followdata"

# 認証情報の設定
service_account_info = st.secrets["gcp_service_account"]
credentials = service_account.Credentials.from_service_account_info(service_account_info)
project_id = service_account_info["project_id"] 
client = bigquery.Client(credentials=credentials, project=project_id)

# BigQueryからデータを取得する関数
@st.cache(ttl=600)
def run_query(query, params=None):
    # ScalarQueryParameterオブジェクトのリストを作成
    if params:
        query_params = [bigquery.ScalarQueryParameter(None, "STRING", param) for param in params]
    else:
        query_params = None
    
    query_job = client.query(query, job_config=bigquery.QueryJobConfig(query_parameters=query_params))
    rows_raw = query_job.result()
    rows = [dict(row) for row in rows_raw]
    return rows

# ユーザーがキーワードを入力できるようにする
organizer_keyword = st.text_input("主催企業名キーワードを入力してください：", "") # 初期値を空にする

# カテゴリ選択を横に3つ並べる
col1, col2, col3 = st.columns(3)

# --- 業種選択 ---
with col1:
    industries = [
        {"User_Company": "製造"},
        {"User_Company": "通信キャリア・データセンター"},
        {"User_Company": "商社"},
        {"User_Company": "小売"},
        {"User_Company": "金融"},
        {"User_Company": "建設・土木・設備工事"},
        {"User_Company": "マーケティング・広告・出版・印刷"},
        {"User_Company": "教育"},
        {"User_Company": "その他"},
        {"User_Company": "10. システム・インテグレータ"},
        {"User_Company": "11. IT・ビジネスコンサルティング"},
        {"User_Company": "12. IT関連製品販売"},
        {"User_Company": "13. IT関連製品販売"},
        {"User_Company": "14. SaaS・Webサービス事業"},
        {"User_Company": "15. その他ITサービス関連"},
    ]
    gb = GridOptionsBuilder.from_dataframe(pd.DataFrame(industries))
    gb.configure_selection(selection_mode="multiple", use_checkbox=True)
    gb.configure_grid_options(domLayout='normal')
    grid_options_industries = gb.build()
    st.subheader("業種") # 見出しを修正
    selected_rows_industries = AgGrid(
        pd.DataFrame(industries),
        gridOptions=grid_options_industries,
        data_return_mode=DataReturnMode.FILTERED_AND_SORTED,
        update_mode=GridUpdateMode.MODEL_CHANGED,
        theme='streamlit',
        fit_columns_on_grid_load=True,
        # enable_enterprise_modules=True,
        height=350,
        # width='100%',
    )
    selected_industries = selected_rows_industries["data"]["User_Company"].tolist()

# --- 従業員規模選択 ---
with col2:
    employee_sizes = [
        {"Employee_Size": "1. 5000人以上"},
        {"Employee_Size": "2. 1000人以上5000人未満"},
        {"Employee_Size": "3. 500人以上1000人未満"},
        {"Employee_Size": "4. 300人以上500人未満"},
        {"Employee_Size": "5. 100人以上300人未満"},
        {"Employee_Size": "6. 30人以上100人未満"},
        {"Employee_Size": "7. 10人以上30人未満"},
        {"Employee_Size": "8. 10人未満"},
        {"Employee_Size": "分からない"},
    ]
    gb = GridOptionsBuilder.from_dataframe(pd.DataFrame(employee_sizes))
    gb.configure_selection(selection_mode="multiple", use_checkbox=True)
    gb.configure_grid_options(domLayout='normal')
    grid_options_employee_sizes = gb.build()
    st.subheader("従業員規模") # 見出しを修正
    selected_rows_employee_sizes = AgGrid(
        pd.DataFrame(employee_sizes),
        gridOptions=grid_options_employee_sizes,
        data_return_mode=DataReturnMode.FILTERED_AND_SORTED,
        update_mode=GridUpdateMode.MODEL_CHANGED,
        theme='streamlit',
        fit_columns_on_grid_load=True,
        # enable_enterprise_modules=True,
        height=350,
        # width='100%',
    )
    selected_employee_sizes = selected_rows_employee_sizes["data"]["Employee_Size"].tolist()

# --- 役職選択 ---
with col3:
    positions = [
        {"Position_Category": "1. 経営者・役員クラス"},
        {"Position_Category": "2. 事業部長/工場長クラス"},
        {"Position_Category": "3. 部長クラス"},
        {"Position_Category": "4. 課長クラス"},
        {"Position_Category": "5. 係長・主任クラス"},
        {"Position_Category": "6. 一般社員・職員クラス"},
        {"Position_Category": "7. その他"},
    ]
    gb = GridOptionsBuilder.from_dataframe(pd.DataFrame(positions))
    gb.configure_selection(selection_mode="multiple", use_checkbox=True)
    gb.configure_grid_options(domLayout='normal')
    grid_options_positions = gb.build()
    st.subheader("役職") # 見出しを修正
    selected_rows_positions = AgGrid(
        pd.DataFrame(positions),
        gridOptions=grid_options_positions,
        data_return_mode=DataReturnMode.FILTERED_AND_SORTED,
        update_mode=GridUpdateMode.MODEL_CHANGED,
        theme='streamlit',
        fit_columns_on_grid_load=True,
        # enable_enterprise_modules=True,
        height=350,
        # width='100%',
    )
    selected_positions = selected_rows_positions["data"]["Position_Category"].tolist()

# 実行ボタンを追加
execute_button = st.button("実行")

# ボタンが押された場合のみ処理を実行
if execute_button:
    # 現在の日付と過去3ヶ月の日付を取得
    today = datetime.today()
    three_months_ago = today - timedelta(days=365)

    # 選択された項目に基づいてクエリを変更
    where_clauses = []
    if selected_industries:
        # IT関連企業をまとめるための条件を追加
        it_related_condition = " OR ".join([f"User_Company = '{i}'" for i in ["10. システム・インテグレータ", "11. IT・ビジネスコンサルティング", "12. IT関連製品販売", "13. IT関連製品販売"]])
        industry_conditions = " OR ".join([f"User_Company = '{industry}'" for industry in selected_industries if industry != "IT関連企業"])
        if industry_conditions:
            where_clauses.append(f"({industry_conditions} OR ({it_related_condition}))")
        else:
            where_clauses.append(f"({it_related_condition})")
    if selected_employee_sizes:
        employee_size_conditions = " OR ".join([f"Employee_Size = '{size}'" for size in selected_employee_sizes])
        where_clauses.append(f"({employee_size_conditions})")
    if selected_positions:
        position_conditions = " OR ".join([f"Position_Category = '{position}'" for position in selected_positions])
        where_clauses.append(f"({position_conditions})")

    # WHERE句を構築
    if where_clauses:
        where_clause = " AND ".join(where_clauses)
        attendee_query = f"""
        SELECT DISTINCT Company_Name
        FROM `{destination_table}`
        WHERE Organizer_Name LIKE %s AND {where_clause}
        """
        attendee_data = run_query(attendee_query, (f"%{organizer_keyword}%",))
    else:
        st.warning("業種、従業員規模、役職のいずれかを選択してください。")
        attendee_data = []

    # --- (以降の処理は同じ) ---
