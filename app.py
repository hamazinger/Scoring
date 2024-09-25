import streamlit as st
from google.cloud import bigquery
from google.oauth2 import service_account
import pandas as pd
from datetime import datetime, timedelta
from wordcloud import WordCloud
import matplotlib.pyplot as plt
from janome.tokenizer import Tokenizer
import re
import requests

# 認証関数
def authenticate(username, password):
    url = 'https://majisemi.com/e/api/check_user'
    data = {'name': username, 'pass': password}
    response = requests.post(url, data=data)
    response_json = response.json()
    if response_json.get('status') == 'ok':
        majisemi = response_json.get('majisemi', False)
        group_code = response_json.get('group_code', '')
        payment = response_json.get('payment', '')
        if majisemi:
            return {'authenticated': True, 'majisemi': True, 'group_code': group_code}
        elif payment == 'マジセミ倶楽部':
            return {'authenticated': True, 'majisemi': False, 'group_code': group_code}
        else:
            return {'authenticated': False}
    else:
        return {'authenticated': False}

# ログインページの関数
def login_page():
    if "login_checked" not in st.session_state:
        st.session_state.login_checked = False

    if not st.session_state.login_checked:
        col1, col2, col3 = st.columns([1,2,1])

        with col2:
            title_placeholder = st.empty()
            title_placeholder.title("リードスコアリング")
            username_placeholder = st.empty()
            password_placeholder = st.empty()
            username = username_placeholder.text_input("ユーザー名")
            password = password_placeholder.text_input("パスワード", type="password")
            login_message_placeholder = st.empty()
            login_message_placeholder.write("※マジカンのアカウントでログインできます")

            login_button_placeholder = st.empty()
            if login_button_placeholder.button("ログイン"):
                auth_result = authenticate(username, password)
                if auth_result['authenticated']:
                    st.session_state['authenticated'] = True
                    st.session_state['majisemi'] = auth_result['majisemi']
                    st.session_state['group_code'] = auth_result.get('group_code', '')
                    st.session_state.login_checked = True
                    title_placeholder.empty()
                    username_placeholder.empty()
                    password_placeholder.empty()
                    login_button_placeholder.empty()
                    login_message_placeholder.empty()
                else:
                    st.error("認証に失敗しました。")

    if st.session_state.login_checked:
        main_page()

# メインページ
def main_page():
    st.title("リードスコアリング")

    try:
        service_account_info = st.secrets["gcp_service_account"]
        credentials = service_account.Credentials.from_service_account_info(service_account_info)
        project_id = service_account_info["project_id"]
        client = bigquery.Client(credentials=credentials, project=project_id)
    except KeyError:
        st.error("GCPの認証情報が見つかりません。StreamlitのSecretsに設定してください。")
        st.stop()

    followdata_table = "mythical-envoy-386309.majisemi.majisemi_followdata_addCode"

    def run_query(query: str, _params=None):
        job_config = bigquery.QueryJobConfig()
        if _params:
            job_config.query_parameters = _params
        query_job = client.query(query, job_config=job_config)
        rows_raw = query_job.result()
        rows = [dict(row) for row in rows_raw]
        return rows

    def show_query_and_params(query, params):
        st.text("実行されたクエリ:")
        st.code(query)
        st.text("クエリパラメータ:")
        st.json({p.name: p.value for p in params})

    @st.cache_data(ttl=3600)
    def get_organizer_names():
        query = f"""
        SELECT Organizer_Code, MIN(Organizer_Name) AS Organizer_Name
        FROM `{followdata_table}`
        GROUP BY Organizer_Code
        ORDER BY Organizer_Name
        """
        result = run_query(query)
        return [f"{row['Organizer_Name']}【{row['Organizer_Code']}】" for row in result]

    if st.session_state.get('majisemi', False):
        organizer_names = get_organizer_names()
        organizer_keyword = st.selectbox("主催企業名を選択してください：", [""] + organizer_names)
    else:
        group_code = st.session_state.get('group_code')
        if group_code:
            query = f"""
            SELECT DISTINCT Organizer_Name, Organizer_Code
            FROM `{followdata_table}`
            WHERE Organizer_Code = @group_code
            """
            result = run_query(query, [bigquery.ScalarQueryParameter("group_code", "STRING", group_code)])
            organizer_name_list = [f"{row['Organizer_Name']}【{row['Organizer_Code']}】" for row in result]
            if organizer_name_list:
                organizer_keyword = organizer_name_list[0]
                st.write(f"主催企業名: {organizer_keyword}")
            else:
                st.error("Organizer_Name が見つかりませんでした。")
                st.stop()
        else:
            st.error("group_code がありません。")
            st.stop()

    col1, col2, col3 = st.columns(3)

    with col1:
        st.subheader("業種")
        industries = [
            "製造", "通信キャリア・データセンター", "商社", "小売", "金融",
            "建設・土木・設備工事", "マーケティング・広告・出版・印刷", "教育", "IT関連企業"
        ]
        selected_industries = st.multiselect("業種を選択してください", industries)

    with col2:
        st.subheader("従業員規模")
        employee_sizes = [
            "5000人以上", "1000人以上5000人未満", "500人以上1000人未満",
            "300人以上500人未満", "100人以上300人未満", "30人以上100人未満",
            "10人以上30人未満", "10人未満"
        ]
        selected_employee_sizes = st.multiselect("従業員規模を選択してください", employee_sizes)

    with col3:
        st.subheader("役職")
        positions = [
            "経営者・役員クラス", "事業部長/工場長クラス", "部長クラス",
            "課長クラス", "係長・主任クラス", "一般社員・職員クラス"
        ]
        selected_positions = st.multiselect("役職を選択してください", positions)

    execute_button = st.button("実行")

    if execute_button:
        st.cache_data.clear()

        today = datetime.today()
        six_months_ago = today - timedelta(days=180)

        query_parameters = []

        # IT関連企業でヒットさせたい業種リスト
        it_industry_values = [
            "システム・インテグレータ",
            "IT・ビジネスコンサルティング",
            "IT関連製品販売",
            "SaaS・Webサービス事業",
            "その他ITサービス関連"
        ]

        additional_conditions = []
        # IT関連企業フィルタ
        if "IT関連企業" in selected_industries:
            it_industry_conditions = " OR ".join([f"User_Company LIKE '%{value}%'" for value in it_industry_values])
            additional_conditions.append(f"({it_industry_conditions})")
            selected_industries.remove("IT関連企業")  # IT関連企業は別に扱うのでリストから削除

        # その他の業種フィルタ
        if selected_industries:
            other_industry_conditions = " OR ".join([f"User_Company LIKE '%' || @industry_{i} || '%'" for i in range(len(selected_industries))])
            additional_conditions.append(f"({other_industry_conditions})")
            query_parameters.extend([bigquery.ScalarQueryParameter(f"industry_{i}", "STRING", industry) for i, industry in enumerate(selected_industries)])

        # 従業員規模フィルタ
        if selected_employee_sizes:
            employee_size_conditions = " OR ".join([f"Employee_Size LIKE '%' || @employee_size_{i} || '%'" for i in range(len(selected_employee_sizes))])
            additional_conditions

.append(f"({employee_size_conditions})")
            query_parameters.extend([bigquery.ScalarQueryParameter(f"employee_size_{i}", "STRING", size) for i, size in enumerate(selected_employee_sizes)])

        # 役職フィルタ
        if selected_positions:
            position_conditions = " OR ".join([f"Position_Category LIKE '%' || @position_{i} || '%'" for i in range(len(selected_positions))])
            additional_conditions.append(f"({position_conditions})")
            query_parameters.extend([bigquery.ScalarQueryParameter(f"position_{i}", "STRING", position) for i, position in enumerate(selected_positions)])

        # Organizer_Codeを使った処理
        if st.session_state.get('majisemi', False):
            # Organizer_Codeを抽出
            organizer_code = organizer_keyword.split('【')[-1].replace('】', '')
        else:
            group_code = st.session_state.get('group_code')
            organizer_code = group_code  # group_code が Organizer_Code として使用される場合
            if not organizer_code:
                st.error("Organizer_Code が見つかりませんでした。")
                st.stop()

        # Organizer_Codeを使ったクエリ
        query_parameters.append(bigquery.ScalarQueryParameter("organizer_code", "STRING", organizer_code))
        organizer_filter = "Organizer_Code = @organizer_code"

        attendee_query = f"""
        SELECT DISTINCT
            Company_Name, Organizer_Code
        FROM
            `{followdata_table}`
        WHERE {organizer_filter}
        """

        # IT関連企業とその他の業種条件をORで結合し、その他のフィルタ条件とはANDで結合
        if additional_conditions:
            attendee_query += " AND (" + " OR ".join(additional_conditions) + ")"

        show_query_and_params(attendee_query, query_parameters)

        try:
            attendee_data = run_query(attendee_query, query_parameters)
            filtered_companies = [row['Company_Name'] for row in attendee_data if row.get('Company_Name')]
            filtered_companies = list(set(filtered_companies))

            if filtered_companies:
                st.write(f"フィルタリング後の企業数: {len(filtered_companies)}")
            else:
                st.warning("フィルタリング後の企業が見つかりませんでした。")
                st.stop()

        except Exception as e:
            st.error(f"BigQueryのクエリに失敗しました: {str(e)}")
            st.stop()

# メイン関数
def main():
    if 'authenticated' not in st.session_state:
        st.session_state['authenticated'] = False

    if st.session_state['authenticated']:
        main_page()
    else:
        login_page()

if __name__ == "__main__":
    main()
