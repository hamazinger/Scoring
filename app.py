import streamlit as st
from google.cloud import bigquery
from google.oauth2 import service_account
from datetime import datetime, timedelta
import requests
import matplotlib.pyplot as plt
from wordcloud import WordCloud
from janome.tokenizer import Tokenizer
import re

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
        col1, col2, col3 = st.columns([1, 2, 1])

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
        organizer_code = organizer_keyword.split('【')[-1].replace('】', '')
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
                organizer_code = organizer_keyword.split('【')[-1].replace('】', '')
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
        selected_industries = []
        for idx, industry in enumerate(industries):
            if st.checkbox(industry, key=f"industry_{idx}"):
                selected_industries.append(industry)

    with col2:
        st.subheader("従業員規模")
        employee_sizes = [
            "5000人以上", "1000人以上5000人未満", "500人以上1000人未満",
            "300人以上500人未満", "100人以上300人未満", "30人以上100人未満",
            "10人以上30人未満", "10人未満"
        ]
        selected_employee_sizes = []
        for idx, size in enumerate(employee_sizes):
            if st.checkbox(size, key=f"employee_size_{idx}"):
                selected_employee_sizes.append(size)

    with col3:
        st.subheader("役職")
        positions = [
            "経営者・役員クラス", "事業部長/工場長クラス", "部長クラス",
            "課長クラス", "係長・主任クラス", "一般社員・職員クラス"
        ]
        selected_positions = []
        for idx, position in enumerate(positions):
            if st.checkbox(position, key=f"position_{idx}"):
                selected_positions.append(position)

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

        # 業種フィルタをOR条件で構築
        if selected_industries:
            industry_conditions = []
            param_counter = 0  # パラメータ名のカウンター
            for industry in selected_industries:
                if industry == "IT関連企業":
                    # "IT関連企業" が選択された場合は、it_industry_values の値をOR条件で追加
                    for it_industry in it_industry_values:
                        param_name = f"industry_{param_counter}"
                        industry_conditions.append(f"User_Company LIKE '%' || @{param_name} || '%'")
                        query_parameters.append(bigquery.ScalarQueryParameter(param_name, "STRING", it_industry))
                        param_counter += 1
                else:
                    param_name = f"industry_{param_counter}"
                    industry_conditions.append(f"User_Company LIKE '%' || @{param_name} || '%'")
                    query_parameters.append(bigquery.ScalarQueryParameter(param_name, "STRING", industry))
                    param_counter += 1
            
            additional_conditions.append("(" + " OR ".join(industry_conditions) + ")")

        # 従業員規模フィルタをOR条件で構築
        if selected_employee_sizes:
            employee_size_conditions = " OR ".join([f"Employee_Size LIKE '%' || @employee_size_{i} || '%'" for i in range(len(selected_employee_sizes))])
            additional_conditions.append(f"({employee_size_conditions})")
            query_parameters.extend([bigquery.ScalarQueryParameter(f"employee_size_{i}", "STRING", size) for i, size in enumerate(selected_employee_sizes)])

        # 役職フィルタをOR条件で構築
        if selected_positions:
            position_conditions = " OR ".join([f"Position_Category LIKE '%' || @position_{i} || '%'" for i in range(len(selected_positions))])
            additional_conditions.append(f"({position_conditions})")
            query_parameters.extend([bigquery.ScalarQueryParameter(f"position_{i}", "STRING", position) for i, position in enumerate(selected_positions)])

        # Organizer_Codeを使ったクエリ
        query_parameters.append(bigquery.ScalarQueryParameter("organizer_code", "STRING", organizer_code))
        organizer_filter = "Organizer_Code = @organizer_code"
        
        # クエリ構築: 各条件をANDで結合
        attendee_query = f"""
        SELECT DISTINCT
            Company_Name, Organizer_Code
        FROM
            `{followdata_table}`
        WHERE {organizer_filter}
        """

        # 業種、従業員規模、役職のフィルタをANDで結合
        if additional_conditions:
            attendee_query += " AND " + " AND ".join(additional_conditions)

        try:
            attendee_data = run_query(attendee_query, query_parameters)
            filtered_companies = [row['Company_Name'] for row in attendee_data if row.get('Company_Name')]
            filtered_companies = list(set(filtered_companies))

            if not filtered_companies:
                st.warning("フィルタリング後の企業が見つかりませんでした。")
                st.stop()

            def escape_company_name(name):
                return name.replace("'", "''")

            escaped_companies = [escape_company_name(company) for company in filtered_companies]

            all_seminars_query = f"""
            SELECT *
            FROM `{followdata_table}`
            WHERE Company_Name IN UNNEST(@companies) 
              AND Seminar_Date >= @six_months_ago
            ORDER BY Company_Name, Seminar_Date
            """

            query_params = [
                bigquery.ArrayQueryParameter("companies", "STRING", escaped_companies),
                bigquery.ScalarQueryParameter("six_months_ago", "DATE", six_months_ago.date())
            ]

            all_seminars_data = run_query(all_seminars_query, query_params)

            # スコアの計算ロジック
            def calculate_score(row):
                score = 0
                if row['Status'] == '出席':
                    score += 3
                if any(row.get(f'Post_Seminar_Survey_Answer_{i}', '') for i in range(1, 4)):
                    score += 3
                if row.get('Desired_Follow_Up_Actions'):
                    if '製品やサービス導入に関する具体的な要望がある' in row['Desired_Follow_Up_Actions']:
                        score += 5
                    elif '資料希望' in row['Desired_Follow_Up_Actions']:
                        score += 3
                if row.get('Pre_Seminar_Survey_Answer_2'):
                    if '既に同様の商品・サービスを導入済み' in row['Pre_Seminar_Survey_Answer_2']:
                        score += 3
                    elif '既に候補の製品・サービスを絞っており、その評価・選定をしている' in row['Pre_Seminar_Survey_Answer_2']:
                        score += 3
                    elif '製品・サービスの候補を探している' in row['Pre_Seminar_Survey_Answer_2']:
                        score += 2
                    elif '導入するかどうか社内で検討中' in row['Pre_Seminar_Survey_Answer_2']:
                        score += 1
                return score

            # スコアリングの実行
            company_scores = {}
            for row in all_seminars_data:
                score = calculate_score(row)
                company_name = row['Company_Name']
                if company_name in company_scores:
                    company_scores[company_name] += score
                else:
                    company_scores[company_name] = score

            # スコア順にソート
            sorted_scores = sorted(company_scores.items(), key=lambda item: item[1], reverse=True)

            if not sorted_scores:
                st.warning("スコアリング後の企業が見つかりませんでした。")
                st.stop()

            # ワードクラウドの生成
            def generate_wordcloud(font_path, text):
                t = Tokenizer()
                tokens = t.tokenize(text)
                words = [token.surface for token in tokens if token.part_of_speech.split(',')[0] in ['名詞', '動詞']]
        
                words = [word for word in words if len(word) > 1]
                words = [word for word in words if not re.match('^[ぁ-ん]{2}$', word)]
                words = [word for word in words if not re.match('^[一-龠々]{1}[ぁ-ん]{1}$', word)]
        
                exclude_words = {'ギフト', 'ギフトカード', 'サービス', 'できる', 'ランキング', '可能', '課題', '会員', '会社', '開始', '開発', '活用', '管理', '企業', '機能',
                                 '記事', '技術', '業界', '後編', '公開', '最適', '支援', '事業', '実現', '重要', '世界', '成功', '製品', '戦略', '前編', '対策', '抽選',
                                 '調査', '提供', '投資', '導入', '発表', '必要', '方法', '目指す', '問題', '利用', '理由', 'する', '解説', '影響', '与える'}
                words = [word for word in words if word not in exclude_words]
        
                wordcloud = WordCloud(font_path=font_path, background_color='white', width=800, height=400).generate(' '.join(words))
        
                fig, ax = plt.subplots(figsize=(10, 5))
                ax.imshow(wordcloud, interpolation='bilinear')
                ax.axis('off')
                st.pyplot(fig)

            st.header("トップ5企業")
            for i in range(min(5, len(sorted_scores))):
                company_name, score = sorted_scores[i]
                # st.subheader(f"{i + 1}位. {company_name} (スコア: {score})")
                st.subheader(f"{i + 1}位. {company_name})
                
                # 他社セミナーの抽出
                other_seminars = [
                    row for row in all_seminars_data 
                    if row['Company_Name'] == company_name and row['Organizer_Code'] != organizer_code
                ]
                
                if other_seminars:
                    seminar_titles = ' '.join([row['Seminar_Title'] for row in other_seminars])
                    try:
                        generate_wordcloud('NotoSansJP-Regular.ttf', seminar_titles)
                    except Exception as e:
                        st.error(f"ワードクラウドの生成中にエラーが発生しました: {str(e)}")
                else:
                    st.warning(f"{company_name}の他社セミナー参加履歴が見つかりませんでした。")
                
                # st.write("参加セミナー詳細:")
                # company_seminars = [row for row in all_seminars_data if row['Company_Name'] == company_name]
                # for seminar in company_seminars[:5]:
                #     st.write(f"- {seminar['Seminar_Title']} (主催: {seminar['Organizer_Name']})")
                
                st.write("---")

        except Exception as e:
            st.error(f"エラーが発生しました: {str(e)}")
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
