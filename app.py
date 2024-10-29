import streamlit as st
from google.cloud import bigquery
from google.oauth2 import service_account
from datetime import datetime, timedelta
import requests
import matplotlib.pyplot as plt
from wordcloud import WordCloud
from janome.tokenizer import Tokenizer
import re
import pandas as pd

# 認証関数は既存のまま保持
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

def login_page():
    if "login_checked" not in st.session_state:
        st.session_state.login_checked = False

    if not st.session_state.login_checked:
        col1, col2, col3 = st.columns([1, 2, 1])

        with col2:
            title_placeholder = st.empty()
            title_placeholder.title("Lead Scoring")
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

def main_page():
    st.title("企業セミナー参加傾向分析")

    try:
        # BigQuery認証設定
        service_account_info = st.secrets["gcp_service_account"]
        credentials = service_account.Credentials.from_service_account_info(service_account_info)
        project_id = service_account_info["project_id"]
        client = bigquery.Client(credentials=credentials, project=project_id)
    except KeyError:
        st.error("GCPの認証情報が見つかりません。StreamlitのSecretsに設定してください。")
        st.stop()

    followdata_table = "mythical-envoy-386309.majisemi.majisemi_followdata_addCode"

    # クエリ実行関数
    def run_query(query: str, _params=None):
        job_config = bigquery.QueryJobConfig()
        if _params:
            job_config.query_parameters = _params
        query_job = client.query(query, job_config=job_config)
        rows_raw = query_job.result()
        rows = [dict(row) for row in rows_raw]
        return rows

    # 検索ボックスの作成
    search_term = st.text_input("企業名を入力してください")

    if search_term:
        # 企業名で検索するクエリ
        search_query = f"""
        SELECT DISTINCT Company_Name, Seminar_Title
        FROM `{followdata_table}`
        WHERE Company_Name LIKE @search_term
        """
        
        query_params = [
            bigquery.ScalarQueryParameter("search_term", "STRING", f"%{search_term}%")
        ]
        
        search_results = run_query(search_query, query_params)
        
        if search_results:
            # 検索結果の表示
            companies = list(set([result['Company_Name'] for result in search_results]))
            selected_company = st.selectbox(
                "検索結果から企業を選択してください:", 
                companies
            )
            
            if selected_company:
                # 選択された企業のセミナータイトルを取得
                seminar_titles = [
                    result['Seminar_Title'] 
                    for result in search_results 
                    if result['Company_Name'] == selected_company
                ]
                
                # ワードクラウドの生成
                st.subheader(f"{selected_company}のセミナー参加傾向")
                
                def generate_wordcloud(text):
                    # Janomeで形態素解析
                    t = Tokenizer()
                    tokens = t.tokenize(text)
                    words = [token.surface for token in tokens if token.part_of_speech.split(',')[0] in ['名詞', '動詞']]
                    
                    # フィルタリング
                    words = [word for word in words if len(word) > 1]
                    words = [word for word in words if not re.match('^[ぁ-ん]{2}$', word)]
                    words = [word for word in words if not re.match('^[一-龠々]{1}[ぁ-ん]{1}$', word)]
                    
                    # 除外ワード
                    exclude_words = {
                        'ギフト', 'ギフトカード', 'サービス', 'できる', 'ランキング', '可能', 
                        '課題', '会員', '会社', '開始', '開発', '活用', '管理', '企業', '機能',
                        '記事', '技術', '業界', '後編', '公開', '最適', '支援', '事業', '実現', 
                        '重要', '世界', '成功', '製品', '戦略', '前編', '対策', '抽選', '調査', 
                        '提供', '投資', '導入', '発表', '必要', '方法', '目指す', '問題', '利用', 
                        '理由', 'する', '解説', '影響', '与える'
                    }
                    words = [word for word in words if word not in exclude_words]
                    
                    try:
                        # ワードクラウドの生成
                        wordcloud = WordCloud(
                            font_path='NotoSansJP-Regular.ttf',
                            background_color='white',
                            width=800,
                            height=400
                        ).generate(' '.join(words))
                        
                        fig, ax = plt.subplots(figsize=(10, 5))
                        ax.imshow(wordcloud, interpolation='bilinear')
                        ax.axis('off')
                        st.pyplot(fig)
                    except Exception as e:
                        st.error(f"ワードクラウドの生成中にエラーが発生しました: {str(e)}")

                # セミナータイトルを結合してワードクラウドを生成
                all_titles = ' '.join(seminar_titles)
                generate_wordcloud(all_titles)
                
                # セミナー参加履歴の表示（オプション）
                if st.checkbox("セミナー参加履歴を表示"):
                    st.write("参加セミナー一覧:")
                    for title in seminar_titles:
                        st.write(f"- {title}")
        else:
            st.warning("検索結果が見つかりませんでした。")

def main():
    if 'authenticated' not in st.session_state:
        st.session_state['authenticated'] = False

    if st.session_state['authenticated']:
        main_page()
    else:
        login_page()

if __name__ == "__main__":
    main()
