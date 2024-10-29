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

def login_page():
    if "login_checked" not in st.session_state:
        st.session_state.login_checked = False

    if not st.session_state.login_checked:
        col1, col2, col3 = st.columns([1, 2, 1])

        with col2:
            title_placeholder = st.empty()
            title_placeholder.title("Intent Analytics")
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
    st.title("Intent Analytics")

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

    # 検索ボックス
    search_term = st.text_input("検索キーワードを入力してください")

    if search_term:
        # キーワードに部分一致する企業のセミナータイトルを取得
        search_query = f"""
        SELECT Seminar_Title
        FROM `{followdata_table}`
        WHERE Company_Name LIKE @search_term
        """
        
        query_params = [
            bigquery.ScalarQueryParameter("search_term", "STRING", f"%{search_term}%")
        ]
        
        search_results = run_query(search_query, query_params)
        
        if search_results:
            # ワードクラウド生成
            def generate_wordcloud(titles):
                # Janomeで形態素解析
                t = Tokenizer()
                text = ' '.join(titles)
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
                
                if not words:
                    return None

                try:
                    wordcloud = WordCloud(
                        font_path='NotoSansJP-Regular.ttf',
                        background_color='white',
                        width=800,
                        height=400
                    ).generate(' '.join(words))
                    
                    fig, ax = plt.subplots(figsize=(10, 5))
                    ax.imshow(wordcloud, interpolation='bilinear')
                    ax.axis('off')
                    return fig
                except Exception as e:
                    st.error(f"ワードクラウドの生成中にエラーが発生しました: {str(e)}")
                    return None

            # セミナータイトルのリストを作成
            seminar_titles = [result['Seminar_Title'] for result in search_results]
            
            # # 検索結果件数を表示
            # st.write(f"検索結果: {len(seminar_titles)}件")
            
            # ワードクラウドの生成と表示
            fig = generate_wordcloud(seminar_titles)
            if fig:
                st.pyplot(fig)
            else:
                st.warning("ワードクラウドを生成できる有効な単語が見つかりませんでした。")
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
