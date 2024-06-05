import streamlit as st
from google.cloud import bigquery
from google.oauth2 import service_account
import matplotlib.pyplot as plt
import japanize_matplotlib
from wordcloud import WordCloud
from janome.tokenizer import Tokenizer
import re
from datetime import datetime, timedelta
from st_aggrid import AgGrid, GridOptionsBuilder, GridUpdateMode
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

# --- 業種選択 ---
industries = [
    {"業種": "IT関連企業"},
    {"業種": "製造"},
    {"業種": "商社"},
    {"業種": "通信・キャリア・データセンター"},
    {"業種": "建設・土木・設備工事"},
    {"業種": "小売"},
    {"業種": "マーケティング・広告・出版・印刷"},
    {"業種": "金融"},
    {"業種": "不動産・ビル管理・セキュリティ"},
    {"業種": "教育"},
    {"業種": "医療・福祉・介護サービス"},
    {"業種": "運輸"},
    {"業種": "人材派遣・人材紹介"},
    {"業種": "その他"},
]
gb = GridOptionsBuilder.from_dataframe(pd.DataFrame(industries))
gb.configure_selection(selection_mode="multiple", use_checkbox=True)
grid_options_industries = gb.build()
st.subheader("業種を選択してください")
selected_rows_industries = AgGrid(
    pd.DataFrame(industries),
    gridOptions=grid_options_industries,
    update_mode=GridUpdateMode.SELECTION_CHANGED,
)
selected_industries = [row["業種"] for row in selected_rows_industries["data"]] # "data"を使用

# --- 従業員規模選択 ---
employee_sizes = [
    {"従業員規模": "5000人以上"},
    {"従業員規模": "1000人以上5000人未満"},
    {"従業員規模": "500人以上1000人未満"},
    {"従業員規模": "300人以上500人未満"},
    {"従業員規模": "100人以上300人未満"},
    {"従業員規模": "30人以上100人未満"},
    {"従業員規模": "10人以上30人未満"},
    {"従業員規模": "10人未満"},
    {"従業員規模": "分からない"},
]
gb = GridOptionsBuilder.from_dataframe(pd.DataFrame(employee_sizes))
gb.configure_selection(selection_mode="multiple", use_checkbox=True)
grid_options_employee_sizes = gb.build()
st.subheader("従業員規模を選択してください")
selected_rows_employee_sizes = AgGrid(
    pd.DataFrame(employee_sizes),
    gridOptions=grid_options_employee_sizes,
    update_mode=GridUpdateMode.SELECTION_CHANGED,
)
selected_employee_sizes = [row["従業員規模"] for row in selected_rows_employee_sizes["data"]] # "data"を使用

# --- 役職選択 ---
positions = [
    {"役職": "経営者・役員クラス"},
    {"役職": "事業部長/工場長クラス"},
    {"役職": "部長クラス"},
    {"役職": "課長クラス"},
    {"役職": "係長・主任クラス"},
    {"役職": "一般社員・職員クラス"},
]
gb = GridOptionsBuilder.from_dataframe(pd.DataFrame(positions))
gb.configure_selection(selection_mode="multiple", use_checkbox=True)
grid_options_positions = gb.build()
st.subheader("役職を選択してください")
selected_rows_positions = AgGrid(
    pd.DataFrame(positions),
    gridOptions=grid_options_positions,
    update_mode=GridUpdateMode.SELECTION_CHANGED,
)
selected_positions = [row["役職"] for row in selected_rows_positions["data"]] # "data"を使用

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
        industry_conditions = " OR ".join([f"Industry = '{industry}'" for industry in selected_industries])
        where_clauses.append(f"({industry_conditions})")
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

    # None値をフィルタリングして企業名のリストを生成
    filtered_companies = [row['Company_Name'] for row in attendee_data if row['Company_Name'] is not None]
    filtered_companies = list(set(filtered_companies))  # 重複を削除

    # IN句が空の場合の処理を追加
    if filtered_companies:
        # クォートされた企業名のリストを生成
        quoted_companies = ", ".join([f"'{company}'" for company in filtered_companies])

        # 過去3ヶ月間のセミナーのみを対象とするSQLクエリ
        all_seminars_query = f"""
        SELECT *
        FROM `{destination_table}`
        WHERE Company_Name IN ({quoted_companies})
        AND Seminar_Date >= '{three_months_ago.strftime('%Y-%m-%d')}'
        ORDER BY Company_Name, Seminar_Date
        """

        # クエリ実行
        all_seminars_data = run_query(all_seminars_query)

        # スコア計算関数
        def calculate_score(row):
            score = 0
            if row['Status'] == '出席':
                score += 3
            if any(row.get(f'Post_Seminar_Survey_Answer_{i}', '') for i in range(1, 4)):
                score += 3
            if row.get('Desired_Follow_Up_Actions') is not None:
                if '製品やサービス導入に関する具体的な要望がある' in row['Desired_Follow_Up_Actions']:
                    score += 5
                elif '資料希望' in row['Desired_Follow_Up_Actions']:
                    score += 3
            if row.get('Pre_Seminar_Survey_Answer_2') == '既に同様の商品・サービスを導入済み':
                score += 3
            elif row.get('Pre_Seminar_Survey_Answer_2') == '既に候補の製品・サービスを絞っており、その評価・選定をしている':
                score += 3
            elif row.get('Pre_Seminar_Survey_Answer_2') == '製品・サービスの候補を探している':
                score += 2
            elif row.get('Pre_Seminar_Survey_Answer_2') == '導入するかどうか社内で検討中（課題の確認、情報収集、要件の整理、予算の検討）':
                score += 1
            return score

        # スコアを計算し、辞書に格納
        company_scores = {}
        for row in all_seminars_data:
            company_name = row['Company_Name']
            score = calculate_score(row)
            if company_name in company_scores:
                company_scores[company_name] += score
            else:
                company_scores[company_name] = score

        # スコアの高い順にソート
        sorted_scores = sorted(company_scores.items(), key=lambda item: item[1], reverse=True)

        # トップ3企業を表示
        st.header("トップ3企業:")
        for i in range(3):
            company_name, score = sorted_scores[i]
            st.write(f"{i+1}. {company_name}: {score}点")

        # 形態素解析とワードクラウド生成
        def generate_wordcloud(font_path, text, title):
            t = Tokenizer()
            tokens = t.tokenize(text)
            words = [token.surface for token in tokens if token.part_of_speech.split(',')[0] in ['名詞', '動詞']]

            # フィルタリング条件
            words = [word for word in words if len(word) > 1]
            words = [word for word in words if not re.match('^[ぁ-ん]{2}$', word)]
            words = [word for word in words if not re.match('^[一-龠々]{1}[ぁ-ん]{1}$', word)]
            
            # キーワードの除外
            exclude_words = {'ギフト', 'ギフトカード', 'サービス', 'できる', 'ランキング', '可能', '課題', '会員', '会社', '開始', '開発', '活用', '管理', '企業', '機能',
                            '記事', '技術', '業界', '後編', '公開', '最適', '支援', '事業', '実現', '重要', '世界', '成功', '製品', '戦略', '前編', '対策', '抽選', '調査',
                            '提供', '投資', '導入', '発表', '必要', '方法', '目指す', '問題', '利用', '理由', 'する', '解説', '影響', '与える'}
            words = [word for word in words if word not in exclude_words]
            
            # ワードクラウドの生成
            wordcloud = WordCloud(font_path=font_path, background_color='white', width=800, height=400).generate(' '.join(words))

            # ワードクラウドの表示
            fig, ax = plt.subplots(figsize=(10, 5))
            ax.imshow(wordcloud, interpolation='bilinear')
            ax.set_title(title)
            ax.axis('off')
            st.pyplot(fig)

        # トップ3企業のセミナータイトルからワードクラウドを作成
        st.header("セミナータイトルワードクラウド")
        for i in range(3):
            company_name, _ = sorted_scores[i]
            # 企業ごとのセミナータイトルを取得
            seminar_titles = ' '.join([row['Seminar_Title'] for row in all_seminars_data if row['Company_Name'] == company_name])
            # ワードクラウドを生成
            generate_wordcloud('NotoSansJP-Regular.ttf', seminar_titles, f'{company_name}のセミナータイトルワードクラウド')

    else:
        st.warning("キーワードに一致する企業が見つかりませんでした。")
