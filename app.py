import streamlit as st
from google.cloud import bigquery, storage
from google.oauth2 import service_account
import pandas as pd
import matplotlib.pyplot as plt
import japanize_matplotlib
from wordcloud import WordCloud
from janome.tokenizer import Tokenizer
import re
from datetime import datetime, timedelta
from st_aggrid import AgGrid, GridOptionsBuilder, GridUpdateMode, DataReturnMode
import codecs

# Streamlitアプリのタイトルを設定
st.title("マジセミリードスコアリング＆ワードクラウド")

# 認証情報の設定
service_account_info = st.secrets["gcp_service_account"]
credentials = service_account.Credentials.from_service_account_info(service_account_info)
project_id = service_account_info["project_id"]
client = bigquery.Client(credentials=credentials, project=project_id)

# BigQueryからデータを取得する関数
@st.cache_data(ttl=600)
def run_query(query: str, params=None):
    if params:
        job_config = bigquery.QueryJobConfig(query_parameters=params)
    else:
        job_config = bigquery.QueryJobConfig()
    query_job = client.query(query, job_config=job_config)
    rows_raw = query_job.result()
    rows = [dict(row) for row in rows_raw]
    return rows

# GCSからファイルをダウンロードして、Pandasデータフレームに読み込む関数
def download_blob_to_dataframe(bucket_name, source_blob_name, destination_file_name, credentials):
    storage_client = storage.Client(credentials=credentials)
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(source_blob_name)
    blob.download_to_filename(destination_file_name)

    with codecs.open(destination_file_name, "r", "Shift-JIS", "ignore") as file:
        df = pd.read_table(file, delimiter=",")
    return df

# GCSからデータを取得
year = 2024
month = 5
bucket_name = 'jib-z62k-tpr5bhb_e'
source_blob_name = f'{year}_{month}_advs.csv'
destination_file_name = f'{year}_{month}_advs.csv'

df = download_blob_to_dataframe(bucket_name, source_blob_name, destination_file_name, credentials)

# データの列名を変換
def column_rename_natural(df):
    df['従業員数（企業規模）'] = df['従業員数（企業規模）'].replace('5000人以上','1. 5000人以上')
    # ... 他の変換もここで行う
    return df

df = column_rename_natural(df)

# Streamlit UIの設定
organizer_keyword = st.text_input("主催企業名キーワードを入力してください：", "")

# 業種選択
with st.columns(3)[0]:
    industries = df['業種'].unique().tolist()
    selected_industries = st.multiselect("業種を選択", industries)

# 従業員規模選択
with st.columns(3)[1]:
    employee_sizes = df['従業員数（企業規模）'].unique().tolist()
    selected_employee_sizes = st.multiselect("従業員規模を選択", employee_sizes)

# 役職選択
with st.columns(3)[2]:
    positions = df['役職区分'].unique().tolist()
    selected_positions = st.multiselect("役職を選択", positions)

# 実行ボタン
execute_button = st.button("実行")

if execute_button:
    today = datetime.today()
    three_months_ago = today - timedelta(days=90)

    where_clauses = []
    query_parameters = []

    if selected_industries:
        industry_conditions = " OR ".join([f"業種 = @industry_{i}" for i in range(len(selected_industries))])
        where_clauses.append(f"({industry_conditions})")
        query_parameters += [
            bigquery.ScalarQueryParameter(f"industry_{i}", "STRING", industry)
            for i, industry in enumerate(selected_industries)
        ]

    if selected_employee_sizes:
        employee_size_conditions = " OR ".join([f"従業員数（企業規模） = @employee_size_{i}" for i in range(len(selected_employee_sizes))])
        where_clauses.append(f"({employee_size_conditions})")
        query_parameters += [
            bigquery.ScalarQueryParameter(f"employee_size_{i}", "STRING", size)
            for i, size in enumerate(selected_employee_sizes)
        ]

    if selected_positions:
        position_conditions = " OR ".join([f"役職区分 = @position_{i}" for i in range(len(selected_positions))])
        where_clauses.append(f"({position_conditions})")
        query_parameters += [
            bigquery.ScalarQueryParameter(f"position_{i}", "STRING", position)
            for i, position in enumerate(selected_positions)
        ]

    if where_clauses:
        where_clause = " AND ".join(where_clauses)
        attendee_query = f"""
        SELECT DISTINCT Company_Name
        FROM `{destination_table}`
        WHERE Organizer_Name LIKE @organizer_keyword AND {where_clause}
        """
        query_parameters.append(bigquery.ScalarQueryParameter("organizer_keyword", "STRING", f"%{organizer_keyword}%"))

        try:
            attendee_data = run_query(attendee_query, query_parameters)
        except Exception as e:
            st.error(f"BigQueryのクエリに失敗しました: {e}")
            st.stop()
    else:
        st.warning("業種、従業員規模、役職のいずれかを選択してください。")
        attendee_data = []

    filtered_companies = [row['Company_Name'] for row in attendee_data if row['Company_Name'] is not None]
    filtered_companies = list(set(filtered_companies))

    if filtered_companies:
        quoted_companies = ", ".join(["'{}'".format(company.replace("'", "''")) for company in filtered_companies])

        all_seminars_query = f"""
        SELECT *
        FROM `{destination_table}`
        WHERE Company_Name IN ({quoted_companies})
        AND Seminar_Date >= @three_months_ago
        ORDER BY Company_Name, Seminar_Date
        """

        try:
            all_seminars_data = run_query(all_seminars_query, [bigquery.ScalarQueryParameter("three_months_ago", "DATE", three_months_ago.strftime('%Y-%m-%d'))])
        except Exception as e:
            st.error(f"BigQueryのクエリに失敗しました: {e}")
            st.stop()

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

        company_scores = {}
        for row in all_seminars_data:
            company_name = row['Company_Name']
            score = calculate_score(row)
            if company_name in company_scores:
                company_scores[company_name] += score
            else:
                company_scores[company_name] = score

        sorted_scores = sorted(company_scores.items(), key=lambda item: item[1], reverse=True)

        st.header("トップ3企業:")
        for i in range(min(3, len(sorted_scores))):
            company_name, score = sorted_scores[i]
            st.write(f"{i + 1}. {company_name}: {score}点")

        def generate_wordcloud(font_path, text, title):
            t = Tokenizer()
            tokens = t.tokenize(text)
            words = [token.surface for token in tokens if token.part_of_speech.split(',')[0] in ['名詞', '動詞']]

            words = [word for word in words if len(word) > 1]
            words = [word for word in words if not re.match('^[ぁ-ん]{2}$', word)]
            words = [word for word in words if not re.match('^[一-龠々]{1}[ぁ-ん]{1}$', word)]

            exclude_words = {'ギフト', 'ギフトカード', 'サービス', 'できる', 'ランキング', '可能', '課題', '会員', '会社', '開始', '開発', '活用', '管理', '企業', '機能',
                             '記事', '技術', '業界', '後編', '公開', '最適', '支援', '事業', '実現', '重要', '世界', '成功', '製品', '戦略', '前編', '対策', '抽選', '調査',
                             '提供', '投資', '導入', '発表', '必要', '方法', '目指す', '問題', '利用', '理由', 'する', '解説', '影響', '与える'}
            words = [word for word in words if word not in exclude_words]

            wordcloud = WordCloud(font_path=font_path, background_color='white', width=800, height=400).generate(' '.join(words))

            fig, ax = plt.subplots(figsize=(10, 5))
            ax.imshow(wordcloud, interpolation='bilinear')
            ax.set_title(title)
            ax.axis('off')
            st.pyplot(fig)

        st.header("セミナータイトルワードクラウド")
        for i in range(min(3, len(sorted_scores))):
            company_name, _ = sorted_scores[i]
            seminar_titles = ' '.join([row['Seminar_Title'] for row in all_seminars_data if row['Company_Name'] == company_name])
            generate_wordcloud('NotoSansJP-Regular.ttf', seminar_titles, f'{company_name}のセミナータイトルワードクラウド')

    else:
        st.warning("キーワードに一致する企業が見つかりませんでした。")
