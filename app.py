import streamlit as st
from google.cloud import bigquery
from google.oauth2 import service_account
import matplotlib.pyplot as plt
import japanize_matplotlib
from wordcloud import WordCloud
from janome.tokenizer import Tokenizer
import re

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
def run_query(query):
    query_job = client.query(query)
    rows_raw = query_job.result()
    rows = [dict(row) for row in rows_raw]
    return rows

# ユーザーがキーワードを入力できるようにする
organizer_keyword = st.text_input("主催企業名キーワードを入力してください：", "Aiven")

# 現在の日付と過去3ヶ月の日付を取得
today = datetime.today()
three_months_ago = today - timedelta(days=365)

# マジセミ株式会社が主催したセミナーに参加した企業リストを取得するクエリ
attendee_query = f"""
SELECT DISTINCT Company_Name
FROM `{destination_table}`
WHERE Organizer_Name LIKE '%{organizer_keyword}%'
"""
# クエリ実行
attendee_data = run_query(attendee_query) # DataFrameに変換しない

# None値をフィルタリングして企業名のリストを生成
filtered_companies = [row['Company_Name'] for row in attendee_data if row['Company_Name'] is not None]
filtered_companies = list(set(filtered_companies)) # 重複を削除

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
all_seminars_data = run_query(all_seminars_query) # DataFrameに変換しない

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
