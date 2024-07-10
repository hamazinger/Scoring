import streamlit as st
from google.cloud import bigquery
from google.oauth2 import service_account
import pandas as pd
from datetime import datetime, timedelta
from wordcloud import WordCloud
import matplotlib.pyplot as plt
from janome.tokenizer import Tokenizer
import re

# Streamlitアプリのタイトルを設定
st.title("リードスコアリング")

# 認証情報の設定
try:
    service_account_info = st.secrets["gcp_service_account"]
    credentials = service_account.Credentials.from_service_account_info(service_account_info)
    project_id = service_account_info["project_id"]
    client = bigquery.Client(credentials=credentials, project=project_id)
except KeyError:
    st.error("GCPの認証情報が見つかりません。StreamlitのSecretsに設定してください。")
    st.stop()

# テーブル名を完全修飾名で指定
followdata_table = "mythical-envoy-386309.majisemi.majisemi_followdata"

# BigQueryからデータを取得する関数
@st.cache_data(ttl=600)
def run_query(query: str, _params=None):
    job_config = bigquery.QueryJobConfig()
    if _params:
        job_config.query_parameters = _params
    query_job = client.query(query, job_config=job_config)
    rows_raw = query_job.result()
    rows = [dict(row) for row in rows_raw]
    return rows

# デバッグ用：クエリとパラメータを表示する関数
def show_query_and_params(query, params):
    st.text("実行されたクエリ:")
    st.code(query)
    st.text("クエリパラメータ:")
    st.json({p.name: p.value for p in params})

# ユーザーがキーワードを入力できるようにする
organizer_keyword = st.text_input("主催企業名を入力してください：", "")

# カテゴリ選択を横に3つ並べる
col1, col2, col3 = st.columns(3)

# --- 業種選択 ---
with col1:
    st.subheader("業種")
    industries = [
        "製造", "通信キャリア・データセンター", "商社", "小売", "金融",
        "建設・土木・設備工事", "マーケティング・広告・出版・印刷", "教育", "IT関連企業"
    ]
    selected_industries = st.multiselect("業種を選択してください", industries)

# --- 従業員規模選択 ---
with col2:
    st.subheader("従業員規模")
    employee_sizes = [
        "5000人以上", "1000人以上5000人未満", "500人以上1000人未満",
        "300人以上500人未満", "100人以上300人未満", "30人以上100人未満",
        "10人以上30人未満", "10人未満"
    ]
    selected_employee_sizes = st.multiselect("従業員規模を選択してください", employee_sizes)

# --- 役職選択 ---
with col3:
    st.subheader("役職")
    positions = [
        "経営者・役員クラス", "事業部長/工場長クラス", "部長クラス",
        "課長クラス", "係長・主任クラス", "一般社員・職員クラス"
    ]
    selected_positions = st.multiselect("役職を選択してください", positions)

# 実行ボタンを追加
execute_button = st.button("実行")

# ボタンが押された場合のみ処理を実行
if execute_button:
    # 3年前の日付を計算
    today = datetime.today()
    three_years_ago = today - timedelta(days=365*3)

    organizer_keyword_with_wildcard = f"%{organizer_keyword}%"
    organizer_keyword_full_width = "％" + "".join([chr(ord(c) + 65248) if ord(c) < 128 else c for c in organizer_keyword]) + "％"

    query_parameters = [
        bigquery.ScalarQueryParameter("organizer_keyword", "STRING", organizer_keyword_with_wildcard),
        bigquery.ScalarQueryParameter("organizer_keyword_full", "STRING", organizer_keyword_full_width)
    ]

    # クエリを修正
    attendee_query = f"""
    SELECT DISTINCT
        Company_Name, Organizer_Name
    FROM
        `{followdata_table}`
    WHERE (LOWER(Organizer_Name) LIKE LOWER(@organizer_keyword)
           OR LOWER(Organizer_Name) LIKE LOWER(@organizer_keyword_full))
    """

    if selected_industries:
        industry_conditions = " OR ".join([f"User_Company LIKE '%' || @industry_{i} || '%'" for i in range(len(selected_industries))])
        attendee_query += f" AND ({industry_conditions})"
        query_parameters.extend([bigquery.ScalarQueryParameter(f"industry_{i}", "STRING", industry) for i, industry in enumerate(selected_industries)])

    if selected_employee_sizes:
        employee_size_conditions = " OR ".join([f"Employee_Size LIKE '%' || @employee_size_{i} || '%'" for i in range(len(selected_employee_sizes))])
        attendee_query += f" AND ({employee_size_conditions})"
        query_parameters.extend([bigquery.ScalarQueryParameter(f"employee_size_{i}", "STRING", size) for i, size in enumerate(selected_employee_sizes)])

    if selected_positions:
        position_conditions = " OR ".join([f"Position_Category LIKE '%' || @position_{i} || '%'" for i in range(len(selected_positions))])
        attendee_query += f" AND ({position_conditions})"
        query_parameters.extend([bigquery.ScalarQueryParameter(f"position_{i}", "STRING", position) for i, position in enumerate(selected_positions)])

    # デバッグ: クエリとパラメータを表示
    show_query_and_params(attendee_query, query_parameters)

    try:
        attendee_data = run_query(attendee_query, query_parameters)
        
        # デバッグ: クエリの結果を表示
        st.write("クエリ結果:", attendee_data)

        # attendee_dataから会社名リストを作成
        filtered_companies = [row['Company_Name'] for row in attendee_data if row.get('Company_Name')]
        filtered_companies = list(set(filtered_companies))  # 重複を削除

        # フィルタリング後の企業リストを確認
        if filtered_companies:
            st.write(f"フィルタリング後の企業数: {len(filtered_companies)}")
            st.write("フィルタリング後の企業:", filtered_companies[:10])  # 最初の10社のみ表示
        else:
            st.warning("フィルタリング後の企業が見つかりませんでした。")
            st.stop()

        # 会社名をエスケープする関数
        def escape_company_name(name):
            return name.replace("'", "''")

        # エスケープした会社名のリストを作成
        escaped_companies = [escape_company_name(company) for company in filtered_companies]

        # IN句の代わりにUNNESTを使用
        all_seminars_query = f"""
        SELECT *
        FROM `{followdata_table}`
        WHERE Company_Name IN UNNEST(@companies)
        AND Seminar_Date >= @three_years_ago
        ORDER BY Company_Name, Seminar_Date
        """

        query_params = [
            bigquery.ArrayQueryParameter("companies", "STRING", escaped_companies),
            bigquery.ScalarQueryParameter("three_years_ago", "DATE", three_years_ago.date())
        ]

        # デバッグ: all_seminars_queryとquery_paramsを表示
        st.write("all_seminars_query:", all_seminars_query)
        st.write("query_params:", query_params)

        # テーブルの内容をサンプリングするクエリ
        sample_query = f"""
        SELECT *
        FROM `{followdata_table}`
        LIMIT 10
        """

        try:
            sample_data = run_query(sample_query)
            st.write("テーブルのサンプルデータ:", sample_data)
        except Exception as e:
            st.error(f"サンプルデータの取得に失敗しました: {str(e)}")

        try:
            all_seminars_data = run_query(all_seminars_query, query_params)
        except Exception as e:
            st.error(f"BigQueryのクエリに失敗しました: {str(e)}")
            st.error(f"クエリ: {all_seminars_query}")
            st.error(f"パラメータ: {query_params}")
            st.stop()

        # all_seminars_dataの内容を確認
        st.write("all_seminars_dataの長さ:", len(all_seminars_data))
        st.write("all_seminars_dataの最初の5件:", all_seminars_data[:5])

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
                elif '導入するかどうか社内で検討中（課題の確認、情報収集、要件の整理、予算の検討）' in row['Pre_Seminar_Survey_Answer_2']:
                    score += 1
            return score

        # スコア計算のデバッグ出力
        st.write("スコア計算のデバッグ:")
        company_scores = {}
        # all_seminars_data全体をループするように修正
        for i, row in enumerate(all_seminars_data):
            score = calculate_score(row)
            st.write(f"企業: {row['Company_Name']}, スコア: {score}")
            if i == 0:
                st.write("スコア計算の詳細:", row)
            
            company_name = row['Company_Name']
            if company_name in company_scores:
                company_scores[company_name] += score
            else:
                company_scores[company_name] = score

        # ソート後のスコアを確認
        sorted_scores = sorted(company_scores.items(), key=lambda item: item[1], reverse=True)
        if sorted_scores:
            st.write(f"ソート後の企業数: {len(sorted_scores)}")
            st.write("上位10社のスコア:", sorted_scores[:10])
        else:
            st.warning("スコア計算後の企業が見つかりませんでした。")
            st.stop()

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
                             '記事', '技術', '業界', '後編', '公開', '最適', '支援', '事業', '実現', '重要', '世界', '成功', '製品', '戦略', '前編', '対策', '抽選', '調査', '提供', '投資', '導入', '発表', '必要', '方法', '目指す', '問題', '利用', '理由', 'する', '解説', '影響', '与える'}
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
            if seminar_titles:
                try:
                    generate_wordcloud('NotoSansJP-Regular.ttf', seminar_titles, f'{company_name}のセミナータイトルワードクラウド')
                except Exception as e:
                    st.error(f"ワードクラウドの生成中にエラーが発生しました: {str(e)}")
            else:
                st.warning(f"{company_name}のセミナータイトルが見つかりませんでした。")

    except Exception as e:
        st.error(f"BigQueryのクエリに失敗しました: {str(e)}")
        st.stop()
