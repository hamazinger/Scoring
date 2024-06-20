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
def run_query(query: str, params=None):
    # ScalarQueryParameterオブジェクトのリストを作成
    if params:
        query_params = [bigquery.ScalarQueryParameter(None, "STRING", param) for param in params]
    else:
        query_params = None

    # job_configに渡す前に、ScalarQueryParameterオブジェクトをAPI表現に変換
    job_config = bigquery.QueryJobConfig(query_parameters=[param.to_api_repr() for param in query_params] if query_params else None)
    query_job = client.query(query, job_config=job_config)
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
        {"User_Company": "1. 製造"},
        {"User_Company": "2. 通信キャリア・データセンター"},
        {"User_Company": "3. 商社"},
        {"User_Company": "4. 小売"},
        {"User_Company": "5. 金融"},
        {"User_Company": "6. 建設・土木・設備工事"},
        {"User_Company": "7. マーケティング・広告・出版・印刷"},
        {"User_Company": "8. 教育"},
        # {"User_Company": "9. その他"}, # 不要な選択肢を削除
        {"User_Company": "10. システム・インテグレータ"},
        {"User_Company": "11. IT・ビジネスコンサルティング"},
        {"User_Company": "12. IT関連製品販売"},
        {"User_Company": "13. IT関連製品販売"},
        {"User_Company": "14. SaaS・Webサービス事業"},
        {"User_Company": "15. その他ITサービス関連"},
    ]
    gb = GridOptionsBuilder.from_dataframe(pd.DataFrame(industries))
    gb.configure_selection(selection_mode="multiple", use_checkbox=True, pre_selected_rows=list(range(len(industries))))
    gb.configure_grid_options(domLayout='normal', headerHeight=0) # headerHeight=0 でヘッダーを非表示にする
    grid_options_industries = gb.build()
    st.subheader("業種")
    selected_rows_industries = AgGrid(
        pd.DataFrame(industries),
        gridOptions=grid_options_industries,
        data_return_mode=DataReturnMode.FILTERED_AND_SORTED,
        update_mode=GridUpdateMode.MODEL_CHANGED,
        theme='streamlit',
        fit_columns_on_grid_load=True,
        height=350,
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
        # {"Employee_Size": "分からない"}, # 不要な選択肢を削除
    ]
    gb = GridOptionsBuilder.from_dataframe(pd.DataFrame(employee_sizes))
    gb.configure_selection(selection_mode="multiple", use_checkbox=True, pre_selected_rows=list(range(len(employee_sizes))))
    gb.configure_grid_options(domLayout='normal', headerHeight=0) # headerHeight=0 でヘッダーを非表示にする
    grid_options_employee_sizes = gb.build()
    st.subheader("従業員規模") 
    selected_rows_employee_sizes = AgGrid(
        pd.DataFrame(employee_sizes),
        gridOptions=grid_options_employee_sizes,
        data_return_mode=DataReturnMode.FILTERED_AND_SORTED,
        update_mode=GridUpdateMode.MODEL_CHANGED,
        theme='streamlit',
        fit_columns_on_grid_load=True,
        height=350,
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
        # {"Position_Category": "7. その他"}, # 不要な選択肢を削除
    ]
    gb = GridOptionsBuilder.from_dataframe(pd.DataFrame(positions))
    gb.configure_selection(selection_mode="multiple", use_checkbox=True, pre_selected_rows=list(range(len(positions))))
    gb.configure_grid_options(domLayout='normal', headerHeight=0) # headerHeight=0 でヘッダーを非表示にする
    grid_options_positions = gb.build()
    st.subheader("役職") 
    selected_rows_positions = AgGrid(
        pd.DataFrame(positions),
        gridOptions=grid_options_positions,
        data_return_mode=DataReturnMode.FILTERED_AND_SORTED,
        update_mode=GridUpdateMode.MODEL_CHANGED,
        theme='streamlit',
        fit_columns_on_grid_load=True,
        height=350,
    )
    selected_positions = selected_rows_positions["data"]["Position_Category"].tolist()

# 実行ボタンを追加
execute_button = st.button("実行")

# ボタンが押された場合のみ処理を実行
if execute_button:
    # 現在の日付と過去3ヶ月の日付を取得
    today = datetime.today()
    three_months_ago = today - timedelta(days=90)

    # 選択された項目に基づいてクエリを変更
    where_clauses = []
    query_parameters = []

    if selected_industries:
        industry_conditions = " OR ".join([f"User_Company = @industry_{i}" for i in range(len(selected_industries))])
        where_clauses.append(f"({industry_conditions})")
        query_parameters += [
            bigquery.ScalarQueryParameter(f"industry_{i}", "STRING", industry)
            for i, industry in enumerate(selected_industries)
        ]

    if selected_employee_sizes:
        employee_size_conditions = " OR ".join([f"Employee_Size = @employee_size_{i}" for i in range(len(selected_employee_sizes))])
        where_clauses.append(f"({employee_size_conditions})")
        query_parameters += [
            bigquery.ScalarQueryParameter(f"employee_size_{i}", "STRING", size)
            for i, size in enumerate(selected_employee_sizes)
        ]

    if selected_positions:
        position_conditions = " OR ".join([f"Position_Category = @position_{i}" for i in range(len(selected_positions))])
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
    filtered_companies = list(set(filtered_companies))  # 重複を削除

    if filtered_companies:
        # クォートされた企業名のリストを生成（シングルクォートをエスケープ）
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
            from wordcloud import WordCloud
            from janome.tokenizer import Tokenizer
            import re
            import matplotlib.pyplot as plt

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
