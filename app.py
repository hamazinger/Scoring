import streamlit as st
from google.cloud import bigquery
from google.oauth2 import service_account
import pandas as pd
from datetime import datetime, timedelta
from st_aggrid import AgGrid, GridOptionsBuilder, GridUpdateMode, DataReturnMode
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

destination_table = "mythical-envoy-386309.majisemi.majisemi_followdata"

# BigQueryからデータを取得する関数
@st.cache_data(ttl=600)
def run_query(query: str, _params=None):
    query_job = client.query(query, job_config=bigquery.QueryJobConfig(query_parameters=_params))
    rows_raw = query_job.result()
    rows = [dict(row) for row in rows_raw]
    return rows

# ユーザーがキーワードを入力できるようにする
organizer_keyword = st.text_input("主催企業名を入力してください：", "")  # 初期値を空にする

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
        {"User_Company": "IT関連企業"},
    ]
    gb = GridOptionsBuilder.from_dataframe(pd.DataFrame(industries))
    gb.configure_selection(selection_mode="multiple", use_checkbox=True)
    gb.configure_grid_options(domLayout='normal', headerHeight=0)  # ヘッダーを非表示にする
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
    selected_industries = [row["User_Company"] for row in selected_rows_industries["data"].to_dict('records') if row.get("_selected_", False)]

# --- 従業員規模選択 ---
with col2:
    employee_sizes = [
        {"Employee_Size": "5000人以上"},
        {"Employee_Size": "1000人以上5000人未満"},
        {"Employee_Size": "500人以上1000人未満"},
        {"Employee_Size": "300人以上500人未満"},
        {"Employee_Size": "100人以上300人未満"},
        {"Employee_Size": "30人以上100人未満"},
        {"Employee_Size": "10人以上30人未満"},
        {"Employee_Size": "10人未満"},
    ]
    gb = GridOptionsBuilder.from_dataframe(pd.DataFrame(employee_sizes))
    gb.configure_selection(selection_mode="multiple", use_checkbox=True)
    gb.configure_grid_options(domLayout='normal', headerHeight=0)  # ヘッダーを非表示にする
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
    selected_employee_sizes = [row["Employee_Size"] for row in selected_rows_employee_sizes["data"].to_dict('records') if row.get("_selected_", False)]

# --- 役職選択 ---
with col3:
    positions = [
        {"Position_Category": "経営者・役員クラス"},
        {"Position_Category": "事業部長/工場長クラス"},
        {"Position_Category": "部長クラス"},
        {"Position_Category": "課長クラス"},
        {"Position_Category": "係長・主任クラス"},
        {"Position_Category": "一般社員・職員クラス"},
    ]
    gb = GridOptionsBuilder.from_dataframe(pd.DataFrame(positions))
    gb.configure_selection(selection_mode="multiple", use_checkbox=True)
    gb.configure_grid_options(domLayout='normal', headerHeight=0)  # ヘッダーを非表示にする
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
    selected_positions = [row["Position_Category"] for row in selected_rows_positions["data"].to_dict('records') if row.get("_selected_", False)]

# 実行ボタンを追加
execute_button = st.button("実行")

# ボタンが押された場合のみ処理を実行
if execute_button:
    today = datetime.today()
    three_months_ago = today - timedelta(days=90)

    organizer_keyword_with_wildcard = f"%{organizer_keyword}%"

    where_clauses = []
    query_parameters = []

    if selected_industries:
        industry_conditions = " OR ".join([f"User_Company = @industry_{i}" for i in range(len(selected_industries))])
        where_clauses.append(f"({industry_conditions})")
        query_parameters.extend([bigquery.ScalarQueryParameter(f"industry_{i}", "STRING", industry) for i, industry in enumerate(selected_industries)])

    if selected_employee_sizes:
        employee_size_conditions = " OR ".join([f"Employee_Size = @employee_size_{i}" for i in range(len(selected_employee_sizes))])
        where_clauses.append(f"({employee_size_conditions})")
        query_parameters.extend([bigquery.ScalarQueryParameter(f"employee_size_{i}", "STRING", size) for i, size in enumerate(selected_employee_sizes)])

    if selected_positions:
        position_conditions = " OR ".join([f"Position_Category = @position_{i}" for i in range(len(selected_positions))])
        where_clauses.append(f"({position_conditions})")
        query_parameters.extend([bigquery.ScalarQueryParameter(f"position_{i}", "STRING", position) for i, position in enumerate(selected_positions)])

    query_parameters.append(bigquery.ScalarQueryParameter("organizer_keyword", "STRING", organizer_keyword_with_wildcard))

    attendee_query = f"""
    SELECT DISTINCT Company_Name
    FROM `{destination_table}`
    WHERE Organizer_Name LIKE @organizer_keyword
    """

    if where_clauses:
        attendee_query += f" AND {' AND '.join(where_clauses)}"

    # デバッグ情報の表示
    st.write("デバッグ: 選択された業種", selected_industries)
    st.write("デバッグ: 選択された従業員規模", selected_employee_sizes)
    st.write("デバッグ: 選択された役職", selected_positions)
    st.write("デバッグ: 主催企業キーワード", organizer_keyword)
    st.write("デバッグ: 生成されたクエリ", attendee_query)
    st.write("デバッグ: クエリパラメータ", query_parameters)

    try:
        attendee_data = run_query(attendee_query, query_parameters)
        st.write("デバッグ: attendee_data", attendee_data)
    except Exception as e:
        st.error(f"BigQueryのクエリに失敗しました: {e}")
        st.stop()

    filtered_companies = [row['Company_Name'] for row in attendee_data if row.get('Company_Name')]
    filtered_companies = list(set(filtered_companies))  # 重複を削除

    st.write("デバッグ: フィルタリング前の企業数", len(attendee_data))
    st.write("デバッグ: フィルタリング後の企業数", len(filtered_companies))
    st.write("デバッグ: フィルタリング後の企業", filtered_companies)

    if filtered_companies:
        # 会社名をエスケープする関数
        def escape_company_name(name):
            return name.replace("'", "''")

        # エスケープした会社名のリストを作成
        escaped_companies = [escape_company_name(company) for company in filtered_companies]

        # IN句の代わりにUNNESTを使用
        all_seminars_query = f"""
        SELECT *
        FROM `{destination_table}`
        WHERE Company_Name IN UNNEST(@companies)
        AND Seminar_Date >= @three_months_ago
        ORDER BY Company_Name, Seminar_Date
        """

        query_params = [
            bigquery.ArrayQueryParameter("companies", "STRING", escaped_companies),
            bigquery.ScalarQueryParameter("three_months_ago", "DATE", three_months_ago.strftime('%Y-%m-%d'))
        ]

        try:
            all_seminars_data = run_query(all_seminars_query, query_params)
        except Exception as e:
            st.error(f"BigQueryのクエリに失敗しました: {e}")
            st.stop()

        # デバッグ情報の追加
        st.write("デバッグ: all_seminars_query", all_seminars_query)
        st.write("デバッグ: query_params", query_params)

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

    # デバッグ情報の追加表示
    st.subheader("デバッグ情報")
    st.write("クエリ実行結果:")
    st.json(attendee_data)
    
    st.write("選択された条件:")
    st.write(f"業種: {selected_industries}")
    st.write(f"従業員規模: {selected_employee_sizes}")
    st.write(f"役職: {selected_positions}")
    st.write(f"主催企業キーワード: {organizer_keyword}")

    # データベースの内容サンプルを表示
    sample_query = f"""
    SELECT *
    FROM `{destination_table}`
    LIMIT 10
    """
    sample_data = run_query(sample_query)
    st.write("データベースのサンプルデータ:")
    st.dataframe(sample_data)

    # 条件に一致するレコードの数を確認
    count_query = f"""
    SELECT COUNT(*) as count
    FROM `{destination_table}`
    WHERE Organizer_Name LIKE @organizer_keyword
    """
    count_data = run_query(count_query, [bigquery.ScalarQueryParameter("organizer_keyword", "STRING", organizer_keyword_with_wildcard)])
    st.write(f"主催企業名に '{organizer_keyword}' を含むレコード数: {count_data[0]['count']}")

    # 各条件ごとのレコード数を確認
    for condition in ['User_Company', 'Employee_Size', 'Position_Category']:
        condition_query = f"""
        SELECT {condition}, COUNT(*) as count
        FROM `{destination_table}`
        WHERE Organizer_Name LIKE @organizer_keyword
        GROUP BY {condition}
        ORDER BY count DESC
        """
        condition_data = run_query(condition_query, [bigquery.ScalarQueryParameter("organizer_keyword", "STRING", organizer_keyword_with_wildcard)])
        st.write(f"{condition}ごとのレコード数:")
        st.dataframe(condition_data)
