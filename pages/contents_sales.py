import streamlit as st
import pandas as pd
from sqlalchemy import create_engine
from datetime import timedelta
import altair as alt

# ── 페이지 초기 상태 설정 ─────────────────────────────────────────
if "coin_top_n" not in st.session_state:
    st.session_state.coin_top_n = 10

# ── RDS 연결 정보 (secrets.toml) ──────────────────────────────────
user     = st.secrets["DB"]["DB_USER"]
password = st.secrets["DB"]["DB_PASSWORD"]
host     = st.secrets["DB"]["DB_HOST"]
port     = st.secrets["DB"]["DB_PORT"]
db       = st.secrets["DB"]["DB_NAME"]

engine = create_engine(
    f"mysql+pymysql://{user}:{password}@{host}:{port}/{db}?charset=utf8mb4",
    pool_recycle=3600,
    connect_args={"connect_timeout": 10}
)

# ── Coin 데이터 로드 함수 ─────────────────────────────────────────
@st.cache_data
def load_coin_data():
    sql = """
    SELECT
      date,
      Title,
      (g_coin - g_coin_cncl) + (b_coin - b_coin_cncl) AS Total_coins
    FROM purchase_log_bomkr
    """
    df = pd.read_sql(sql, con=engine)
    df["date"] = pd.to_datetime(df["date"], errors="coerce")
    df = df.dropna(subset=["date"]).reset_index(drop=True)
    return df

# ── 페이지 제목 및 입력 ─────────────────────────────────────────
st.header("🪙 코인 매출 분석")

coin_df = load_coin_data()
coin_date_range = st.date_input("코인 분석 기간 설정", [], key="coin_date")

if len(coin_date_range) == 2:
    s, e = pd.to_datetime(coin_date_range[0]), pd.to_datetime(coin_date_range[1])
    df_p = coin_df[(coin_df["date"] >= s) & (coin_df["date"] <= e)]

    # 전체 사용 코인 합계
    total_coins = int(df_p["Total_coins"].sum())

    # 작품별 코인 사용량 집계 및 정렬
    coin_sum = df_p.groupby("Title")["Total_coins"].sum().sort_values(ascending=False)
    first_launch = coin_df.groupby("Title")["date"].min()

    # Top N 설정
    top_n = st.session_state.coin_top_n
    top_n_sum = int(coin_sum.head(top_n).sum())
    ratio = top_n_sum / total_coins if total_coins else 0

    # 헤더: Top N / 전체 & 비율
    st.subheader(
        f"📋 Top {top_n} 작품: {top_n_sum:,} / {total_coins:,} ({ratio:.1%})"
    )

    # Top N 테이블 준비
    top_df = (
        coin_sum.head(top_n)
        .reset_index(name="Total_coins")
    )
    top_df.insert(0, "Rank", range(1, len(top_df) + 1))
    top_df["Launch Date"] = top_df["Title"].map(first_launch).dt.strftime("%Y-%m-%d")
    top_df["is_new"] = pd.to_datetime(top_df["Launch Date"]) >= s

    # 강조 함수
    def hl(row):
        is_new = top_df.loc[row.name, "is_new"]
        return ["color: yellow" if (col == "Title" and is_new) else "" for col in row.index]

    disp = top_df[["Rank","Title","Total_coins","Launch Date"]].copy()
    styled = (
        disp.style
            .apply(hl, axis=1)
            .format({"Total_coins": "{:,}"})
            .set_table_styles([
                {"selector": "th", "props": [("text-align","center")]},
                {"selector": "td", "props": [("text-align","center")]},
                {"selector": "th.row_heading, th.blank", "props": [("display","none")]}
            ])
    )
    st.markdown(styled.to_html(index=False, escape=False), unsafe_allow_html=True)

    # 더보기 버튼
    if len(coin_sum) > top_n:
        if st.button("더보기", key="btn_coin_more"):
            st.session_state.coin_top_n += 10
