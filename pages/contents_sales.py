import streamlit as st
import pandas as pd
from sqlalchemy import create_engine
from prophet import Prophet
from prophet.make_holidays import make_holidays_df
from datetime import timedelta
import altair as alt

# ── coin_top_n 상태 초기화 ─────────────────────────────────────────
if "coin_top_n" not in st.session_state:
    st.session_state.coin_top_n = 10

# ── 한국 공휴일 (Prophet 예측에 사용) ───────────────────────────────
holidays_kr = make_holidays_df(year_list=[2024, 2025], country="KR")

# ── RDS 연결 정보 (secrets.toml) ────────────────────────────────────
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

# ── 데이터 로드 ───────────────────────────────────────────────────
@st.cache_data
def load_coin_data():
    # purchase_log_bomkr 테이블에서 g_coin, b_coin, g_coin_cncl, b_coin_cncl 을 불러와
    # 토탈코인(total_coin)을 계산
    sql = """
      SELECT
        date,
        Title,
        (g_coin - g_coin_cncl) + (b_coin - b_coin_cncl) AS total_coin
      FROM purchase_log_bomkr
    """
    df = pd.read_sql(sql, con=engine)
    # 날짜 파싱
    df["date"] = pd.to_datetime(df["date"], errors="coerce")
    # total_coin 정수로
    df["total_coin"] = pd.to_numeric(df["total_coin"], errors="coerce").fillna(0).astype(int)
    return df

# ── 페이지 상단 ───────────────────────────────────────────────────
st.header("🪙 코인 매출 분석")
coin_date_range = st.date_input("코인 분석 기간 설정", [], key="coin_date")

if len(coin_date_range) == 2:
    s, e = map(pd.to_datetime, coin_date_range)
    coin_df = load_coin_data()
    # 기간 필터링
    df_p = coin_df[(coin_df["date"] >= s) & (coin_df["date"] <= e)]

    # 전체 사용 코인
    total_coins = int(df_p["total_coin"].sum())

    # 작품별 합산 & 내림차순 정렬
    coin_sum = (
        df_p
        .groupby("Title")["total_coin"]
        .sum()
        .sort_values(ascending=False)
    )

    # Top N
    top_n      = st.session_state.coin_top_n
    top_n_sum  = int(coin_sum.head(top_n).sum())
    ratio      = top_n_sum / total_coins if total_coins else 0

    # 헤더에 합계/비율 표시
    st.subheader(
        f"📋 Top {top_n} 작품: "
        f"{top_n_sum:,} / {total_coins:,} ({ratio:.1%})"
    )

    # 테이블 준비
    first_launch = coin_df.groupby("Title")["date"].min()
    top_df = coin_sum.head(top_n).reset_index(name="Total_coins")
    top_df.insert(0, "Rank", range(1, len(top_df) + 1))
    top_df["Launch Date"] = top_df["Title"].map(first_launch).dt.strftime("%Y-%m-%d")
    top_df["is_new"]      = pd.to_datetime(top_df["Launch Date"]) >= s

    # 하이라이트 함수
    def hl(row):
        is_new = top_df.loc[row.name, "is_new"]
        return ["color: yellow" if (col=="Title" and is_new) else "" for col in row.index]

    disp = top_df[["Rank","Title","Total_coins","Launch Date"]].copy()
    styled = (
        disp.style
            .apply(hl, axis=1)
            .format({"Total_coins":"{:,}"})
            .set_table_styles([
                {"selector":"th", "props":[("text-align","center")]},
                {"selector":"td", "props":[("text-align","center")]},
                {"selector":"th.row_heading, th.blank","props":[("display","none")]}
            ])
    )
    st.markdown(styled.to_html(index=False, escape=False), unsafe_allow_html=True)

    # 더보기 버튼
    if len(coin_sum) > top_n:
        if st.button("더보기", key="btn_coin_more"):
            st.session_state.coin_top_n += 10
