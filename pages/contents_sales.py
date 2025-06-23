import streamlit as st
import pandas as pd
import altair as alt
from sqlalchemy import create_engine
from prophet import Prophet
from prophet.make_holidays import make_holidays_df
from datetime import timedelta

# ── coin_top_n 초기화 ─────────────────────────
if "coin_top_n" not in st.session_state:
    st.session_state.coin_top_n = 10

# ── 한국 공휴일 ───────────────────────────────
holidays_kr = make_holidays_df(year_list=[2024,2025], country="KR")

# ── DB 연결(Secrets.toml) ───────────────────────
user     = st.secrets["DB"]["DB_USER"]
password = st.secrets["DB"]["DB_PASSWORD"]
host     = st.secrets["DB"]["DB_HOST"]
port     = st.secrets["DB"]["DB_PORT"]
db       = st.secrets["DB"]["DB_NAME"]
engine   = create_engine(
    f"mysql+pymysql://{user}:{password}@{host}:{port}/{db}?charset=utf8mb4"
)

st.header("🪙 코인 매출 분석")

# 데이터 로드
@st.cache_data
def load_coin_data():
    df = pd.read_sql("SELECT date, Title, Total_coins FROM purchase_bomkr", con=engine)
    df["Total_coins"] = pd.to_numeric(df["Total_coins"], errors="coerce").fillna(0).astype(int)
    df["date"]       = pd.to_datetime(df["date"], errors="coerce")
    return df

coin_df = load_coin_data()

# 기간 설정
coin_date_range = st.date_input("코인 분석 기간 설정", [], key="coin_date")
if len(coin_date_range)==2:
    s, e = pd.to_datetime(coin_date_range[0]), pd.to_datetime(coin_date_range[1])
    df_p = coin_df[(coin_df.date>=s)&(coin_df.date<=e)]

    # 전체/TopN 계산
    total_coins = df_p.Total_coins.sum()
    coin_sum    = df_p.groupby("Title").Total_coins.sum().sort_values(ascending=False)
    top_n       = st.session_state.coin_top_n
    top_n_sum   = coin_sum.head(top_n).sum()
    ratio       = top_n_sum/total_coins if total_coins else 0

    st.subheader(f"📋 Top {top_n} 작품: {top_n_sum:,} / {int(total_coins):,} ({ratio:.1%})")

    # 테이블 렌더링
    first_launch = coin_df.groupby("Title").date.min()
    df_top = (
        coin_sum
        .head(top_n)
        .reset_index(name="Total_coins")
        .assign(
            Rank=lambda d: range(1, len(d)+1),
            **{"Launch Date": lambda d: d["Title"].map(first_launch).dt.strftime("%Y-%m-%d")}
        )
    )
    df_top["is_new"] = pd.to_datetime(df_top["Launch Date"])>=s

    def hl(row):
        is_new = df_top.loc[row.name,"is_new"]
        return ["color: yellow" if (col=="Title" and is_new) else "" for col in row.index]

    disp = df_top[["Rank","Title","Total_coins","Launch Date"]].copy()
    styled = (
        disp.style
            .apply(hl, axis=1)
            .format({"Total_coins":"{:,}"})
            .set_table_styles([
                {"selector":"th, td","props":[("text-align","center")]},
                {"selector":"th.row_heading, th.blank","props":[("display","none")]}
            ])
    )
    st.markdown(styled.to_html(index=False, escape=False), unsafe_allow_html=True)

    if len(coin_sum)>top_n and st.button("더보기", key="btn_coin_more"):
        st.session_state.coin_top_n += 10
