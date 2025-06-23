import streamlit as st
import pandas as pd
from sqlalchemy import create_engine
from datetime import timedelta

# ── RDS 연결 (secrets.toml) ─────────────────────────────────────
user     = st.secrets['DB']['DB_USER']
password = st.secrets['DB']['DB_PASSWORD']
host     = st.secrets['DB']['DB_HOST']
port     = st.secrets['DB']['DB_PORT']
db       = st.secrets['DB']['DB_NAME']

engine = create_engine(
    f"mysql+pymysql://{user}:{password}@{host}:{port}/{db}?charset=utf8mb4",
    pool_recycle=3600,
    connect_args={"connect_timeout": 10}
)

# ── 데이터 로드 함수 정의 ─────────────────────────────────────────
@st.cache_data
def load_payment_data():
    """
    전체 결제 데이터 로드
    """
    sql = """
    SELECT
      `date`,
      SUM(amount) AS amount
    FROM payment_bomkr
    GROUP BY `date`
    """
    df = pd.read_sql(sql, con=engine)
    df['date'] = pd.to_datetime(df['date'], format='%Y-%m-%d', errors='coerce')
    return df.dropna(subset=['date'])

@st.cache_data
def load_coin_data():
    """
    전체 코인 사용 데이터 로드
    """
    sql = """
    SELECT date, Total_coins
    FROM purchase_bomkr
    """
    df = pd.read_sql(sql, con=engine)
    df['date']        = pd.to_datetime(df['date'], errors='coerce')
    df['Total_coins'] = pd.to_numeric(df['Total_coins'], errors='coerce').fillna(0).astype(int)
    return df.dropna(subset=['date'])

# ── 홈 요약 페이지 ───────────────────────────────────────────────
st.title("🏠 홈 요약 대시보드")

# 데이터 불러오기
pay_df  = load_payment_data()
coin_df = load_coin_data()

# 요약 지표 계산
total_pay  = int(pay_df['amount'].sum())
days_count = pay_df['date'].nunique()
avg_pay    = total_pay / days_count if days_count else 0
total_coins= int(coin_df['Total_coins'].sum())

# 메트릭 표시
c1, c2 = st.columns(2)
with c1:
    st.metric("총 결제 금액", f"{total_pay:,}원", f"일평균 {avg_pay:,.0f}원")
with c2:
    st.metric("총 코인 사용량", f"{total_coins:,} 코인")

# 최근 추이 차트
st.markdown("---")
st.subheader("📈 최근 30일 결제 추이")
recent_pay = pay_df[pay_df['date'] >= pd.Timestamp.today() - timedelta(days=30)]
st.line_chart(recent_pay.set_index('date')['amount'])

st.subheader("🪙 최근 30일 코인 사용량 추이")
recent_coin = coin_df[coin_df['date'] >= pd.Timestamp.today() - timedelta(days=30)]
coin_trend  = recent_coin.groupby('date')['Total_coins'].sum().reset_index()
st.line_chart(coin_trend.set_index('date')['Total_coins'])
