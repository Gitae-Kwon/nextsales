import streamlit as st
import pandas as pd
from sqlalchemy import create_engine
from prophet import Prophet
from prophet.make_holidays import make_holidays_df
from datetime import timedelta
import altair as alt

# ── 0) 한국 공휴일 (Prophet 예측에 사용) ──────────────────────────────
holidays_kr = make_holidays_df(year_list=[2024, 2025], country="KR")

# ── 1) RDS 연결 정보 (secrets.toml) ─────────────────────────────────
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

# ── 2) 연결 테스트 ───────────────────────────────────────────────────
try:
    conn = engine.connect()
    st.success("✅ DB 연결 성공!")
    conn.close()
except Exception as e:
    st.error(f"❌ DB 연결 실패: {e}")
    st.stop()

st.title("📊 웹툰 매출 & 결제 분석 대시보드 + 이벤트 인사이트")
weekdays = ["Sunday","Monday","Tuesday","Wednesday","Thursday","Friday","Saturday"]

# ── 3) 데이터 로드 함수 정의 ─────────────────────────────────────────
@st.cache_data
def load_coin_data():
    df = pd.read_sql(
        "SELECT date, Title, Total_coins FROM purchase_bomkr",
        con=engine
    )
    # 1) 문자열→숫자
    df["Total_coins"] = pd.to_numeric(df["Total_coins"], errors="coerce").fillna(0).astype(int)
    # 2) date 컬럼도 안전하게 파싱
    df["date"] = pd.to_datetime(df["date"], errors="coerce")
    return df

@st.cache_data
def load_payment_data():
    sql = """
      SELECT
        `date`,
        SUM(amount)                              AS amount,
        SUM(CASE WHEN payment_count = 1 THEN 1 ELSE 0 END) AS first_count
      FROM payment_bomkr
      GROUP BY `date`
    """
    df = pd.read_sql(sql, con=engine)

    # 1) 날짜 문자열 → datetime (실패는 NaT)
    df["date"] = pd.to_datetime(df["date"], format="%Y-%m-%d", errors="coerce")
    # 2) 파싱 실패 원본 행 따로 보관
    bad_idx  = df["date"].isna()
    bad_rows = df.loc[bad_idx, :].copy()
    # 3) 실패한 행 제거
    df = df.loc[~bad_idx, :].reset_index(drop=True)

    return df, bad_rows

# ── 4) 메인 로직 시작 ───────────────────────────────────────────────
# (a) 결제 데이터 로드 및 파싱 실패 행 처리
pay_df, bad_rows = load_payment_data()
if not bad_rows.empty:
    st.warning(f"⚠️ 날짜 파싱 실패 {len(bad_rows):,}건 → 해당 행들은 제거됩니다")
    st.write("❗ 파싱 실패 원본 행들(예시):", bad_rows.head())

# (b) 코인 데이터 로드
coin_df = load_coin_data()

# ── 5) 1) 결제 매출 분석 ─────────────────────────────────────────────
st.header("💳 결제 매출 분석")

# 이벤트 임계치 설정
if "pay_thresh" not in st.session_state:
    st.session_state.pay_thresh = 1.5
th_pay = st.number_input(
    "평균 대비 몇 % 이상일 때 결제 이벤트로 간주?",
    min_value=100, max_value=500,
    value=int(st.session_state.pay_thresh * 100),
    step=5
)
if st.button("결제 임계치 적용"):
    st.session_state.pay_thresh = th_pay / 100
st.caption(f"현재 결제 이벤트 임계치: {int(st.session_state.pay_thresh*100)}%")

# 이벤트 검출
df_pay = pay_df.sort_values("date").reset_index(drop=True)
df_pay["rolling_avg"] = df_pay["amount"].rolling(7, center=True, min_periods=1).mean()
df_pay["event_flag"]  = df_pay["amount"] > df_pay["rolling_avg"] * st.session_state.pay_thresh
df_pay["weekday"]     = df_pay["date"].dt.day_name()
pay_counts = df_pay[df_pay["event_flag"]]["weekday"].value_counts()

# 요일별 발생 분포
st.subheader("🌟 결제 이벤트 발생 요일 분포")
df_ev = pd.DataFrame({
    "weekday": weekdays,
    "count":   [pay_counts.get(d, 0) for d in weekdays]
})
chart_ev = alt.Chart(df_ev).mark_bar(color="blue").encode(
    x=alt.X("weekday:N", sort=weekdays, title="요일"),
    y=alt.Y("count:Q",    title="이벤트 횟수"),
    tooltip=["weekday","count"]
).properties(height=250)
st.altair_chart(chart_ev, use_container_width=True)

# 요일별 평균 증가 배수
st.subheader("💹 결제 이벤트 발생 시 요일별 평균 증가 배수")
rates = []
for d in weekdays:
    sub = df_pay[(df_pay["weekday"]==d) & df_pay["event_flag"]]
    if not sub.empty:
        rates.append((sub["amount"] / sub["rolling_avg"]).mean())
    else:
        rates.append(0)
df_ev["rate"] = rates
chart_rate = alt.Chart(df_ev).mark_bar(color="cyan").encode(
    x=alt.X("weekday:N", sort=weekdays, title="요일"),
    y=alt.Y("rate:Q",     title="평균 증가 배수"),
    tooltip=["weekday","rate"]
).properties(height=250)
st.altair_chart(chart_rate, use_container_width=True)

# 최근 3개월 추이
st.subheader("📈 결제 매출 최근 3개월 추이")
recent_pay = df_pay[df_pay["date"] >= df_pay["date"].max() - timedelta(days=90)]
st.line_chart(recent_pay.set_index("date")["amount"])

# 향후 15일 예측 (한국 공휴일 포함)
st.subheader("🔮 결제 매출 향후 15일 예측")
prop_df = df_pay.rename(columns={"date":"ds","amount":"y"})
m1 = Prophet()
m1.add_country_holidays(country_name="KR")
m1.fit(prop_df)
future = m1.make_future_dataframe(periods=15)
fc     = m1.predict(future)
pay_fc = fc[fc["ds"] > df_pay["date"].max()]
st.line_chart(pay_fc.set_index("ds")["yhat"])

# 첫 결제 추이
st.subheader("🚀 첫 결제 추이 (최근 3개월)")
recent_fc = df_pay[df_pay["date"] >= df_pay["date"].max() - timedelta(days=90)]
st.line_chart(recent_fc.set_index("date")["first_count"])


# ── 6) 2) 코인 매출 분석 ─────────────────────────────────────────────
st.header("🪙 코인 매출 분석")

coin_date_range = st.date_input("코인 분석 기간 설정", [], key="coin_date")
if len(coin_date_range) == 2:
    s, e = pd.to_datetime(coin_date_range[0]), pd.to_datetime(coin_date_range[1])
    df_p = coin_df[(coin_df["date"]>=s) & (coin_df["date"]<=e)]
    coin_sum = df_p.groupby("Title")["Total_coins"].sum().sort_values(ascending=False)
    first_launch = coin_df.groupby("Title")["date"].min()

    if "coin_top_n" not in st.session_state:
        st.session_state.coin_top_n = 10
    top_n       = st.session_state.coin_top_n
    total_coins = coin_sum.sum()

    df_top = coin_sum.head(top_n).reset_index(name="Total_coins")
    df_top.insert(0, "Rank", range(1, len(df_top)+1))
    df_top["Launch Date"] = df_top["Title"].map(first_launch).dt.strftime("%Y-%m-%d")
    df_top["is_new"]      = pd.to_datetime(df_top["Launch Date"]) >= s

    def hl(row):
        is_new = df_top.loc[row.name, "is_new"]
        return [
            "color: yellow" if (col == "Title" and is_new) else ""
            for col in row.index
        ]

    disp = df_top[["Rank","Title","Total_coins","Launch Date"]].copy()
    styled = (
        disp.style
            .apply(hl, axis=1)
            .format({"Total_coins":"{:,}"})
            .set_table_styles([
                {"selector":"th","props":[("text-align","center")]},
                {"selector":"td","props":[("text-align","center")]},
                {"selector":"th.row_heading, th.blank","props":[("display","none")]}
            ])
    )

    st.subheader(f"📋 Top {top_n} 작품 (코인 사용량) {total_coins:,}")
    st.markdown(styled.to_html(index=False, escape=False), unsafe_allow_html=True)

    if len(coin_sum) > top_n and st.button("더보기"):
        st.session_state.coin_top_n += 10

# ── 7) 3) 결제 주기 분석 ─────────────────────────────────────────────
st.header("⏱ 결제 주기 & 평균 결제금액 분석")
c1, c2, c3 = st.columns(3)
with c1:
    dr = st.date_input("기간 설정", [], key="cycle_dr")
with c2:
    k  = st.number_input("첫 번째 결제 건수", 1, 10, 2, key="cnt_k")
with c3:
    m  = st.number_input("두 번째 결제 건수", 1, 10, 3, key="cnt_m")

if st.button("결제 주기 계산"):
    if len(dr) == 2:
        st_dt, en_dt = pd.to_datetime(dr[0]), pd.to_datetime(dr[1])
        df_raw = pd.read_sql(
            "SELECT user_id, platform, payment_count, amount, date FROM payment_bomkr",
            con=engine
        )
        df_raw["date"] = pd.to_datetime(df_raw["date"], errors="coerce")
        df_filt = df_raw[
            (df_raw["date"]>=st_dt) &
            (df_raw["date"]<=en_dt) &
            (df_raw["payment_count"].isin([k,m]))
        ]

        df_k = (
            df_filt[df_filt["payment_count"]==k]
            .set_index("user_id")[["date","amount","platform"]]
            .rename(columns={"date":"d_k","amount":"a_k"})
        )
        df_m = (
            df_filt[df_filt["payment_count"]==m]
            .set_index("user_id")[["date","amount"]]
        )
        df_m.columns = ["d_m","a_m"]
        joined = df_k.join(df_m, how="inner")
        joined["cycle"] = (joined["d_m"] - joined["d_k"]).dt.days

        cycles   = joined["cycle"]
        amt_ser  = joined[["a_k","a_m"]].stack()
        pc       = joined["platform"].value_counts()
        mapping  = {"M":"Mobile Web","W":"PC Web","P":"Android","A":"Apple"}

        st.success(
            f"주기 → 평균: {cycles.mean():.1f}일 | 중앙값: {cycles.median():.1f}일 | 최빈값: {cycles.mode().iat[0]:.1f}일"
        )
        st.success(
            f"금액 → 평균: {amt_ser.mean():.2f} | 중앙값: {amt_ser.median():.2f} | 최빈값: {amt_ser.mode().iat[0]:.2f}"
        )
        st.success(
            "플랫폼 → " +
            ", ".join(f"{mapping.get(p,p)}: {cnt}건 ({cnt/len(joined):.1%})" for p,cnt in pc.items())
        )
    else:
        st.error("❗️ 시작일 · 종료일을 모두 선택해주세요.")
