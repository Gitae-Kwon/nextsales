import streamlit as st
import pandas as pd
from sqlalchemy import create_engine
from prophet import Prophet
from prophet.make_holidays import make_holidays_df
from datetime import timedelta, datetime
import altair as alt

# ── 페이지 설정 ──────────────────────────────────────────────────
st.set_page_config(page_title="웹툰 매출 분석", layout="wide")

# ── 세션 상태 초기화 ──────────────────────────────────────────────
if "coin_top_n" not in st.session_state:
    st.session_state.coin_top_n = 10

if "pay_thresh" not in st.session_state:
    st.session_state.pay_thresh = 1.5

# ── 한국 공휴일 설정 ──────────────────────────────────────────────
@st.cache_data
def get_korean_holidays():
    return make_holidays_df(year_list=[2024, 2025], country="KR")

# ── DB 연결 설정 ──────────────────────────────────────────────────
@st.cache_resource
def init_db_connection():
    try:
        user = st.secrets["DB"]["DB_USER"]
        password = st.secrets["DB"]["DB_PASSWORD"]
        host = st.secrets["DB"]["DB_HOST"]
        port = st.secrets["DB"]["DB_PORT"]
        db = st.secrets["DB"]["DB_NAME"]
        
        engine = create_engine(
            f"mysql+pymysql://{user}:{password}@{host}:{port}/{db}?charset=utf8mb4",
            pool_recycle=3600,
            connect_args={"connect_timeout": 10}
        )
        
        # 연결 테스트
        with engine.connect() as conn:
            conn.execute("SELECT 1")
        
        return engine
    except Exception as e:
        st.error(f"❌ DB 연결 실패: {e}")
        st.stop()

engine = init_db_connection()
st.success("✅ DB 연결 성공!")

# ── 데이터 로드 함수들 ──────────────────────────────────────────────
@st.cache_data
def load_payment_data(start_date=None, end_date=None):
    """결제 데이터 로드"""
    try:
        base_query = """
          SELECT
            `date`,
            SUM(amount) AS amount,
            SUM(CASE WHEN payment_count = 1 THEN 1 ELSE 0 END) AS first_count
          FROM payment_bomkr
        """
        
        if start_date and end_date:
            query = base_query + f" WHERE date BETWEEN '{start_date}' AND '{end_date}' GROUP BY `date`"
        else:
            query = base_query + " GROUP BY `date`"
        
        df = pd.read_sql(query, con=engine)
        df["date"] = pd.to_datetime(df["date"], format="%Y-%m-%d", errors="coerce")
        
        # 파싱 실패한 행들 처리
        bad_rows = df[df["date"].isna()]
        clean_df = df.dropna(subset=["date"]).reset_index(drop=True)
        
        return clean_df, bad_rows
        
    except Exception as e:
        st.error(f"결제 데이터 로드 실패: {e}")
        return pd.DataFrame(), pd.DataFrame()

@st.cache_data
def load_coin_data():
    """코인 데이터 로드"""
    try:
        df = pd.read_sql(
            "SELECT date, Title, Total_coins FROM purchase_bomkr",
            con=engine
        )
        df["Total_coins"] = pd.to_numeric(df["Total_coins"], errors="coerce").fillna(0).astype(int)
        df["date"] = pd.to_datetime(df["date"], errors="coerce")
        df = df.dropna(subset=["date"]).reset_index(drop=True)
        return df
    except Exception as e:
        st.error(f"코인 데이터 로드 실패: {e}")
        return pd.DataFrame()

@st.cache_data
def load_cycle_data(start_date, end_date):
    """결제 주기 분석용 데이터 로드"""
    try:
        df = pd.read_sql(
            """
            SELECT
              user_id,
              platform,
              payment_count,
              amount,
              date
            FROM payment_bomkr
            WHERE date BETWEEN %s AND %s
            """,
            con=engine,
            params=[start_date, end_date]
        )
        df["date"] = pd.to_datetime(df["date"], errors="coerce")
        return df.dropna(subset=["date"]).reset_index(drop=True)
    except Exception as e:
        st.error(f"주기 분석 데이터 로드 실패: {e}")
        return pd.DataFrame()

@st.cache_data
def train_prophet_model(df):
    """Prophet 모델 훈련 (캐싱으로 성능 개선)"""
    if df.empty:
        return None
    
    try:
        prop_df = df.rename(columns={"date": "ds", "amount": "y"})
        m = Prophet()
        m.add_country_holidays(country_name="KR")
        m.fit(prop_df)
        return m
    except Exception as e:
        st.error(f"Prophet 모델 훈련 실패: {e}")
        return None

# ── 메인 앱 시작 ──────────────────────────────────────────────────
st.title("📊 웹툰 매출 & 결제 분석 대시보드 + 이벤트 인사이트")

weekdays = ["Sunday", "Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday"]

# ── 데이터 로드 ──────────────────────────────────────────────────
with st.spinner("데이터 로딩 중..."):
    pay_df, bad_rows = load_payment_data()
    coin_df = load_coin_data()

# 파싱 실패 알림
if not bad_rows.empty:
    st.warning(f"⚠️ 날짜 파싱 실패 {len(bad_rows):,}건 → 해당 행들은 제거됩니다")
    with st.expander("파싱 실패 행들 확인"):
        st.dataframe(bad_rows.head())

if pay_df.empty:
    st.error("❌ 결제 데이터가 없습니다.")
    st.stop()

# ── 1) 결제 매출 분석 ─────────────────────────────────────────────
st.header("💳 결제 매출 분석")

# 이벤트 임계치 설정
col1, col2 = st.columns([3, 1])
with col1:
    th_pay = st.slider(
        "평균 대비 몇 % 이상일 때 결제 이벤트로 간주?",
        min_value=100, max_value=500,
        value=int(st.session_state.pay_thresh * 100),
        step=5
    )
with col2:
    if st.button("임계치 적용"):
        st.session_state.pay_thresh = th_pay / 100
        st.rerun()

st.caption(f"현재 결제 이벤트 임계치: {int(st.session_state.pay_thresh*100)}%")

# 이벤트 검출
df_pay = pay_df.sort_values("date").reset_index(drop=True)
df_pay["rolling_avg"] = df_pay["amount"].rolling(7, center=True, min_periods=1).mean()
df_pay["event_flag"] = df_pay["amount"] > df_pay["rolling_avg"] * st.session_state.pay_thresh
df_pay["weekday"] = df_pay["date"].dt.day_name()

# 이벤트가 있을 때만 분석 진행
if df_pay["event_flag"].sum() > 0:
    pay_counts = df_pay[df_pay["event_flag"]]["weekday"].value_counts()
    
    # 요일별 발생 분포
    st.subheader("🌟 결제 이벤트 발생 요일 분포")
    df_ev = pd.DataFrame({
        "weekday": weekdays,
        "count": [pay_counts.get(d, 0) for d in weekdays]
    })
    
    chart_ev = alt.Chart(df_ev).mark_bar(color="blue").encode(
        x=alt.X("weekday:N", sort=weekdays, title="요일"),
        y=alt.Y("count:Q", title="이벤트 횟수"),
        tooltip=["weekday", "count"]
    ).properties(height=250)
    st.altair_chart(chart_ev, use_container_width=True)
    
    # 요일별 평균 증가 배수
    st.subheader("💹 결제 이벤트 발생 시 요일별 평균 증가 배수")
    rates = []
    for d in weekdays:
        sub = df_pay[(df_pay["weekday"] == d) & df_pay["event_flag"]]
        if not sub.empty:
            rates.append((sub["amount"] / sub["rolling_avg"]).mean())
        else:
            rates.append(0)
    
    df_ev["rate"] = rates
    chart_rate = alt.Chart(df_ev).mark_bar(color="cyan").encode(
        x=alt.X("weekday:N", sort=weekdays, title="요일"),
        y=alt.Y("rate:Q", title="평균 증가 배수"),
        tooltip=["weekday", "rate"]
    ).properties(height=250)
    st.altair_chart(chart_rate, use_container_width=True)
else:
    st.info("현재 임계치에서는 이벤트가 검출되지 않았습니다. 임계치를 낮춰보세요.")

# 최근 3개월 추이
st.subheader("📈 결제 매출 최근 3개월 추이")
recent_pay = df_pay[df_pay["date"] >= df_pay["date"].max() - timedelta(days=90)]
if not recent_pay.empty:
    st.line_chart(recent_pay.set_index("date")["amount"])
else:
    st.info("최근 3개월 데이터가 없습니다.")

# 향후 15일 예측
st.subheader("🔮 결제 매출 향후 15일 예측")
with st.spinner("Prophet 모델 훈련 중..."):
    model = train_prophet_model(df_pay)

if model:
    try:
        future = model.make_future_dataframe(periods=15)
        forecast = model.predict(future)
        pay_forecast = forecast[forecast["ds"] > df_pay["date"].max()]
        
        if not pay_forecast.empty:
            st.line_chart(pay_forecast.set_index("ds")["yhat"])
        else:
            st.info("예측 데이터가 생성되지 않았습니다.")
    except Exception as e:
        st.error(f"예측 실패: {e}")
else:
    st.error("Prophet 모델 훈련에 실패했습니다.")

# 첫 결제 추이
st.subheader("🚀 첫 결제 추이 (최근 3개월)")
if not recent_pay.empty and "first_count" in recent_pay.columns:
    st.line_chart(recent_pay.set_index("date")["first_count"])
else:
    st.info("첫 결제 데이터가 없습니다.")

# ── 2) 코인 매출 분석 ─────────────────────────────────────────────
st.header("🪙 코인 매출 분석")

if coin_df.empty:
    st.error("❌ 코인 데이터가 없습니다.")
else:
    # 기본 날짜 범위 설정 (최근 30일)
    default_end = coin_df["date"].max().date()
    default_start = (coin_df["date"].max() - timedelta(days=30)).date()
    
    coin_date_range = st.date_input(
        "코인 분석 기간 설정", 
        value=[default_start, default_end],
        key="coin_date"
    )
    
    if len(coin_date_range) == 2:
        s, e = pd.to_datetime(coin_date_range[0]), pd.to_datetime(coin_date_range[1])
        df_p = coin_df[(coin_df.date >= s) & (coin_df.date <= e)]
        
        if not df_p.empty:
            # 그룹별 합계 & 전체 사용량
            coin_sum = df_p.groupby("Title")["Total_coins"].sum().sort_values(ascending=False)
            total_coins = int(coin_sum.sum())
            first_launch = coin_df.groupby("Title")["date"].min()
            
            top_n = st.session_state.coin_top_n
            
            # Top N DataFrame
            df_top = coin_sum.head(top_n).reset_index(name="Total_coins")
            top_n_sum = int(df_top["Total_coins"].sum())
            ratio = top_n_sum / total_coins if total_coins > 0 else 0
            
            # 헤더에 전체/TopN 합계 & 비율 표시
            st.subheader(
                f"📋 Top {top_n} 작품 (코인 사용량) "
                f"{top_n_sum:,} / {total_coins:,} ({ratio:.1%})"
            )
            
            # 테이블 생성
            df_top.insert(0, "Rank", range(1, len(df_top) + 1))
            df_top["Launch Date"] = df_top["Title"].map(first_launch).dt.strftime("%Y-%m-%d")
            df_top["is_new"] = pd.to_datetime(df_top["Launch Date"]) >= s
            
            def highlight_new(row):
                is_new = df_top.loc[row.name, "is_new"] if row.name < len(df_top) else False
                return ["color: yellow" if (col == "Title" and is_new) else "" for col in row.index]
            
            display_df = df_top[["Rank", "Title", "Total_coins", "Launch Date"]].copy()
            styled = (
                display_df.style
                .apply(highlight_new, axis=1)
                .format({"Total_coins": "{:,}"})
                .set_table_styles([
                    {"selector": "th", "props": [("text-align", "center")]},
                    {"selector": "td", "props": [("text-align", "center")]},
                    {"selector": "th.row_heading, th.blank", "props": [("display", "none")]}
                ])
            )
            st.markdown(styled.to_html(index=False, escape=False), unsafe_allow_html=True)
            
            # 더보기 버튼
            if len(coin_sum) > st.session_state.coin_top_n:
                if st.button("더보기", key="btn_coin_more"):
                    st.session_state.coin_top_n += 10
                    st.rerun()
        else:
            st.info("선택한 기간에 코인 데이터가 없습니다.")
    else:
        st.info("시작일과 종료일을 모두 선택해주세요.")

# ── 3) 결제 주기 분석 ─────────────────────────────────────────────
st.header("⏱ 결제 주기 & 평균 결제금액 분석")

# 기본 날짜 범위 설정
if not pay_df.empty:
    default_end_cycle = pay_df["date"].max().date()
    default_start_cycle = (pay_df["date"].max() - timedelta(days=90)).date()
else:
    default_end_cycle = datetime.now().date()
    default_start_cycle = (datetime.now() - timedelta(days=90)).date()

with st.form("cycle_form"):
    dr = st.date_input(
        "기간 설정",
        value=[default_start_cycle, default_end_cycle],
        key="cycle_dr"
    )
    
    col1, col2 = st.columns(2)
    with col1:
        k = st.number_input("첫 번째 결제 건수", 1, 10, 2, key="cnt_k")
    with col2:
        m = st.number_input("두 번째 결제 건수", 1, 10, 3, key="cnt_m")
    
    submit = st.form_submit_button("결제 주기 계산")

if submit:
    if len(dr) == 2:
        start = dr[0].strftime("%Y-%m-%d")
        end = dr[1].strftime("%Y-%m-%d")
        
        with st.spinner("결제 주기 분석 중..."):
            df_raw = load_cycle_data(start, end)
        
        if not df_raw.empty:
            df_filt = df_raw[df_raw["payment_count"].isin([k, m])]
            
            if not df_filt.empty:
                # 첫/두번째 결제 분리 및 주기 계산
                df_k = (
                    df_filt[df_filt["payment_count"] == k]
                    .set_index("user_id")[["date", "amount", "platform"]]
                    .rename(columns={"date": "d_k", "amount": "a_k"})
                )
                df_m = (
                    df_filt[df_filt["payment_count"] == m]
                    .set_index("user_id")[["date", "amount"]]
                    .rename(columns={"date": "d_m", "amount": "a_m"})
                )
                
                joined = df_k.join(df_m, how="inner")
                
                if not joined.empty:
                    joined["cycle"] = (joined["d_m"] - joined["d_k"]).dt.days
                    
                    # 통계 계산
                    cycles = joined["cycle"]
                    amt_ser = joined[["a_k", "a_m"]].stack()
                    plat_counts = joined["platform"].value_counts()
                    mapping = {"M": "Mobile Web", "W": "PC Web", "P": "Android", "A": "Apple"}
                    
                    # 결과 출력
                    col1, col2, col3 = st.columns(3)
                    
                    with col1:
                        st.metric("평균 주기", f"{cycles.mean():.1f}일")
                        st.metric("중앙값 주기", f"{cycles.median():.1f}일")
                    
                    with col2:
                        st.metric("평균 결제금액", f"{amt_ser.mean():.0f}원")
                        st.metric("중앙값 결제금액", f"{amt_ser.median():.0f}원")
                    
                    with col3:
                        st.metric("분석 대상 사용자", f"{len(joined):,}명")
                        if not cycles.empty:
                            mode_val = cycles.mode()
                            if len(mode_val) > 0:
                                st.metric("최빈 주기", f"{mode_val.iloc[0]:.0f}일")
                    
                    # 플랫폼 분포
                    st.subheader("📱 플랫폼 분포")
                    platform_text = ", ".join(
                        f"{mapping.get(p, p)}: {cnt}건 ({cnt/len(joined):.1%})"
                        for p, cnt in plat_counts.items()
                    )
                    st.info(platform_text)
                    
                else:
                    st.warning("두 결제 건수를 모두 가진 사용자가 없습니다.")
            else:
                st.warning(f"지정한 결제 건수({k}회, {m}회)에 해당하는 데이터가 없습니다.")
        else:
            st.warning("선택한 기간에 결제 데이터가 없습니다.")
    else:
        st.error("❗️ 시작일과 종료일을 모두 선택해주세요.")

# ── 푸터 정보 ──────────────────────────────────────────────────
st.markdown("---")
st.markdown("*💡 페이지를 새로고침하면 캐시된 데이터가 다시 로드됩니다.*")
