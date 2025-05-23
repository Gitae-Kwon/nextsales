import streamlit as st
import pandas as pd
import matplotlib.pyplot as plt
from prophet import Prophet

st.title("📈 할인 이벤트 기반 매출 예측 앱 (공휴일 반영 포함)")

# ▶ 파일 업로드
uploaded_file = st.file_uploader("엑셀 파일을 업로드하세요 (2행 이후부터 유효 데이터)", type=["xlsx"])

if uploaded_file is not None:
    # 데이터 로드
    df = pd.read_excel(uploaded_file, skiprows=2)
    df = df[["Unnamed: 1", "금액"]].copy()
    df.columns = ["date", "charged_amount"]
    df.dropna(inplace=True)
    df["date"] = pd.to_datetime(df["date"])
    df = df.sort_values("date")

    # 파생 변수 생성 (이벤트 감지)
    df["prev"] = df["charged_amount"].shift(1)
    df["avg7"] = df["charged_amount"].rolling(7, center=True, min_periods=1).mean()
    df["is_event"] = ((df["charged_amount"] >= df["prev"] * 2) |
                      (df["charged_amount"] >= df["avg7"] * 2)).astype(int)

    # Prophet 준비 (🇫🇷 프랑스 공휴일 반영)
    df_prophet = df[["date", "charged_amount", "is_event"]].rename(columns={
        "date": "ds",
        "charged_amount": "y"
    })
    model = Prophet()
    model.add_country_holidays(country_name="FR")  # 공휴일 추가
    model.add_regressor("is_event")
    model.fit(df_prophet)

    # 예측 생성
    future = model.make_future_dataframe(periods=30)
    future = future.merge(df_prophet[["ds", "is_event"]], on="ds", how="left").fillna(0)
    forecast = model.predict(future)

    # 예측 결과 시각화 (최근 30일만 필터링)
    forecast_recent = forecast[forecast["ds"] > df["ds"].max()]

    st.subheader("🔮 향후 30일 예측 매출")
    st.line_chart(forecast_recent.set_index("ds")["yhat"])

    st.subheader("📊 과거 매출 추이")
    st.line_chart(df.set_index("date")["charged_amount"])

    st.subheader("📅 이벤트 감지 일자")
    st.dataframe(df[df["is_event"] == 1][["date", "charged_amount"]].reset_index(drop=True))

    st.subheader("📆 공휴일 반영된 예측 목록")
    st.dataframe(forecast[forecast["holidays"].notnull()][["ds", "yhat", "holidays"]])

    # 예측 요소 분해 시각화 추가
    st.subheader("📉 예측 요소 분해 보기")
    fig = model.plot_components(forecast)
    st.pyplot(fig)
else:
    st.info("파일을 업로드하면 분석이 시작됩니다.")
