import streamlit as st
import pandas as pd
from prophet import Prophet
import matplotlib.pyplot as plt

st.set_page_config(layout="wide")
st.title("🔮 향후 매출 예측 (공휴일 반영 포함)")

# 📁 엑셀 파일 업로드
uploaded_file = st.file_uploader("📤 일자별 매출 엑셀파일 업로드 (날짜/매출)", type=["xlsx"])

if uploaded_file:
    # 📄 데이터 불러오기
    df = pd.read_excel(uploaded_file)
    df.columns = df.columns.str.strip().str.lower()
    df.rename(columns={"date": "ds", "charged_amount": "y"}, inplace=True)
    df["ds"] = pd.to_datetime(df["ds"])
    df = df[["ds", "y"]].dropna()

    # Prophet 모델 구성 및 공휴일 반영
    model = Prophet()
    model.add_country_holidays(country_name="FR")  # 🇰🇷 한국 공휴일 포함
    model.fit(df)

    # 🔮 향후 30일 예측
    future = model.make_future_dataframe(periods=30)
    forecast = model.predict(future)

    # 📈 전체 예측 그래프
    st.subheader("📈 전체 예측 그래프 (공휴일 반영)")
    fig1 = model.plot(forecast)
    st.pyplot(fig1)

    # 🧠 트렌드 & 요일별/연간 패턴
    st.subheader("📊 구성요소 분해 그래프")
    fig2 = model.plot_components(forecast)
    st.pyplot(fig2)

    # 📅 공휴일이 포함된 일자만 확인
    st.subheader("📆 예측된 공휴일 목록")
    forecast_holiday = forecast[forecast["holidays"].notnull()][["ds", "yhat", "holidays"]]
    st.dataframe(forecast_holiday.reset_index(drop=True))
else:
    st.info("파일을 업로드하면 분석이 시작됩니다.")
