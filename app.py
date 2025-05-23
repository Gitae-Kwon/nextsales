import streamlit as st
import pandas as pd
import matplotlib.pyplot as plt
from prophet import Prophet

# ▶ Data Load (from CSV or internal placeholder)
df = pd.read_excel("dede.xlsx", skiprows=2)
df = df[["Unnamed: 1", "금액"]].copy()
df.columns = ["date", "charged_amount"]
df.dropna(inplace=True)
df["date"] = pd.to_datetime(df["date"])
df = df.sort_values("date")

# ▶ 파생 변수 생성 (이벤트 여부 판단)
df["prev"] = df["charged_amount"].shift(1)
df["avg7"] = df["charged_amount"].rolling(7, center=True, min_periods=1).mean()
df["is_event"] = ((df["charged_amount"] >= df["prev"] * 2) |
                  (df["charged_amount"] >= df["avg7"] * 2)).astype(int)

# ▶ Prophet 준비
df_prophet = df[["date", "charged_amount", "is_event"]].rename(columns={
    "date": "ds",
    "charged_amount": "y"
})
model = Prophet()
model.add_regressor("is_event")
model.fit(df_prophet)

# ▶ 예측 생성
future = model.make_future_dataframe(periods=30)
future = future.merge(df_prophet[["ds", "is_event"]], on="ds", how="left").fillna(0)
forecast = model.predict(future)

# ▶ Streamlit UI
st.title("할인 이벤트 및 일자별 매주 예측")
st.line_chart(forecast.set_index("ds")["yhat"])

st.subheader("현재 데이터 그리기")
st.line_chart(df.set_index("date")["charged_amount"])

st.write("▶ 이벤트 일자")
st.dataframe(df[df["is_event"] == 1][["date", "charged_amount"]].reset_index(drop=True))
