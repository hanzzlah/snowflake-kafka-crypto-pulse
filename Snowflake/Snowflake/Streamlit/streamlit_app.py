import streamlit as st
import pandas as pd
import altair as alt
from snowflake.snowpark.context import get_active_session

# ==========================================
# 1. Page Configuration & Session Setup
# ==========================================
st.set_page_config(page_title="Crypto Pulse Tracker", layout="wide")
session = get_active_session()

st.title("Crypto Pulse Tracker")
st.caption("Live price stream + daily ARIMA forecast band (PKT)")

# ==========================================
# 2. Dynamic Asset Selection
# ==========================================
col_sel, _ = st.columns([1, 3])
with col_sel:
    symbol = st.selectbox("Select Cryptocurrency", ["BTCUSDT", "ETHUSDT", "SOLUSDT"])

# ==========================================
# 3. Data Fetching
# ==========================================
dashboard_query = f"""
    SELECT * 
    FROM CRYPTO_PULSE_DB.PUBLIC.V_DASHBOARD_LIVE 
    WHERE symbol = '{symbol}' 
    ORDER BY trade_time
"""
df = session.sql(dashboard_query).to_pandas()

if df.empty:
    st.warning(f"No live ticks recorded yet for {symbol}. Waiting for stream data...")
else:
    # ==========================================
    # 4. Data Pre-Processing & Interpolation
    # ==========================================
    df["TRADE_TIME"] = pd.to_datetime(df["TRADE_TIME"]).dt.floor("s")
    df = df.groupby("TRADE_TIME").agg({
        "SYMBOL": "first",
        "TRADE_PRICE": "last",
        "PREDICTED_LOW": "last",
        "PREDICTED_HIGH": "last",
        "PRICE_STATUS": "last"
    }).reset_index()

    # Fill time gaps for smooth Altair tooltip hover
    df = df.set_index("TRADE_TIME")
    df = df.resample("1s").asfreq()
    
    df["SYMBOL"] = df["SYMBOL"].ffill()
    df["PRICE_STATUS"] = df["PRICE_STATUS"].ffill()
    
    df["TRADE_PRICE"] = df["TRADE_PRICE"].interpolate(method="linear")
    df["PREDICTED_LOW"] = df["PREDICTED_LOW"].interpolate(method="linear")
    df["PREDICTED_HIGH"] = df["PREDICTED_HIGH"].interpolate(method="linear")
    
    df = df.dropna(subset=["TRADE_PRICE"]).reset_index()

    if df.empty:
        st.info(f"Gathering enough continuous data points for {symbol}...")
    else:
        # ==========================================
        # 5. KPI Metrics
        # ==========================================
        latest = df.iloc[-1]

        col1, col2, col3 = st.columns(3)
        col1.metric("Live Price", f"${latest['TRADE_PRICE']:,.2f}")
        col2.metric("Predicted Range", f"${latest['PREDICTED_LOW']:,.0f} – ${latest['PREDICTED_HIGH']:,.0f}")
        col3.metric("Status", latest['PRICE_STATUS'])

        # Safe Y-axis domain calculation
        min_val = df["TRADE_PRICE"].min()
        max_val = df["TRADE_PRICE"].max()
        if min_val == max_val:
            y_min = min_val * 0.99
            y_max = max_val * 1.01
        else:
            y_min = min_val * 0.9995
            y_max = max_val * 1.0005

        # ==========================================
        # 6. Altair Visualization (Direct Rendering)
        # ==========================================
        nearest = alt.selection_point(nearest=True, on='mouseover', fields=['TRADE_TIME'], empty=False)

        band = alt.Chart(df).mark_area(opacity=0.15, color="steelblue").encode(
            x="TRADE_TIME:T",
            y=alt.Y("PREDICTED_LOW:Q", scale=alt.Scale(domain=[y_min, y_max])),
            y2="PREDICTED_HIGH:Q",
            tooltip=alt.value(None) 
        )
        
        price_line = alt.Chart(df).mark_line(color="#00cc96", strokeWidth=2).encode(
            x=alt.X("TRADE_TIME:T", title="Time (PKT)"),
            y=alt.Y("TRADE_PRICE:Q", title=f"{symbol} Price (USDT)",
                    scale=alt.Scale(domain=[y_min, y_max]),
                    axis=alt.Axis(format=",.0f", labelFontSize=9, labelLimit=100, tickCount=5)),
            tooltip=alt.value(None) 
        )

        selectors = alt.Chart(df).mark_rule(opacity=0, strokeWidth=30).encode(
            x='TRADE_TIME:T',
            tooltip=[
                alt.Tooltip("TRADE_TIME:T", title="Time"),
                alt.Tooltip("TRADE_PRICE:Q", title="Price", format=",.2f"),
                alt.Tooltip("PREDICTED_LOW:Q", title="Band Low", format=",.0f"),
                alt.Tooltip("PREDICTED_HIGH:Q", title="Band High", format=",.0f")
            ]
        ).add_params(nearest)

        points = price_line.mark_point(color="#00cc96", size=60, filled=True).encode(
            opacity=alt.condition(nearest, alt.value(1), alt.value(0)),
            tooltip=alt.value(None)
        )

        rules = alt.Chart(df).mark_rule(color='gray', strokeDash=[3, 3]).encode(
            x='TRADE_TIME:T',
            tooltip=alt.value(None)
        ).transform_filter(nearest)

        alert_pts = df[df["PRICE_STATUS"] != "NORMAL"]
        layers = [band, price_line, selectors, points, rules]
        
        if not alert_pts.empty:
            alerts_layer = alt.Chart(alert_pts).mark_point(color="red", size=100, shape="cross").encode(
                x="TRADE_TIME:T", y="TRADE_PRICE:Q",
                tooltip=[
                    alt.Tooltip("TRADE_TIME:T", title="Alert Time"),
                    alt.Tooltip("TRADE_PRICE:Q", title="Price", format=",.2f")
                ]
            )
            layers.append(alerts_layer)

        chart = alt.layer(*layers).properties(
            height=450,
            padding={"left": 70, "top": 10, "right": 10, "bottom": 10}
        ).configure_axis(
            labelPadding=8
        )

        # Render chart with a unique key per symbol to force fresh component mounting
        st.altair_chart(chart, use_container_width=True, key=f"main_chart_{symbol}")

        # ==========================================
        # 7. Deviation Chart
        # ==========================================
        df["DEVIATION_PCT"] = ((df["TRADE_PRICE"] - df["PREDICTED_LOW"]) /
                                (df["PREDICTED_HIGH"] - df["PREDICTED_LOW"]) * 100)

        st.subheader(f"{symbol} Position Within Predicted Band (%)")
        deviation_chart = alt.Chart(df).mark_line(color="orange").encode(
            x=alt.X("TRADE_TIME:T", title="Time (PKT)"),
            y=alt.Y("DEVIATION_PCT:Q", title="% of Band (0=low, 100=high)")
        ).properties(height=200)
        st.altair_chart(deviation_chart, use_container_width=True, key=f"dev_chart_{symbol}")

        # ==========================================
        # 8. Alert History Table
        # ==========================================
        st.subheader(f"Recent Alerts ({symbol})")
        
        alerts_query = f"""
            SELECT * 
            FROM CRYPTO_PULSE_DB.PUBLIC.PRICE_ALERTS 
            WHERE symbol = '{symbol}' 
            ORDER BY alert_triggered_at DESC 
            LIMIT 20
        """
        alerts = session.sql(alerts_query).to_pandas()

        if alerts.empty:
            st.info(f"No alerts yet — {symbol} price is within predicted range.")
        else:
            st.dataframe(alerts, use_container_width=True)

if st.button("Refresh now"):
    st.rerun()