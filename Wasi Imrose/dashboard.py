import streamlit as st
import pandas as pd
import numpy as np
import plotly.graph_objects as go

# Note: Page Configurations were obtained from Streamlit documentation

# Page Configuration

st.set_page_config(
    page_title="HFT Pipeline Monitor",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Styling

st.markdown("""
<style>
@import url('https://fonts.googleapis.com/css2?family=Space+Mono:wght@400;700&family=DM+Sans:wght@300;400;600&display=swap');

html, body, [class*="css"] {
    font-family: 'DM Sans', sans-serif;
    background-color: #080c18;
    color: #cbd5e1;
}
section[data-testid="stSidebar"] {
    background-color: #0a0f1e;
    border-right: 1px solid #1a2744;
}
[data-testid="stMetric"] {
    background: #0d1428;
    border: 1px solid #1a2744;
    border-radius: 8px;
    padding: 16px 20px;
}
[data-testid="stMetricLabel"] {
    font-family: 'Space Mono', monospace;
    font-size: 0.65rem !important;
    color: #475569 !important;
    text-transform: uppercase;
    letter-spacing: 0.12em;
}
[data-testid="stMetricValue"] {
    font-family: 'Space Mono', monospace;
    font-size: 1.5rem !important;
    color: #38bdf8 !important;
}
h1 { font-family: 'Space Mono', monospace !important; color: #38bdf8 !important; font-size: 1.3rem !important; }
h2, h3 { font-family: 'Space Mono', monospace !important; color: #7dd3fc !important; font-size: 0.85rem !important; text-transform: uppercase; letter-spacing: 0.1em; }
hr { border-color: #1a2744; margin: 1rem 0; }
</style>
""", unsafe_allow_html=True)

# Chart Theme

BASE_LAYOUT = dict(
    paper_bgcolor="#080c18",
    plot_bgcolor="#0d1428",
    font=dict(family="Space Mono, monospace", color="#64748b", size=10),
    xaxis=dict(gridcolor="#1a2744", zerolinecolor="#1a2744", color="#64748b"),
    yaxis=dict(gridcolor="#1a2744", zerolinecolor="#1a2744", color="#64748b"),
    margin=dict(l=55, r=20, t=45, b=45),
    legend=dict(bgcolor="rgba(0,0,0,0)", bordercolor="#1a2744", font=dict(size=10)),
    height=320,
)

# Load CSV

def load_csv(f):
    try:
        df = pd.read_csv(f)
        df["burst"] = df["burst"].astype(str).str.lower().isin(["true", "1"])
        df["latency_ms"] = pd.to_numeric(df["latency_ms"], errors="coerce")
        df["throughput_msg_per_sec"] = pd.to_numeric(df["throughput_msg_per_sec"], errors="coerce")
        df["p95_latency_ms"] = pd.to_numeric(df["p95_latency_ms"], errors="coerce")
        df["p99_latency_ms"] = pd.to_numeric(df["p99_latency_ms"], errors="coerce")
        return df
    except Exception as e:
        st.error(f"Failed to load file: {e}")
        return pd.DataFrame()

# Charts

def latency_chart(df, label=""):
    fig = go.Figure()

    fig.add_trace(go.Scatter(
        x=df["msg_id"], y=df["latency_ms"],
        mode="lines", name="Latency",
        line=dict(color="#818cf8", width=1.2),
        fill="tozeroy", fillcolor="rgba(129,140,248,0.06)"
    ))

    p95 = df["p95_latency_ms"].dropna()
    if not p95.empty:
        fig.add_trace(go.Scatter(
            x=df.loc[p95.index, "msg_id"], y=p95,
            mode="lines", name="P95",
            line=dict(color="#f97316", width=1.5, dash="dash")
        ))

    p99 = df["p99_latency_ms"].dropna()
    if not p99.empty:
        fig.add_trace(go.Scatter(
            x=df.loc[p99.index, "msg_id"], y=p99,
            mode="lines", name="P99",
            line=dict(color="#ef4444", width=1.5, dash="dot")
        ))

    title = f"LATENCY (ms){' — ' + label if label else ''}"
    fig.update_layout(**BASE_LAYOUT, title=dict(text=title, font=dict(size=11, color="#7dd3fc")))
    fig.update_yaxes(title_text="Latency (ms)")
    fig.update_xaxes(title_text="Message ID")
    return fig


def throughput_chart(df, label=""):
    fig = go.Figure()

    fig.add_trace(go.Scatter(
        x=df["msg_id"], y=df["throughput_msg_per_sec"],
        mode="lines", name="Throughput",
        line=dict(color="#4ade80", width=1.5),
        fill="tozeroy", fillcolor="rgba(74,222,128,0.06)"
    ))

    title = f"THROUGHPUT (msg/s){' — ' + label if label else ''}"
    fig.update_layout(**BASE_LAYOUT, title=dict(text=title, font=dict(size=11, color="#7dd3fc")))
    fig.update_yaxes(title_text="msg/s")
    fig.update_xaxes(title_text="Message ID")
    return fig


def comparison_table(experiments: dict): # Note: This was written with guidance from Claude AI
    rows = []
    for name, df in experiments.items():
        if df.empty:
            continue
        p95 = df["p95_latency_ms"].dropna()
        p99 = df["p99_latency_ms"].dropna()
        rows.append({
            "Experiment": name,
            "Messages": len(df),
            "Avg Latency (ms)": round(df["latency_ms"].mean(), 2),
            "Min Latency (ms)": round(df["latency_ms"].min(), 2),
            "Max Latency (ms)": round(df["latency_ms"].max(), 2),
            "P95 Latency (ms)": round(p95.iloc[-1], 2) if not p95.empty else "—",
            "P99 Latency (ms)": round(p99.iloc[-1], 2) if not p99.empty else "—",
            "Avg Throughput (msg/s)": round(df["throughput_msg_per_sec"].mean(), 2),
        })
    return pd.DataFrame(rows)

# Sidebar

with st.sidebar:
    st.markdown("### HFT PIPELINE")
    st.markdown("---")
    st.markdown("**Upload experiment CSVs**")
    st.caption("Each file = one experiment. Label them clearly (e.g. baseline.csv, burst.csv)")

    uploaded_files = st.file_uploader(
        "Upload CSV files",
        type="csv",
        accept_multiple_files=True
    )

    st.markdown("---")
    st.markdown("**Chart options**")
    show_individual = st.checkbox("Show charts per experiment", value=True)
    show_comparison = st.checkbox("Show comparison table", value=True)

# Header

st.markdown("# HFT PIPELINE MONITOR")
st.markdown("Kafka streaming pipeline — experiment analysis dashboard")
st.markdown("---")

# Main

if not uploaded_files:
    st.info("Upload one or more experiment CSV files from the sidebar to begin.")
    st.markdown("""
    **Expected files:**
    - `baseline_metrics.csv`
    - `burst_metrics.csv`
    - `scaling_metrics.csv`
    - `fault_metrics.csv`
    """)
else:
    # load all files
    experiments = {}
    for f in uploaded_files:
        name = f.name.replace(".csv", "").replace("_", " ").title()
        experiments[name] = load_csv(f)

    
    # Summary Metrics
    
    primary_name = list(experiments.keys())[0]
    primary_df = experiments[primary_name]

    st.markdown(f"### {primary_name.upper()} — SUMMARY")
    p95 = primary_df["p95_latency_ms"].dropna()
    p99 = primary_df["p99_latency_ms"].dropna()

    c1, c2, c3, c4, c5 = st.columns(5)
    c1.metric("Messages", f"{len(primary_df):,}")
    c2.metric("Avg Latency", f"{primary_df['latency_ms'].mean():.1f} ms")
    c3.metric("P95 Latency", f"{p95.iloc[-1]:.1f} ms" if not p95.empty else "—")
    c4.metric("P99 Latency", f"{p99.iloc[-1]:.1f} ms" if not p99.empty else "—")
    c5.metric("Avg Throughput", f"{primary_df['throughput_msg_per_sec'].mean():.1f} msg/s")

    st.markdown("---")

    
    # Charts per experiment
    
    if show_individual:
        for name, df in experiments.items():
            if df.empty:
                continue
            st.markdown(f"### {name.upper()}")
            col1, col2 = st.columns(2)
            with col1:
                st.plotly_chart(latency_chart(df, name), use_container_width=True)
            with col2:
                st.plotly_chart(throughput_chart(df, name), use_container_width=True)

        st.markdown("---")

    
    # Comparison Table
    
    if show_comparison and len(experiments) > 0:
        st.markdown("### EXPERIMENT COMPARISON")
        comp = comparison_table(experiments)
        st.dataframe(
            comp,
            use_container_width=True,
            hide_index=True
        )

    
    # Raw Data
    
    with st.expander("View raw data"):
        selected = st.selectbox("Experiment", list(experiments.keys()))
        st.dataframe(experiments[selected], use_container_width=True)
