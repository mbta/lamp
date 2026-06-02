"""
Interactive marimo frontend for bus prediction analyzer.

Provides:
- Data loading (trip_updates, vehicle_positions parquets)
- Real-time filtering (route, time_of_day, prediction accuracy)
- Metrics visualization (accuracy tables, bias detection, performance by group)
- Configuration control (IBI bins, time-of-day bins, thresholds)

To run: marimo run analysis/bus_prediction_analyzer_marimo.py
"""

import marimo

__generated_with = "0.16.5"
app = marimo.App(width="medium")


@app.cell
def _():
    import sys
    import os
    import marimo as mo

    cwd = os.getcwd()
    exe = sys.executable
    paths = sys.path[:5]

    mo.md(f"""
    ### 🔧 Startup Diagnostics
    - **cwd:** `{cwd}`
    - **interpreter:** `{exe}`
    - **sys.path (first 5):**
    ```
    {chr(10).join(paths)}
    ```
    """)
    return (mo,)


@app.cell
def _():
    import polars as pl
    import plotly.express as px
    from datetime import datetime

    from bus_prediction_analyzer_utils import (
        default_config,
        run_analysis,
    )
    return datetime, default_config, pl, px, run_analysis


@app.cell
def _(mo):
    mo.md(
        """
    # 🚌 Bus Prediction Analyzer

    Interactive analysis of GTFS-RT prediction accuracy using the **IBI (Itinerary-Based Indicator)** metric.
    """
    )
    return


@app.cell
def _(mo):
    mo.md("## 📂 Step 1: Load Data")

    tu_path = mo.ui.text(
        label="Trip Updates Parquet Path",
        value="s3://lamp-data/gtfs-rt/trip_updates.parquet",
    )

    vp_path = mo.ui.text(
        label="Vehicle Positions Parquet Path",
        value="s3://lamp-data/gtfs-rt/vehicle_positions.parquet",
    )

    load_button = mo.ui.button(label="📂 Load Data")

    mo.vstack([tu_path, vp_path, load_button])
    return load_button, tu_path, vp_path


@app.cell
def _(load_button, mo, pl, tu_path, vp_path):
    @mo.cache
    def load_data(tu_p: str, vp_p: str, clicked: bool):
        """Load parquet files when button is clicked."""
        if not clicked:
            return None, None, "Waiting for data..."
        try:
            tu_df = pl.scan_parquet(tu_p).collect()
            vp_df = pl.scan_parquet(vp_p).collect()
            return tu_df, vp_df, None
        except Exception as e:
            return None, None, str(e)

    tu_df, vp_df, load_error = load_data(tu_path.value, vp_path.value, load_button.clicked)

    if load_error:
        mo.md(f"❌ **Error:** {load_error}")
    elif tu_df is None or vp_df is None:
        mo.md("⏳ **Waiting for data...** Click 'Load Data' button above.")
    else:
        mo.md(f"""
        ✅ **Data Loaded**
        - Trip Updates: {tu_df.shape[0]:,} rows
        - Vehicle Positions: {vp_df.shape[0]:,} rows
        """)
    return tu_df, vp_df


@app.cell
def _(default_config, mo):
    mo.md("## ⚙️ Step 2: Configuration")

    config = default_config()

    mo.md(f"""
    **Active Configuration:**
    - IBI Bins: {', '.join(b.name for b in config.ibi_bins)}
    - Time-of-Day Bins: {', '.join(b.name for b in config.time_of_day_bins)}
    - Ignore Threshold: {config.ignore_threshold_sec}s
    """)
    return (config,)


@app.cell
def _(mo):
    mo.md("## 🚀 Step 3: Run Analysis")
    run_button = mo.ui.button(label="🚀 Run Analysis")
    run_button
    return (run_button,)


@app.cell
def _(config, mo, pl, run_analysis, run_button, tu_df, vp_df):
    @mo.cache
    def analyze(tu, vp, cfg, clicked: bool):
        """Run analysis when button is clicked."""
        if not clicked or tu is None or vp is None:
            return None, "Waiting to run analysis..."
        try:
            results = run_analysis(tu, vp, cfg)
            return results, None
        except Exception as e:
            return None, str(e)

    results, analysis_error = analyze(tu_df, vp_df, config, run_button.clicked)

    if analysis_error:
        mo.md(f"❌ {analysis_error}")
    elif results is None:
        mo.md("⏳ **Ready to analyze.** Click 'Run Analysis' button above.")
    else:
        joined = results.get("joined", pl.DataFrame())
        mo.md(f"✅ **Analysis Complete** — {joined.shape[0]:,} predictions analyzed")
    return (results,)


@app.cell
def _(mo, pl, px, results):
    mo.stop(results is None)

    mo.md("## 📊 Step 4: Results")
    mo.md("### Overall IBI Accuracy")

    ibi_acc = results.get("ibi_accuracy", pl.DataFrame())

    if not ibi_acc.is_empty():
        overall = ibi_acc.filter(pl.col("ibi_bin") == "overall")
        if not overall.is_empty():
            acc_pct = overall["accuracy_pct"][0]
            mo.md(f"# {acc_pct:.1f}%")

        per_bin = ibi_acc.filter(pl.col("ibi_bin") != "overall")
        if not per_bin.is_empty():
            fig = px.bar(
                per_bin.to_pandas(),
                x="ibi_bin",
                y="accuracy_pct",
                title="Accuracy by Prediction Window",
                labels={"ibi_bin": "Window", "accuracy_pct": "Accuracy (%)"},
                color="accuracy_pct",
                color_continuous_scale="RdYlGn",
                range_color=[0, 100],
            )
            mo.vstack([mo.plotly(fig), mo.md("**Per-Bin Details**"), mo.ui.table(per_bin.to_pandas())])
    return


@app.cell
def _(mo, pl, px, results):
    mo.stop(results is None)

    mo.md("### Route-Level Performance")
    route_acc = results.get("route_accuracy", pl.DataFrame())

    if not route_acc.is_empty():
        route_sorted = route_acc.sort("accuracy_pct", descending=True)
        fig = px.bar(
            route_sorted.to_pandas(),
            x="route_id",
            y="accuracy_pct",
            title="Accuracy by Route",
            labels={"route_id": "Route", "accuracy_pct": "Accuracy (%)"},
            color="accuracy_pct",
            color_continuous_scale="RdYlGn",
            range_color=[0, 100],
        )
        mo.plotly(fig)
    return


@app.cell
def _(mo, pl, px, results):
    mo.stop(results is None)

    mo.md("### Time-of-Day Performance")
    tod_acc = results.get("time_of_day_accuracy", pl.DataFrame())

    if not tod_acc.is_empty():
        fig = px.bar(
            tod_acc.to_pandas(),
            x="time_of_day_bin",
            y="accuracy_pct",
            title="Accuracy by Time of Day",
            labels={"time_of_day_bin": "Period", "accuracy_pct": "Accuracy (%)"},
            color="accuracy_pct",
            color_continuous_scale="RdYlGn",
            range_color=[0, 100],
        )
        mo.plotly(fig)
    return


@app.cell
def _(mo, pl, px, results):
    mo.stop(results is None)

    mo.md("### Prediction Bias Detection")
    bias = results.get("bias", pl.DataFrame())

    if not bias.is_empty():
        bias_df = bias.to_pandas()
        fig = px.scatter(
            bias_df,
            x="mean_error",
            y="std_error",
            color="is_biased",
            hover_data=["trip_id"],
            title="Trip Bias: Mean Error vs Consistency",
            labels={
                "mean_error": "Mean Error (s)",
                "std_error": "Std Dev (s)",
                "is_biased": "Biased?",
            },
            color_discrete_map={True: "#ef553b", False: "#00cc96"},
        )
        fig.add_vline(x=30, line_dash="dash", line_color="gray", opacity=0.3)
        fig.add_vline(x=-30, line_dash="dash", line_color="gray", opacity=0.3)
        fig.add_hline(y=20, line_dash="dash", line_color="gray", opacity=0.3)
        mo.plotly(fig)

        biased_count = bias.filter(pl.col("is_biased")).shape[0]
        mo.md(f"**Biased Trips:** {biased_count} of {bias.shape[0]}")

        if biased_count > 0:
            biased_sorted = (
                bias.filter(pl.col("is_biased"))
                .with_columns(abs_mean=pl.col("mean_error").abs())
                .sort("abs_mean", descending=True)
                .head(10)
            )
            mo.ui.table(biased_sorted.to_pandas())
    return


@app.cell
def _(mo, results):
    mo.stop(results is None)

    mo.md("### Export Results")

    export_format = mo.ui.radio(
        options=["Parquet", "CSV"],
        value="Parquet",
    )

    export_button = mo.ui.button(label="💾 Export")

    mo.vstack([export_format, export_button])
    return export_button, export_format


@app.cell
def _(datetime, export_button, export_format, mo, pl, results):
    mo.stop(results is None)

    if export_button.clicked:
        joined = results.get("joined", pl.DataFrame())
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = f"bus_prediction_analysis_{timestamp}"
        try:
            if export_format.value == "Parquet":
                joined.write_parquet(f"/tmp/{filename}.parquet")
                mo.toast(f"✅ Exported: {filename}.parquet")
            else:
                joined.write_csv(f"/tmp/{filename}.csv")
                mo.toast(f"✅ Exported: {filename}.csv")
        except Exception as e:
            mo.toast(f"❌ Failed: {e}")
    return


if __name__ == "__main__":
    app.run()
