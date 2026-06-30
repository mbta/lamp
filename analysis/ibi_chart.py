"""Plotly chart builders for the IBI ETA accuracy benchmark."""

import plotly.graph_objects as go
import polars as pl
import plotly.express as px

from bus_prediction_analyzer_utils import AnalyzerConfig, default_config

BIN_SPECS: list[dict] = [
    {"label": "0-3 min", "min_s": 0, "max_s": 180, "early_s": 30, "late_s": 90},
    {"label": "3-6 min", "min_s": 180, "max_s": 360, "early_s": 60, "late_s": 150},
    {"label": "6-10 min", "min_s": 360, "max_s": 600, "early_s": 60, "late_s": 210},
    {"label": "10-15 min", "min_s": 600, "max_s": 900, "early_s": 90, "late_s": 270},
]


def _fmt_tolerance(seconds: int, positive: bool) -> str:
    m, s = abs(seconds) // 60, abs(seconds) % 60
    sign = "+" if positive else "-"
    if m and s:
        return f"{sign}{m}m {s}s"
    return f"{sign}{m}m" if m else f"{sign}{s}s"


def _add_threshold_columns(df: pl.DataFrame, config: AnalyzerConfig) -> pl.DataFrame:
    """Add early_s and late_s columns based on ibi_bin."""
    early_expr = pl.lit(None, dtype=pl.Float64)
    late_expr = pl.lit(None, dtype=pl.Float64)
    for b in config.ibi_bins:
        early_expr = (
            pl.when(pl.col("ibi_bin") == b.name)
            .then(pl.lit(b.early_threshold_sec))
            .otherwise(early_expr)
        )
        late_expr = (
            pl.when(pl.col("ibi_bin") == b.name)
            .then(pl.lit(b.late_threshold_sec))
            .otherwise(late_expr)
        )
    return df.with_columns(early_s=early_expr, late_s=late_expr)


def make_benchmark_chart(
    predictions_df: pl.DataFrame, config: AnalyzerConfig | None = None
) -> go.Figure:
    """Build the IBI/TransitApp ETA accuracy scatter chart.

    Args:
        predictions_df: DataFrame output from
            :func:`~analysis.bus_prediction_analyzer_utils.is_prediction_accurate`,
            filtered to rows where ``ibi_bin`` is not null. Expected columns:
            ``ibi_bin``, ``is_accurate``, ``prediction_error_meas_sec``,
            ``prediction_ahead_meas_sec``.
        config: AnalyzerConfig with IBI bin thresholds. If None, uses default.

    Returns:
        A Plotly :class:`~plotly.graph_objects.Figure`.
    """
    if config is None:
        config = default_config()

    # Filter to rows with valid ibi_bin and add threshold columns
    df = predictions_df.filter(pl.col("ibi_bin").is_not_null())
    df = _add_threshold_columns(df, config)

    # Calculate predicted minutes (seconds until arrival) and error minutes
    pm = -df["prediction_ahead_meas_sec"] / 60
    em = df["prediction_error_meas_sec"] / 60
    accurate = df["is_accurate"]
    too_late = ~accurate & (em > df["late_s"] / 60)
    too_early = ~accurate & (em < -(df["early_s"] / 60))

    # Staircase boundary lines (x-axis is reversed: 15→0)
    xs = [15.0, 10.0, 10.0, 6.0, 6.0, 3.0, 3.0, 0.0]
    y_up = [4.5, 4.5, 3.5, 3.5, 2.5, 2.5, 1.5, 1.5]
    y_lo = [-1.5, -1.5, -1.0, -1.0, -1.0, -1.0, -0.5, -0.5]

    fig = go.Figure()

    # Green tolerance band
    fig.add_trace(
        go.Scatter(
            x=xs,
            y=y_lo,
            mode="lines",
            line=dict(color="rgba(0,168,89,0.75)", width=2),
            fill=None,
            showlegend=False,
            hoverinfo="skip",
        )
    )
    fig.add_trace(
        go.Scatter(
            x=xs,
            y=y_up,
            mode="lines",
            line=dict(color="rgba(0,168,89,0.75)", width=2),
            fill="tonexty",
            fillcolor="rgba(46,204,113,0.18)",
            showlegend=False,
            hoverinfo="skip",
        )
    )

    # Too late — yellow
    xl, yl = pm.filter(too_late).to_list(), em.filter(too_late).to_list()
    if xl:
        fig.add_trace(
            go.Scatter(
                x=xl,
                y=yl,
                mode="markers",
                marker=dict(color="#f1c40f", size=9, line=dict(color="white", width=1)),
                name="Excess wait time — Inaccurate ETA",
            )
        )

    # Accurate — green
    xa, ya = pm.filter(accurate).to_list(), em.filter(accurate).to_list()
    if xa:
        fig.add_trace(
            go.Scatter(
                x=xa,
                y=ya,
                mode="markers",
                marker=dict(color="#2ecc71", size=9, line=dict(color="white", width=1)),
                name="Catch the ride — Accurate ETA",
            )
        )

    # Too early — pink
    xe, ye = pm.filter(too_early).to_list(), em.filter(too_early).to_list()
    if xe:
        fig.add_trace(
            go.Scatter(
                x=xe,
                y=ye,
                mode="markers",
                marker=dict(color="#e91e63", size=9, line=dict(color="white", width=1)),
                name="Miss the ride — Inaccurate ETA",
            )
        )

    # Bin separator lines
    for bx in [3.0, 6.0, 10.0]:
        fig.add_vline(x=bx, line_dash="dot", line_color="#cccccc", line_width=1.5)

    # Arrival line
    fig.add_vline(x=0.0, line_color="#2196f3", line_width=2.5)
    fig.add_annotation(
        x=0.0,
        y=1.0,
        xref="x",
        yref="paper",
        text="<b>ARRIVAL</b>",
        font=dict(color="#2196f3", size=11),
        showarrow=False,
        xanchor="left",
        yanchor="bottom",
    )

    cat_labels = [
        "Where is my ride?",
        "Do I need to hustle?",
        "Do I need to leave now?",
        "Is the vehicle coming?",
    ]
    for i, spec in enumerate(BIN_SPECS):
        xmid = (spec["min_s"] + spec["max_s"]) / 2 / 60
        fig.add_annotation(
            x=xmid,
            y=spec["late_s"] / 60 + 0.18,
            text=_fmt_tolerance(spec["late_s"], positive=True),
            showarrow=False,
            font=dict(size=10, color="#444"),
            yanchor="bottom",
        )
        fig.add_annotation(
            x=xmid,
            y=-spec["early_s"] / 60 - 0.18,
            text=_fmt_tolerance(spec["early_s"], positive=False),
            showarrow=False,
            font=dict(size=10, color="#444"),
            yanchor="top",
        )
        fig.add_annotation(
            x=xmid,
            y=1.04,
            xref="x",
            yref="paper",
            text=f"<i>{cat_labels[i]}</i>",
            showarrow=False,
            font=dict(size=11, color="#777"),
            yanchor="bottom",
        )
        fig.add_annotation(
            x=xmid,
            y=-0.08,
            xref="x",
            yref="paper",
            text=f"{spec['label']} away",
            showarrow=False,
            font=dict(size=11, color="#444"),
            yanchor="top",
        )

    fig.update_layout(
        title="IBI/TransitApp ETA Accuracy Benchmark",
        xaxis=dict(
            title="Time until arrival (minutes)",
            range=[16.0, -0.5],
            tickvals=[0, 3, 6, 10, 15],
            showgrid=False,
            zeroline=False,
        ),
        yaxis=dict(
            title="Prediction error (minutes)",
            range=[-2.2, 5.6],
            zeroline=True,
            zerolinecolor="rgba(180,180,180,0.9)",
            zerolinewidth=1,
            gridcolor="rgba(200,200,200,0.3)",
        ),
        legend=dict(
            orientation="v",
            x=1.01,
            y=0.5,
            xanchor="left",
            bordercolor="#dddddd",
            borderwidth=1,
        ),
        template="plotly_white",
        height=540,
        margin=dict(r=220, t=80, b=80),
    )
    return fig


def make_prediction_timeline_chart(
    df: pl.DataFrame,
    position_col: str = "stop_sequence",
    prediction_time_col: str = "prediction_timestamp",
    accurate_col: str = "is_accurate",
) -> go.Figure:
    """Scatter plot of position vs prediction time, color-coded by accuracy.

    Args:
        df: DataFrame with prediction data.
        position_col: Column for x-axis (e.g., stop_sequence).
        prediction_time_col: Column for y-axis (timestamp or seconds).
        accurate_col: Boolean column indicating prediction accuracy.
    """
    # Sort by prediction time for increasing order on y-axis
    df = df.sort(prediction_time_col)

    # Create accuracy label for coloring
    df = df.with_columns(
        accuracy_label=pl.when(pl.col(accurate_col))
        .then(pl.lit("Accurate"))
        .otherwise(pl.lit("Inaccurate"))
    )

    fig = px.scatter(
        df.to_pandas(),
        x=position_col,
        y=prediction_time_col,
        color="accuracy_label",
        color_discrete_map={"Accurate": "#2ecc71", "Inaccurate": "#e74c3c"},
        labels={
            position_col: "Position (Stop Sequence)",
            prediction_time_col: "Prediction Time",
            "accuracy_label": "Prediction Result",
        },
    )

    fig.update_traces(marker=dict(size=8, line=dict(width=1, color="white")))
    fig.update_layout(
        title="Prediction Accuracy by Position and Time",
        xaxis_title="Position",
        yaxis_title="Prediction Time",
        legend_title="Accuracy",
        hovermode="closest",
    )

    return fig