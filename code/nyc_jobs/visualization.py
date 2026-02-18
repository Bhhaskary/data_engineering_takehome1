"""Visualization helpers."""

from pathlib import Path

from pyspark.sql import DataFrame


def _try_import_matplotlib():
    import matplotlib.pyplot as plt

    return plt


def _save_fig(fig, output_path: Path) -> None:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    fig.savefig(output_path, dpi=150)


def plot_top_categories(df: DataFrame, output_path: Path) -> None:
    plt = _try_import_matplotlib()
    pdf = df.toPandas()
    fig, ax = plt.subplots(figsize=(10, 6))
    ax.barh(pdf["Job Category"], pdf["job_postings"])
    ax.invert_yaxis()
    ax.set_title("Top 10 Job Categories by Posting Count")
    ax.set_xlabel("Job Postings")
    fig.tight_layout()
    _save_fig(fig, output_path)
    plt.close(fig)


def plot_salary_by_category(df: DataFrame, output_path: Path, top_n: int = 10) -> None:
    plt = _try_import_matplotlib()
    pdf = df.orderBy(df["avg_salary"].desc()).limit(top_n).toPandas()
    fig, ax = plt.subplots(figsize=(12, 6))
    x_positions = list(range(len(pdf)))
    ax.bar(x_positions, pdf["avg_salary"])
    ax.set_title("Average Annualized Salary by Job Category (Top 10)")
    ax.set_ylabel("Average Salary (USD)")
    ax.set_xticks(x_positions)
    ax.set_xticklabels(pdf["Job Category"], rotation=45, ha="right")
    fig.tight_layout()
    _save_fig(fig, output_path)
    plt.close(fig)


def plot_salary_distribution_box(df: DataFrame, output_path: Path, top_n: int = 10) -> None:
    plt = _try_import_matplotlib()
    pdf = df.orderBy(df["median_salary"].desc()).limit(top_n).toPandas()
    fig, ax = plt.subplots(figsize=(12, 6))

    for i, row in pdf.iterrows():
        ax.vlines(i, row["min_salary"], row["max_salary"], color="#9ca3af", linewidth=2)
        ax.vlines(i, row["p25_salary"], row["p75_salary"], color="#1f77b4", linewidth=8)
        ax.scatter(i, row["median_salary"], color="#111827", s=20, zorder=5)

    ax.set_title("Salary Distribution by Job Category (Top 10 by Median)")
    ax.set_ylabel("Annualized Salary (USD)")
    ax.set_xticks(list(range(len(pdf))))
    ax.set_xticklabels(pdf["Job Category"], rotation=45, ha="right")
    fig.tight_layout()
    _save_fig(fig, output_path)
    plt.close(fig)


def plot_avg_salary_per_agency(df: DataFrame, output_path: Path, top_n: int = 15) -> None:
    plt = _try_import_matplotlib()
    pdf = df.orderBy(df["avg_salary"].desc()).limit(top_n).toPandas()
    fig, ax = plt.subplots(figsize=(12, 7))
    ax.barh(pdf["Agency"], pdf["avg_salary"], color="#2ca02c")
    ax.invert_yaxis()
    ax.set_title("Average Salary per Agency (Last 2 Years, Top 15)")
    ax.set_xlabel("Average Annualized Salary (USD)")
    fig.tight_layout()
    _save_fig(fig, output_path)
    plt.close(fig)


def plot_highest_paid_skills(df: DataFrame, output_path: Path, top_n: int = 15) -> None:
    plt = _try_import_matplotlib()
    pdf = df.orderBy(df["avg_salary"].desc()).limit(top_n).toPandas()
    fig, ax = plt.subplots(figsize=(10, 6))
    ax.barh(pdf["skill"], pdf["avg_salary"], color="#ff7f0e")
    ax.invert_yaxis()
    ax.set_title("Highest Paid Skills (Top 15)")
    ax.set_xlabel("Average Annualized Salary (USD)")
    fig.tight_layout()
    _save_fig(fig, output_path)
    plt.close(fig)


def plot_highest_salary_roles(df: DataFrame, output_path: Path, top_n: int = 15) -> None:
    plt = _try_import_matplotlib()
    pdf = df.orderBy(df["salary_to_annualized"].desc()).limit(top_n).toPandas()
    fig, ax = plt.subplots(figsize=(12, 7))
    labels = (pdf["Agency"] + " | " + pdf["Business Title"]).tolist()
    ax.barh(labels, pdf["salary_to_annualized"], color="#d62728")
    ax.invert_yaxis()
    ax.set_title("Highest Salary Job Posting per Agency (Top 15 by Max Salary)")
    ax.set_xlabel("Maximum Annualized Salary (USD)")
    fig.tight_layout()
    _save_fig(fig, output_path)
    plt.close(fig)


def plot_degree_salary_correlation(df: DataFrame, output_path: Path) -> None:
    plt = _try_import_matplotlib()
    value = float(df.collect()[0]["degree_salary_corr"])
    fig, ax = plt.subplots(figsize=(6, 4))
    ax.bar(["Correlation"], [value], color="#6b7280")
    ax.axhline(0, color="black", linewidth=1)
    ax.set_ylim(-1, 1)
    ax.set_title("Degree vs Salary Correlation")
    ax.text(0, value + (0.05 if value >= 0 else -0.08), f"{value:.3f}", ha="center")
    fig.tight_layout()
    _save_fig(fig, output_path)
    plt.close(fig)
