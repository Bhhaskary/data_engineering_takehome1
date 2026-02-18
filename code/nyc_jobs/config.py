"""Project configuration constants."""

from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
DATASET_PATH = Path("/dataset/nyc-jobs.csv")
OUTPUT_DIR = PROJECT_ROOT / "output"
CHARTS_DIR = OUTPUT_DIR / "charts"

APP_NAME = "pyspark-assessment"
SPARK_MASTER = "spark://master:7077"
