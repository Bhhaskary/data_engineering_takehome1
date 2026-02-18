"""CLI entrypoint for the NYC jobs pipeline."""

import argparse

from nyc_jobs.pipeline import run


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run NYC jobs assessment pipeline")
    parser.add_argument(
        "--cluster",
        action="store_true",
        help="Use Spark master configured for docker cluster (spark://master:7077).",
    )
    return parser.parse_args()


if __name__ == "__main__":
    args = parse_args()
    run(use_cluster_master=args.cluster)

