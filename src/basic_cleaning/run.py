#!/usr/bin/env python
"""
Download from W&B the raw dataset and apply some basic data cleaning, exporting the result to a new artifact
"""
import argparse
import logging
import os

import pandas as pd

import wandb

logging.basicConfig(level=logging.INFO, format="%(asctime)-15s %(message)s")
logger = logging.getLogger()


def go(args):

    run = wandb.init(job_type="basic_cleaning")
    run.config.update(args)

    # Download input artifact. This will also log that this script is using this
    # particular version of the artifact
    artifact_local_path = run.use_artifact(args.input_artifact).file()

    ######################
    # YOUR CODE HERE     #
    ######################
    dataframe = pd.read_csv(artifact_local_path, index_col="id")
    minimum_price = args.min_price
    maximum_price = args.max_price
    idx = dataframe["price"].between(minimum_price, maximum_price)

    dataframe = dataframe[idx].copy()
    idx = dataframe["longitude"].between(-74.25, -73.50) & dataframe["latitude"].between(40.5, 41.2)
    dataframe = dataframe[idx].copy()
    dataframe["last_review"] = pd.to_datetime(dataframe["last_review"])

    dataframe.to_csv(args.output_artifact)

    artifact = wandb.Artifact(
        args.output_artifact,
        type=args.output_type,
        description=args.output_description,
    )
    artifact.add_file("clean_sample.csv")
    run.log_artifact(artifact)
    artifact.wait()


if __name__ == "__main__":

    parser = argparse.ArgumentParser(description="A very basic data cleaning")

    parser.add_argument("--input_artifact", type=str, help="Input artifact name", required=True)

    parser.add_argument("--output_artifact", type=str, help="Output artifact name", required=True)

    parser.add_argument("--output_type", type=str, help="Output artifact type", required=True)

    parser.add_argument(
        "--output_description", type=str, help="Output artifact description", required=True
    )

    parser.add_argument("--min_price", type=int, help="Minimum price", required=True)

    parser.add_argument("--max_price", type=int, help="Maximum price", required=True)

    args = parser.parse_args()

    go(args)
