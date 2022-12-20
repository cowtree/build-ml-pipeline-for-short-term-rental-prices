#!/usr/bin/env python
"""
ownload from W&B the raw dataset and apply some basic data cleaning, exporting the result to a new artifact
"""
import argparse
import logging
import pandas as pd
import wandb


logging.basicConfig(level=logging.INFO, format="%(asctime)-15s %(message)s")
logger = logging.getLogger()


def go(args):

    run = wandb.init(job_type="basic_cleaning")
    run.config.update(args)

    # Download input artifact. This will also log that this script is using this
    # particular version of the artifact
    # artifact_local_path = run.use_artifact(args.input_artifact).file()

    ######################
    # YOUR CODE HERE     #
    ######################
    logger.info("Downloading artifact")
    artifact_local_path = run.use_artifact(args.input_artifact).file()

    logger.info("Loading data")
    df = pd.read_csv(artifact_local_path)

    # Drop outliers
    logger.info("Dropping outliers")
    idx = df['price'].between(args.min_price, args.max_price)
    df = df[idx].copy()
    # Convert last_review to datetime
    df['last_review'] = pd.to_datetime(df['last_review'])

    # Save cleaned data
    logger.info("Saving cleaned data")
    idx = df['longitude'].between(-74.25, -73.50) & df['latitude'].between(40.5, 41.2)
    df = df[idx].copy()
    df.to_csv("cleaned_data.csv", index=False)

    artifact = wandb.Artifact(
        name=args.output_artifact,
        type=args.output_type,
        description=args.output_description,
    )
    artifact.add_file("cleaned_data.csv")
    run.log_artifact(artifact)

if __name__ == "__main__":

    parser = argparse.ArgumentParser(description="A very basic data cleaning")


    parser.add_argument(
        "--input_artifact", 
        type=str,
        help="The name of the artifact to download (sample.csv)",
        required=True
    )

    parser.add_argument(
        "--output_artifact", 
        type=str,
        help="The name of the artifact to upload",
        required=True
    )

    parser.add_argument(
        "--output_type", 
        type=str,
        help="The type of the artifact to upload (in csv format)",
        required=True
    )

    parser.add_argument(
        "--output_description", 
        type=str,
        help="The description of the artifact to upload",
        required=True
    )

    parser.add_argument(
        "--min_price", 
        type=float,
        help="The minimum price to keep in the dataset",
        required=True
    )

    parser.add_argument(
        "--max_price", 
        type=float,
        help="The maximum price to keep in the dataset",
        required=True
    )
    args = parser.parse_args()

    go(args)
