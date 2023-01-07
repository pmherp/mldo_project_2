#!/usr/bin/env python
"""
This step does all basic preprocessing of the raw data. Author: Philip Herp. Date: 07.01.2023
"""
import argparse
import logging
import wandb
import pandas as pd

#from wandb_utils.log_artifact import log_artifact


logging.basicConfig(level=logging.INFO, format="%(asctime)-15s %(message)s")
logger = logging.getLogger()

def go(args):

    run = wandb.init(job_type="preprocess_data")
    run.config.update(args)

    # Download input artifact. This will also log that this script is using this
    # particular version of the artifact
    # artifact_local_path = run.use_artifact(args.input_artifact).file()

    ######################
    # YOUR CODE HERE     #
    ######################
    logger.info(f'Downloading input artifact: {args.input_artifact}')
    artifact = run.use_artifact(args.input_artifact)
    artifact_path = artifact.file()
    raw_data = pd.read_csv(artifact_path)

    logger.info(f'Preprocessing data: {args.input_artifact}')
    # Drop outliers
    idx = raw_data['price'].between(args.min_price, args.max_price)
    raw_data = raw_data[idx].copy()
    # Convert last_review to datetime
    raw_data['last_review'] = pd.to_datetime(raw_data['last_review'])

    logger.info(f'Saving preprocessed data: {args.output_artifact}')
    raw_data.to_csv(args.output_artifact, index=False)

    logger.info(f'Creating artifact: {args.output_artifact}')
    artifact = wandb.Artifact(
        args.output_artifact,
        type=args.output_type,
        description=args.output_description,
    )

    logger.info(f'Saving artifact: {args.output_artifact}')
    artifact.add_file(args.output_artifact)
    run.log_artifact(artifact)


if __name__ == "__main__":

    parser = argparse.ArgumentParser(
        description="This step does basic preprocessing of the raw data"#, 
        #fromfile_prefix_chars='@'
    )


    parser.add_argument(
        "--input_artifact", 
        type=str,
        help='artifact to input into operation, usually from previous step',
        required=True
    )

    parser.add_argument(
        "--output_artifact", 
        type=str,
        help='name of the artifact produced by operation',
        required=True
    )

    parser.add_argument(
        "--output_type", 
        type=str,
        help='type of the output artifact',
        required=True
    )

    parser.add_argument(
        "--output_description", 
        type=str,
        help='description for the output artifact',
        required=True
    )

    parser.add_argument(
        "--min_price", 
        type=float,
        help='minimum price to consider',
        required=True
    )

    parser.add_argument(
        "--max_price", 
        type=float,
        help='maximum price to consider',
        required=True
    )


    args = parser.parse_args()

    go(args)
