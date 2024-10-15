import logging
import boto3
from botocore.config import Config
import fiftyone as fo
import fiftyone.core.logging as fol
import fiftyone.server.routes.media as fom
import fiftyone.core.metadata as fcm
import json

# Constants
LINK_PREFIX = "http://localhost:5151/media?filepath=s3://"
S3_BUCKET_NAME = 'coco-val-2017'
S3_PREFIX = 'val2017'
S3_ENDPOINT_URL = 'http://localhost:9000'
AWS_ACCESS_KEY_ID = 'minio_access_key'
AWS_SECRET_ACCESS_KEY = 'minio_secret_key'

def delete_old_datasets():
    datasets = fo.list_datasets()

    # Remove each dataset
    for dataset_name in datasets:
        fo.delete_dataset(dataset_name)


def load_annotations():
    """Load JSON annotation files."""
    with open('instances_val2017.json', 'r') as file:
        instance_annotation = json.load(file)

    with open('captions_val2017.json', 'r') as file:
        captions_annotation = json.load(file)

    with open('person_keypoints_val2017.json', 'r') as file:
        person_annotation = json.load(file)

    return instance_annotation, captions_annotation, person_annotation


def notify_loggers():
    """Set up logging for FiftyOne and media-related logs."""
    # Initialize FiftyOne logging
    fo.config.logging_level = "DEBUG"
    fol.init_logging()

    # Create and configure media logger
    media_logger = logging.getLogger("fiftyone.server.routes.media")
    media_logger.setLevel(logging.DEBUG)

    # Create a handler to display the logs on the console
    console_handler = logging.StreamHandler()
    console_handler.setLevel(logging.DEBUG)

    # Set the format of the log message
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    console_handler.setFormatter(formatter)

    # Add the handler to the media logger
    media_logger.addHandler(console_handler)

    media_logger.debug("Starting test_s3.py...")


def initialize_s3_client():
    """Initialize the S3 client using boto3."""
    return boto3.client(
        's3',
        endpoint_url=S3_ENDPOINT_URL,
        aws_access_key_id=AWS_ACCESS_KEY_ID,
        aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
        config=Config(signature_version='s3v4')
    )


def get_s3_image_uris(s3, bucket_name, prefix):
    """Fetch a list of image URIs from S3."""
    image_files = []
    paginator = s3.get_paginator('list_objects_v2')
    pages = paginator.paginate(Bucket=bucket_name, Prefix=prefix)

    for page in pages:
        if 'Contents' in page:
            for obj in page['Contents']:
                file_key = obj['Key']
                s3_uri = f"{LINK_PREFIX}{bucket_name}/{file_key}"
                image_files.append(s3_uri)
    
    return image_files

def create_dataset_with_annotations(image_links, instance_annotation, captions_annotation, person_annotation):
    # Create a new FiftyOne dataset
    dataset = fo.Dataset()

    image_dimensions_map = {
        image["id"]: (image["width"], image["height"])
        for image in instance_annotation["images"]
    }

    # Map image_id to corresponding annotations for fast lookup
    # Store a list of instance annotations for each image

    instance_category_id_to_name = {category['id']: category['name'] for category in instance_annotation['categories']}

    instance_map = {}
    for ann in instance_annotation["annotations"]:
        image_id = ann["image_id"]
        if image_id not in instance_map:
            instance_map[image_id] = []
        instance_map[image_id].append(ann)

    # Store a list of captions for each image
    captions_map = {}
    for ann in captions_annotation["annotations"]:
        image_id = ann["image_id"]
        if image_id not in captions_map:
            captions_map[image_id] = []
        captions_map[image_id].append(ann)

    # Store a list of keypoints for each image
    person_keypoints_map = {}
    for ann in person_annotation["annotations"]:
        image_id = ann["image_id"]
        if image_id not in person_keypoints_map:
            person_keypoints_map[image_id] = []
        person_keypoints_map[image_id].append(ann)

    # Loop through the provided image links and add them to the dataset
    for image_link in image_links:
        # Extract the image_id from the image link (this depends on the naming structure in the dataset)
        image_id = int(image_link.split("/")[-1].split(".")[0])

        # Create a FiftyOne sample with the image link
        sample = fo.Sample(filepath=image_link)

        if image_id in instance_map:
            instance_annotations = instance_map[image_id]
            
            detection_list = []
            for instance_data in instance_annotations:
                bbox = instance_data.get("bbox", None)
                if bbox and image_id in image_dimensions_map:
                    image_width, image_height = image_dimensions_map[image_id]
                    
                    category_id = instance_data['category_id']
                    category_name = instance_category_id_to_name.get(category_id, f"category_{category_id}")  # Use label if not found
                    detection = fo.Detection(
                        label=category_name,
                        bounding_box=[
                            bbox[0] / image_width,    # Normalize X
                            bbox[1] / image_height,   # Normalize Y
                            bbox[2] / image_width,    # Normalize width
                            bbox[3] / image_height    # Normalize height
                        ]
                    )
                    detection_list.append(detection)

            sample["ground_truth"] = fo.Detections(detections=detection_list)

        # Add captions annotations (if available)
        # if image_id in captions_map:
        #     caption_data = captions_map[image_id]
        #     sample["caption"] = fo.Label(label=caption_data["caption"])

        # Add person keypoints annotations (if available)
        # if image_id in person_keypoints_map:
        #     keypoint_data = person_keypoints_map[image_id]
        #     keypoints = keypoint_data.get("keypoints", None)

        #     # Add keypoints if available
        #     if keypoints:
        #         keypoint_list = []
        #         for i in range(0, len(keypoints), 3):
        #             x, y, visibility = keypoints[i:i + 3]
        #             keypoint = fo.Keypoint(
        #                 label=f"keypoint_{i//3 + 1}",
        #                 points=[[x / 640, y / 480]],
        #                 confidence=visibility
        #             )
        #             keypoint_list.append(keypoint)

        #     sample["person_keypoints"] = fo.Keypoints(keypoints=keypoint_list)

        # Add the sample to the dataset
        dataset.add_sample(sample)


    dataset.tag_labels("validation")
    return dataset

def recompute_sample_metadata(sample):
    """Recompute and update metadata for a single sample."""
    metadata = fcm.ImageMetadata.build_for(sample.filepath)
    sample.metadata = metadata
    sample.save()  # Save updated metadata to the dataset


def first_sample_metadata(dataset):
    # Get the first sample
    sample = dataset.first()
    
    # Ensure there is at least one sample in the dataset
    print(sample)
    if sample is None:
        print("No samples found in the dataset.")

    # Retrieve metadata from the sample
    metadata = sample.metadata

    # Ensure that metadata exists for the sample
    if metadata is None:
        print("Metadata not found for the first sample.")
    else:
        # Display the metadata
        print(f"Filepath: {sample.filepath}")
        print(f"Size (bytes): {metadata.size_bytes}")
        print(f"MIME Type: {metadata.mime_type}")
        print(f"Width (pixels): {metadata.width}")
        print(f"Height (pixels): {metadata.height}")
        print(f"Number of Channels: {metadata.num_channels}")


def launch_fiftyone_app(dataset):
    """Launch the FiftyOne app to view the dataset."""
    session = fo.launch_app(dataset=dataset, port=5151)
    session.wait()

import requests
import time

def wait_for_server(url="http://localhost:5151"):
    """Wait for the media server to be available."""
    while True:
        try:
            response = requests.get(url)
            if response.status_code == 200:
                print("Server is up!")
                break
        except requests.ConnectionError:
            print("Waiting for server to start...")
        time.sleep(5)  # Retry every 5 seconds
# Main execution
def main():
    # Step 0: Delete all existing dataset.
    delete_old_datasets()
    # Step 1: Set up logging
    notify_loggers()

    # Step 2: Initialize S3 client
    s3 = initialize_s3_client()

    # Step 3: Load annotations
    instance_annotation, captions_annotation, person_annotation = load_annotations()

    # Step 4: Retrieve image URIs from S3
    image_files = get_s3_image_uris(s3, S3_BUCKET_NAME, S3_PREFIX)

    # Step 5: Create and save FiftyOne dataset (currently just using first 100 images )
    dataset = create_dataset_with_annotations(image_files[:100],instance_annotation,captions_annotation,person_annotation)

    # Step 6: Launch FiftyOne app
    session = fo.launch_app(dataset=dataset, port=5151)

    # Step 8: Recompute metadata after the server is available
    dataset.compute_metadata()

    # Keep the app open
    session.wait()


if __name__ == "__main__":
    main()

# {
#    "segmentation":[
#       [
#          240.86,
#          211.31,
#          240.16,
#          197.19,
#          236.98,
#          192.26,
#          237.34,
#          187.67,
#          245.8,
#          188.02,
#          243.33,
#          176.02,
#          250.39,
#          186.96,
#          251.8,
#          166.85,
#          255.33,
#          142.51,
#          253.21,
#          190.49,
#          261.68,
#          183.08,
#          258.86,
#          191.2,
#          260.98,
#          206.37,
#          254.63,
#          199.66,
#          252.51,
#          201.78,
#          251.8,
#          212.01
#       ]
#    ],
#    "area":531.8071000000001,
#    "iscrowd":0,
#    "image_id":139,
#    "bbox":[
#       236.98,
#       142.51,
#       24.7,
#       69.5
#    ],
#    "category_id":64,
#    "id":26547
# }