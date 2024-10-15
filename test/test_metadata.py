import fiftyone as fo
import fiftyone.core.metadata as fom

# The URL of the image served via your local media server
image_url = "http://localhost:5151/media?filepath=s3://coco-val-2017/val2017/000000000139.jpg"

# Create a new sample using the image URL
sample = fo.Sample(filepath=image_url)

# Create a new dataset
dataset = fo.Dataset()

# Add the sample to the dataset
dataset.add_sample(sample)

# Compute the metadata for the dataset (this will populate metadata for all samples in the dataset)
dataset.compute_metadata()

# Retrieve and print the metadata for the sample
sample = dataset.first()  # Get the first (and only) sample in the dataset
print(sample)

# Test ImageMetadata.build_for() method with the image URL
metadata = fom.ImageMetadata.build_for(image_url)

# Print the metadata computed by ImageMetadata.build_for()
print("Metadata computed by ImageMetadata.build_for():")
print(f"Filepath: {image_url}")
print(f"Size (bytes): {metadata.size_bytes}")
print(f"MIME Type: {metadata.mime_type}")
print(f"Width (pixels): {metadata.width}")
print(f"Height (pixels): {metadata.height}")
print(f"Number of Channels: {metadata.num_channels}")

# Optionally launch FiftyOne App to visually inspect the dataset
session = fo.launch_app(dataset)
session.wait()  # Keep the session open until manually closed
