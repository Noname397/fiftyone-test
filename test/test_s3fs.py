import fiftyone as fo
import os

mounted_bucket_path = os.path.expanduser("~/s3bucket/coco-2017")

image_files = [os.path.join(mounted_bucket_path, f) for f in os.listdir(mounted_bucket_path) if f.endswith(('.png', '.jpg', '.jpeg'))]

dataset = fo.Dataset(name="s3_mounted_dataset")
dataset.add_images(image_files)

for sample in dataset.take(5):
    print(sample)

session = fo.launch_app(dataset=dataset)
session.wait()
