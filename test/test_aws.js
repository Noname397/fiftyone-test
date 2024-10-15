import { S3Client, GetObjectCommand } from "@aws-sdk/client-s3";
import { Readable } from "stream";
import { Buffer } from "buffer"; // To handle binary data

// Configure the S3 client with your AWS credentials and endpoint
const s3Client = new S3Client({
  region: "us-east-1", // Replace with your S3 bucket region
  credentials: {
    accessKeyId: "minio_access_key", // Replace with your AWS access key
    secretAccessKey: "minio_secret_key", // Replace with your AWS secret key
  },
  endpoint: "http://localhost:9000", // Replace with your MinIO or S3 endpoint if necessary
  forcePathStyle: true, // Required for MinIO
});

// Function to fetch an image from S3 and return it as an Object URL
async function fetchImageFromS3AsObjectURL(bucketName, objectKey) {
  try {
    const command = new GetObjectCommand({
      Bucket: bucketName,
      Key: objectKey,
    });

    const response = await s3Client.send(command);

    if (response.Body instanceof Readable) {
      const chunks = [];

      for await (const chunk of response.Body) {
        chunks.push(chunk);
      }

      const buffer = Buffer.concat(chunks);
      const blob = new Blob([buffer], { type: response.ContentType });
      const objectUrl = URL.createObjectURL(blob);

      return objectUrl;
    } else {
      console.error("Unexpected response body type:", typeof response.Body);
      return undefined;
    }
  } catch (error) {
    console.error("Error retrieving image from S3:", error);
    return undefined;
  }
}

// Function to extract bucket name and object key from an S3 URI
function extractS3Details(s3Uri) {
  const uriWithoutPrefix = s3Uri.replace("s3://", "");
  const [bucketName, ...keyParts] = uriWithoutPrefix.split("/");
  const objectKey = keyParts.join("/");

  return { bucketName, objectKey };
}

// Example usage
const s3Uri = "s3://coco-2017/000000000009.jpg";
const { bucketName, objectKey } = extractS3Details(s3Uri);

fetchImageFromS3AsObjectURL(bucketName, objectKey).then((objectUrl) => {
  if (objectUrl) {
    console.log("Image Object URL created successfully:", objectUrl);
    const myImgElement = document.getElementById('my-img');
    if (myImgElement) myImgElement.src = objectUrl;
  } else {
    console.error("Failed to fetch image.");
  }
});
