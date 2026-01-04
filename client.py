from minio import Minio
from minio.error import S3Error
import uuid
import io
import random
import string

client = Minio(
    "localhost:9000",
    access_key="minioadmin",
    secret_key="minioadmin",
    secure=False
)

bucket_name = "my-bucket"

if not client.bucket_exists(bucket_name):
    client.make_bucket(bucket_name)


object_name = f"file-{uuid.uuid4().hex}.txt"
random_data = ''.join(random.choices(string.ascii_letters + string.digits, k=50))
data_stream = io.BytesIO(random_data.encode("utf-8"))

client.put_object(bucket_name, object_name, data_stream, length=len(random_data))
print(f"Created object: {object_name}")

print("\nListing objects:")
for obj in client.list_objects(bucket_name):
    print(f" - {obj.object_name}")

print(f"\nReading object: {object_name}")
response = client.get_object(bucket_name, object_name)
print(response.read().decode("utf-8"))

print(f"\nRemoving object: {object_name}")
client.remove_object(bucket_name, object_name)
print("Object removed.")

updated_data = "This is the updated content."
updated_stream = io.BytesIO(updated_data.encode("utf-8"))

client.put_object(bucket_name, object_name, updated_stream, length=len(updated_data))
print("\nObject updated with new content.")
