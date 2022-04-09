from google.cloud import storage


class GoogleCloudStorage:

    def __init__(self, bucket_name: str):
        self.storage_client = storage.Client()
        self.bucket = self.storage_client.bucket(bucket_name)

    def upload_a_file(
        self,
        source_file: str,
        destination_blob: str,
    ) -> None:
        blob = self.bucket.blob(destination_blob)
        blob.upload_from_filename(source_file)

    def download_a_file(
        self,
        source_blob_name: str,
        destination_file_name: str,
    ) -> None:
        blob = self.bucket.blob(source_blob_name)
        blob.download_to_filename(destination_file_name)

    def is_file_exists(self, file_path: str) -> bool:
        blob = self.bucket.blob(file_path)
        return blob.exists()
