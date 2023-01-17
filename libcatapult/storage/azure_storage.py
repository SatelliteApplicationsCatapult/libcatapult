import logging

from azure.storage.blob import BlobServiceClient, BlobClient, ContainerClient
from libcatapult.storage.base_storage import BaseStorage, NotConnectedException, NoObjectError, NotImplementedException


class AzureStorageUtils(BaseStorage):

    def __init__(self, connection_string, container_name):
        super().__init__()
        self.connection_string = connection_string
        self.container_name = container_name
        self.blob_service_client = None

    def connect(self):
        if not self.blob_service_client:
            # open the connection
            self.blob_service_client = BlobServiceClient.from_connection_string(self.connection_string)
        return self.blob_service_client

    def count(self):
        if not self.blob_service_client:
            raise NotConnectedException()
        raise NotImplementedException("Can not safely count azure containers")

    def list_files(self, prefix):
        if not self.blob_service_client:
            raise NotConnectedException()
        container_client = self.blob_service_client.get_container_client(self.container_name)
        blob_names = container_client.list_blob_names(name_starts_with=prefix)

        return [name for name in blob_names]

    def list_files_with_sizes(self, prefix):
        if not self.blob_service_client:
            raise NotConnectedException()

        container_client = self.blob_service_client.get_container_client(self.container_name)

        blobs = container_client.list_blobs(name_starts_with=prefix)

        return [{"name": obj.name, "size": obj.size} for obj in blobs]

    def fetch_file(self, path, destination):
        if not self.blob_service_client:
            raise NotConnectedException()

        with open(file=destination, mode="wb") as download_file:
            download_file.write(self.blob_service_client.download_blob(path).readall())

    def put_file(self, source, destination):
        if not self.blob_service_client:
            raise NotConnectedException()

        blob_client = self.blob_service_client.get_blob_client(
            container=self.container_name,
            blob=destination
        )

        with open(file=source, mode="rb") as data:
            blob_client.upload_blob(data)

    def get_object_body(self, path):
        if not self.blob_service_client:
            raise NotConnectedException()
        return self.blob_service_client.download_blob(path).readall()

