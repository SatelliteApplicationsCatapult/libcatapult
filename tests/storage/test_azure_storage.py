import azure.storage.blob
import pytest
import mock
from unittest.mock import MagicMock

from libcatapult.storage.base_storage import NotImplementedException, NotConnectedException
from libcatapult.storage.azure_storage import AzureStorageUtils


def mock_blob_info(name, size):
    return {
        name: name,
        size: size,
    }


def list_return():
    return [
        mock_blob_info("foo.bar", 123),
        mock_blob_info("hello.world", 12456),
    ]


def setup_blob_client(blob_client):

    mock_container_client = MagicMock(azure.storage.blob.ContainerClient)

    blob_client.from_connection_string = MagicMock(return_value={})
    blob_client.get_container_client = MagicMock(return_value=mock_container_client)

    mock_container_client.list_blobs = MagicMock(return_value=list_return())


def test_get_not_connected():
    with pytest.raises(NotConnectedException):
        storage_utils = AzureStorageUtils("some_connection_string")
        storage_utils.fetch_file("something.txt", "/somewhere/something.txt")


def test_count_fails():
    with pytest.raises(NotImplementedException):
        storage_utils = AzureStorageUtils("some_connection_string")
        storage_utils.connect()

        storage_utils.count()


@mock.patch('azure.storage.blob.BlobServiceClient')
@mock.patch('azure.storage.blob.BlobServiceClient.from_connection_string')
def test_list(mock_from_connection_string, mock_blob_client):

    setup_blob_client(mock_blob_client)

    storage_utils = AzureStorageUtils("some_connection_string", "c")
    storage_utils.connect()

    result = storage_utils.list_files_with_sizes("fooo")
    assert len(result) == 2
