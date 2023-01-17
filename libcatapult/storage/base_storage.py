from abc import abstractmethod


class NotConnectedException(Exception):
    pass


class NoObjectError(Exception):
    pass


class NotImplementedException(Exception):
    pass


class BaseStorage(object):
    """
        BaseStorage allows for the swapping of storage technologies under the hood.

        Once created you should need to call connect() to actually establish a connection.
        When you are done you should call close()
        """

    def __init__(self):
        pass

    @abstractmethod
    def connect(self):
        """
        Establish a connection to the actual service that we are wanting
        :return: nothing
        """
        pass

    @abstractmethod
    def count(self):
        """
        Count the number of objects in the bucket/container/thing.

        :return: The number of objects in the bucket/container/thing
        """
        pass

    @abstractmethod
    def list_files(self, prefix):
        """
        Create and return a list of all files in the bucket/container/thing.

        :param: Prefix to search for, primarily a path but it's just a string match.
        :return: List of strings.
        """
        pass

    @abstractmethod
    def list_files_with_sizes(self, prefix):
        """
        Create and return a list of all files in the bucket/container/thing along with their file sizes.

        :param: Prefix to search for, primarily a path but it's just a string match.
        :return: List of dictionaries with name and size keys.
        """
        pass

    @abstractmethod
    def fetch_file(self, path, destination):
        """
        Download a file from the storage and put it in the destination

        :param path: location in the storage to get the file from.
        :param destination: where on the local file system to put the file
        :return: None
        """
        pass

    @abstractmethod
    def put_file(self, source, destination):
        """
        put a file into the storage from the local file system.

        :param source: a path to a file on the local file system
        :param destination: where in the storage to put the file.
        :return: None
        """
        pass

    @abstractmethod
    def get_object_body(self, path):
        """
        Get an object as a byte array rather than putting it somewhere else on disk
        """
        pass