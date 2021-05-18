import boto3


class S3Utils:
    """
    A simple interface around the S3 access commands.
    Only provides the tools that we need.
    """

    def __init__(self, access, secret, bucket, endpoint_url, region):
        self.s3 = boto3.resource(
            "s3",
            endpoint_url=endpoint_url,
            verify=False,
            region_name=region,
            aws_access_key_id=access,
            aws_secret_access_key=secret,
        )

        self.bucket = bucket
        self.gb = 1024 ** 3

        # Ensure that multipart uploads only happen if the size of a transfer is larger than S3's size limit for
        # non multipart uploads, which is 5 GB. we copy using multipart at anything over 4gb
        self.transfer_config = boto3.s3.transfer.TransferConfig(multipart_threshold=2 * self.gb, max_concurrency=10,
                                                                multipart_chunksize=2 * self.gb, use_threads=True)

    def count(self):
        """
        Count the number of objects in the bucket.

        :return: The number of objects in the bucket
        """
        return sum(1 for _ in self.s3.Bucket(self.bucket).objects.all())

    def list_files(self, prefix):
        """
        Create and return a list of all files in the bucket.

        :param: Prefix to search for, primarily a path but it's just a string match.
        :return: List of strings.
        """

        filenames = []
        for obj in self.s3.Bucket(self.bucket).objects.filter(Prefix=prefix):
            filenames.append(obj.key)

        return filenames

    def list_files_with_sizes(self, prefix):
        """
        Create and return a list of all files in the bucket along with their file sizes.

        :param: Prefix to search for, primarily a path but it's just a string match.
        :return: List of dictionaries with name and size keys.
        """
        results = []
        for obj in self.s3.Bucket(self.bucket).objects.filter(Prefix=prefix):
            results.append({"name": obj.key, "size": obj.size})
        return results

    def fetch_file(self, path, destination):
        """
        Download a file from S3 and put it in the destination

        :param path: location in S3 to get the file from.
        :param destination: where on the local file system to put the file
        :return: None
        """
        self.s3.Bucket(self.bucket).download_file(path, destination)

    def put_file(self, source, destination):
        """
        put a file into S3 from the local file system.

        :param source: a path to a file on the local file system
        :param destination: where in S3 to put the file.
        :return: None
        """
        self.s3.Bucket(self.bucket).upload_file(source, destination)
