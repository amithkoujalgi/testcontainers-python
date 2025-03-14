import os
from datetime import timedelta
from typing import Optional

import requests
from tenacity import sleep

from couchbase.auth import PasswordAuthenticator
from couchbase.cluster import Cluster
from couchbase.options import ClusterOptions, ClusterTimeoutOptions, TLSVerifyMode
from testcontainers.core.generic import DbContainer
from testcontainers.core.utils import raise_for_deprecated_parameter
from testcontainers.core.waiting_utils import wait_container_is_ready, wait_for_logs


class CouchbaseContainer(DbContainer):
    """
    Couchbase database container.

    Example:
        The example spins up a Couchbase database and connects to it using
        the `couchbase-python-client`.

        .. doctest::

            >>> from couchbase.auth import PasswordAuthenticator
            >>> from couchbase.cluster import Cluster
            >>> from testcontainers.couchbase import CouchbaseContainer

            >>> with CouchbaseContainer("couchbase:latest") as couchbase:
            ...     cluster = couchbase.client()
            ...     # Use the cluster for various operations
    """

    def __init__(
        self,
        image: str = "couchbase:latest",
        ports: Optional[list[int]] = None,
        username: Optional[str] = None,
        password: Optional[str] = None,
        bucket: Optional[str] = None,
        **kwargs,
    ) -> None:
        raise_for_deprecated_parameter(kwargs, "user", "username")
        super().__init__(image=image, **kwargs)
        self.username = username or os.environ.get("COUCHBASE_USERNAME", "Administrator")
        self.password = password or os.environ.get("COUCHBASE_PASSWORD", "password")
        self.bucket = bucket or os.environ.get("COUCHBASE_BUCKET", "default")

        if ports is None or len(ports) == 0:
            ports = [
                8091,
                8092,
                8093,
                8094,
                8095,
                8096,
                8097,
                9123,
                11207,
                11210,
                11280,
                18091,
                18092,
                18093,
                18094,
                18095,
                18096,
                18097,
            ]

        for port in ports:
            self.with_exposed_ports(port)
            self.with_bind_ports(port, port)

    @wait_container_is_ready()
    def _connect(self):
        wait_for_logs(self, "and logs available in")
        while True:
            sleep(1)
            try:
                url = f"http://{self.get_container_host_ip()}:{self.get_exposed_port(8091)}/settings/web"
                response = requests.get(url)
                if 200 <= response.status_code < 300:
                    break
                else:
                    pass
                    # print("Error: ", response.text)
            except requests.exceptions.ConnectionError:
                pass

    def _configure(self) -> None:
        self.with_env("COUCHBASE_USERNAME", self.username)
        self.with_env("COUCHBASE_PASSWORD", self.password)
        self.with_env("COUCHBASE_BUCKET", self.bucket)

    def start(self) -> "CouchbaseContainer":
        self._configure()
        super().start()
        self._connect()
        self.set_admin_credentials()
        self.create_data_bucket(bucket="default")
        return self

    def set_admin_credentials(self):
        url = f"http://{self.get_container_host_ip()}:{self.get_exposed_port(8091)}/settings/web"
        data = {"username": self.username, "password": self.password, "port": "SAME"}
        response = requests.post(url, data=data)
        if 200 <= response.status_code < 300:
            return
            # print("Admin credentials set")
        else:
            pass
            # print("Failed to set admin credentials. Error: ", response.text)

    def create_data_bucket(self, bucket: str) -> None:
        # print("Creating data bucket...")
        url = f"http://{self.get_container_host_ip()}:{self.get_exposed_port(8091)}/pools/default/buckets"
        auth = (self.username, self.password)
        data = {"name": bucket, "bucketType": "couchbase", "ramQuotaMB": 256, "replicaNumber": 0}
        # data = {
        #     'name': bucket,
        #     'bucketType': 'membase',
        #     'storageBackend': 'couchstore',
        #     'autoCompactionDefined': 'false',
        #     'evictionPolicy': 'valueOnly',
        #     'threadsNumber': '3',
        #     'replicaNumber': 1,
        #     'durabilityMinLevel': 'none',
        #     'compressionMode': 'passive',
        #     'maxTTL': '0',
        #     'replicaIndex': '0',
        #     'conflictResolutionType': 'seqno',
        #     'ramQuotaMB': '256',
        #     'flushEnabled': '0',
        # }
        while True:
            sleep(1)
            response = requests.post(url, auth=auth, json=data)
            # print(response.json())
            if 200 <= response.status_code < 300:
                # print("Data bucket created")
                pass
                break
            else:
                pass
                # print("Failed to create data bucket. Error: ", response.content)

    def get_connection_url(self) -> str:
        return f"couchbases://{self.get_container_host_ip()}"

    def client(self):
        auth = PasswordAuthenticator(self.username, self.password)
        options = ClusterOptions(
            auth,
            timeout_options=ClusterTimeoutOptions(kv_timeout=timedelta(seconds=10)),
            enable_tcp_keep_alive=True,
            tls_verify=TLSVerifyMode.NONE,
        )
        cluster = Cluster(self.get_connection_url(), options)
        cluster.wait_until_ready(timedelta(seconds=15))
        # print("Connected to DB.")
        return cluster

    def list_buckets(self) -> None:
        # print("Listing data buckets...")
        url = f"http://{self.get_container_host_ip()}:{self.get_exposed_port(8091)}/pools/default/buckets"
        auth = (self.username, self.password)
        response = requests.get(url, auth=auth)
        if 200 <= response.status_code < 300:
            # print(response.json())
            return response.json()
        else:
            pass
            # print("Failed to list data buckets. Error: ", response.content)
