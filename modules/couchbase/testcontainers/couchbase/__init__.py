_UNSET = object()
import os
import time
from datetime import timedelta
from typing import Optional

import requests
from testcontainers.core.generic import DbContainer
from testcontainers.core.waiting_utils import wait_container_is_ready


class CouchbaseContainer(DbContainer):
    def __init__(
            self,
            image: str = "couchbase:latest",
            ports: list[int] = None,
            username: Optional[str] = None,
            password: Optional[str] = None,
            bucket: Optional[str] = None,
            **kwargs,
    ) -> None:
        super().__init__(image=image, **kwargs)
        self.username: str = username or os.environ.get("COUCHBASE_USERNAME", "Administrator")
        self.password: str = password or os.environ.get("COUCHBASE_PASSWORD", "password")
        self.bucket: str = bucket or os.environ.get("COUCHBASE_BUCKET", "default")

        if ports is None or len(ports) == 0:
            ports = [8091, 8092, 8093, 8094, 8095, 8096, 8097, 9123, 11207, 11210, 11280, 18091, 18092, 18093, 18094,
                     18095, 18096, 18097]
        for port in ports:
            self.with_exposed_ports(port)
            self.with_bind_ports(port, port)

    def _configure(self) -> None:
        pass

    def get_connection_url(self) -> str:
        return f"couchbase://localhost"

    def get_cluster_url(self) -> str:
        return self.get_connection_url()

    def start(self, max_retries=5, retry_delay_seconds=10) -> "CouchbaseContainer":
        retry_count = 0
        started: bool = False
        while retry_count < max_retries:
            try:
                self._configure()
                super(DbContainer, self).start()
                started = True
                break
            except Exception as e:
                retry_count += 1
                if retry_count >= max_retries:
                    raise
                if isinstance(e, requests.exceptions.HTTPError):
                    e: requests.exceptions.HTTPError
                    print(
                        f"Connection attempt {retry_count} failed! [Reason]: {e.response.json()['message']}\nRetrying in {retry_delay_seconds} seconds...")
                else:
                    print(
                        f"Connection attempt {retry_count} failed: {str(e)}.\nRetrying in {retry_delay_seconds} seconds...")
                time.sleep(retry_delay_seconds)
        if started:
            self._connect()
        return self

    @wait_container_is_ready()
    def _connect(self):
        time.sleep(10)
        self.set_admin_credentials()
        self.create_data_bucket(bucket=self.bucket)

    # Set Admin credentials
    def set_admin_credentials(self):
        print('Setting admin credentials...')
        url = 'http://127.0.0.1:8091/settings/web'
        data = {'port': 8091, 'username': self.username, 'password': self.password}
        response = requests.post(url, data=data)
        if 200 <= response.status_code < 300:
            print('Admin credentials set successfully')
        else:
            print('Failed to set admin credentials. Error: ', response.text)

    def client(self):
        from couchbase.auth import PasswordAuthenticator
        from couchbase.cluster import Cluster
        from couchbase.options import ClusterOptions, TLSVerifyMode, ClusterTimeoutOptions
        auth = PasswordAuthenticator(self.username, self.password)
        options = ClusterOptions(
            auth,
            timeout_options=ClusterTimeoutOptions(
                kv_timeout=timedelta(seconds=vulcan_settings.KV_TIMEOUT)
            ),
            enable_tcp_keep_alive=True,
            tls_verify=TLSVerifyMode.NONE
        )
        options.apply_profile("wan_development")
        cluster = Cluster(self.get_cluster_url(), options)
        cluster.wait_until_ready(timedelta(seconds=15))
        print("Connected to DB.")
        return cluster

    def create_data_bucket(self, bucket: str = None) -> None:
        print('Creating data bucket...')
        url = 'http://127.0.0.1:8091/pools/default/buckets'
        auth = (self.username, self.password)
        data = {
            'name': bucket,
            'bucketType': 'couchbase',
            'ramQuota': 256,
            'replicaNumber': 0
        }
        response = requests.post(url, auth=auth, data=data)
        if 200 <= response.status_code < 300:
            print('Data bucket created successfully')
        else:
            print('Failed to create data bucket. Error: ', response.content)


c = CouchbaseContainer(username="administrator", password="password")
c.start()
c.create_data_bucket(bucket="new_bucket")
c.client()

print(c.get_cluster_url())
while True:
    time.sleep(2)