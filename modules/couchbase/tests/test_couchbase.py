import json
import urllib.request

import pytest

from testcontainers.couchbase import CouchbaseContainer
from testcontainers.elasticsearch import ElasticSearchContainer


# The versions below should reflect the latest stable releases
@pytest.mark.parametrize("version", ["7.17.18", "8.12.2"])
def test_docker_run_couchbase(version):
    with CouchbaseContainer(username="administrator", password="password") as cc:
        resp = cc.get_connection_url()
        # print(resp)
        # assert json.loads(resp.read().decode())["version"]["number"] == version
    # c = CouchbaseContainer(username="administrator", password="password")
    # c.start()
    # c.create_data_bucket(bucket="new_bucket")
    # c.client()
    #
    # print(c.get_cluster_url())
    # while True:
    #     time.sleep(2)
