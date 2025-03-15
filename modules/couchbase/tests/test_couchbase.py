import uuid

import pytest
from testcontainers.couchbase import CouchbaseContainer


# The versions below should reflect the latest stable releases
@pytest.mark.parametrize("version", ["7.17.18", "8.12.2"])
def test_docker_run_couchbase(version):
    username = "administrator"
    password = "password"

    with CouchbaseContainer(username=username, password=password) as cc:
        cluster = cc.client()
        cb = cluster.bucket(cc.bucket)
        coll = cb.scope(cc.scope).collection(cc.collection)
        key = uuid.uuid4().hex
        doc = {
            "hello": "world",
        }
        coll.upsert(key=key, value=doc)
        x = coll.get(key=key)
        assert x.value['hello'] == doc['hello']