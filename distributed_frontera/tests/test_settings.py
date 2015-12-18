# -*- coding: utf-8 -*-
from distributed_frontera.settings import Settings


def test_instance_attrs():
    s = Settings(attributes={"XYZ": "hey"})
    assert s.attributes["XYZ"] == "hey"


def test_override():
    s = Settings()
    assert s.get("SPIDER_FEED_PARTITIONS") == 2