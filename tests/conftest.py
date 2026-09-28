import pytest
from pyspark.sql import SparkSession


@pytest.fixture(scope="session")
def spark():
    """Session-scoped SparkSession fixture."""
    session = (
        SparkSession.builder.appName("test")
        .master("local[2]")
        .getOrCreate()
    )
    yield session
    session.stop()
