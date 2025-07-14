import pytest
from pyspark.sql import SparkSession

#create spark for all tests
@pytest.fixture(scope='session')
def spark():
    return SparkSession.builder \
      .master("local") \
      .appName("chispa") \
      .getOrCreate()