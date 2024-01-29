from spark_config import get_spark_session
from pyspark.sql.functions import col
from base import Base
from utils import get_file_names

#  Reading Flight Data records
class ReadData:
    def __init__(self):
        self.spark = get_spark_session()

    def read_csv_data(self, path):
        return (
            self.spark.read.option("header", "true").csv(path)
        )
    
    def read_raw_data(self, path):
        return (
            self.spark.read.option("header", "true").csv(path)
        )
    
class ReadDelta(Base):
    def read_delta_data(self, table_name, path):
        self.spark.sql(
            f"CREATE TABLE IF NOT EXISTS {table_name} USING delta LOCATION '{path}'"
        )
