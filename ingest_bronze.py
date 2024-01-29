import os
from read_data1 import ReadData
from utils import get_file_names

from config1 import DELTA_PATH, SOURCE_PATH

class IngestData(ReadData):
    def __init__(self):
        super().__init__()
        self.ingest_airline_csv_data_delta()

    def ingest_airline_csv_data_delta(self):
        airline_source_path = SOURCE_PATH
        file_names = get_file_names(airline_source_path)
        for file_name in file_names:
            airline_df = self.read_csv_data(
                "{}/{}".format(airline_source_path, file_name)
            )
            # airline_df.write.format("delta").mode("append").option("mergeSchema", "true").save(
            #     DELTA_PATH + "/airline"
            airline_df.write.format("delta").mode("append").save(
                DELTA_PATH + "/airline"
            )


        


