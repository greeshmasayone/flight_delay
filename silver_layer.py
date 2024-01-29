from pyspark.sql.functions import col
import pyspark.sql.functions as F
from pyspark.sql.types import (
    BooleanType,
    DateType,
    IntegerType,
    StringType,
    TimestampType,
)
from base import Base
from read_data1 import ReadDelta
from config1 import DELTA_PATH
from utils import *


airline_table = "airline"

class SilverLayer(ReadDelta):
    def __init__(self):
        super().__init__()
        self.clean_airline_data()

    def clean_airline_data(self):
        path = DELTA_PATH + "/airline"
        self.read_delta_data(table_name=airline_table, path=path)
        fields = list_to_string(input_list=airline_fields, delimiter=",")
        bronze_df = self.spark.sql(f"select {fields} from {airline_table}")

        renamed_col_df = bronze_df.select(
            col("FL_DATE").cast(TimestampType()).alias("fl_date"),
            col("OP_CARRIER").alias("op_carrier"),
            col("OP_CARRIER_FL_NUM").cast(IntegerType()).alias("op_carrier_fl_num"),
            col("ORIGIN").alias("origin"),
            col("DEST").alias("dest"),
            col("CRS_DEP_TIME").cast(IntegerType()).alias("crs_dep_time"),
            col("DEP_TIME").cast(IntegerType()).alias("dep_time"),
            col("DEP_DELAY").cast(IntegerType()).alias("dep_delay"),
            col("TAXI_OUT").cast(IntegerType()).alias("taxi_out"),
            col("WHEELS_OFF").cast(IntegerType()).alias("wheels_off"),
            col("WHEELS_ON").cast(IntegerType()).alias("wheesls_on"),
            col("TAXI_IN").cast(IntegerType()).alias("taxi_in"),
            col("CRS_ARR_TIME").cast(IntegerType()).alias("crs_arr_time"),
            col("ARR_TIME").cast(IntegerType()).alias("arr_time"),
            col("ARR_DELAY").cast(IntegerType()).alias("arr_delay"),
            col("CANCELLED").cast(BooleanType()).alias("cancelled"),
            col("CANCELLATION_CODE").alias("cancellation_code"),
            col("DIVERTED").cast(IntegerType()).alias("diverted"),
            col("CRS_ELAPSED_TIME").cast(IntegerType()).alias("crs_elapsed_time"),
            col("ACTUAL_ELAPSED_TIME").cast(IntegerType()).alias("actual_elapsed_time"),
            col("AIR_TIME").cast(IntegerType()).alias("air_time"),
            col("DISTANCE").cast(IntegerType()).alias("distance"),
            col("CARRIER_DELAY").cast(IntegerType()).alias("carrier_delay"),
            col("WEATHER_DELAY").cast(IntegerType()).alias("weather_delay"),
            col("NAS_DELAY").cast(IntegerType()).alias("nas_delay"),
            col("SECURITY_DELAY").cast(IntegerType()).alias("security_delay"),
            col("LATE_AIRCRAFT_DELAY").cast(IntegerType()).alias("late_aircraft_delay"),
            col("UNNAMED").alias("unnamed"),
        )

# remove null values from the cols used for classification:
        remove_null_df = renamed_col_df.dropna(subset= [
            'fl_date',
            'op_carrier',
            'op_carrier_fl_num',
            'origin',
            'dest',
            'crs_dep_time',
            'crs_arr_time',
            'cancelled',
            'diverted',
            'crs_elapsed_time',
            'distance'])
        
#writing to delta table
        
        remove_null_df.write.format("delta").mode("append").save(
            DELTA_PATH + "/silver/airline_info"
        )


# drop the cols which indirectly indicate if a flight is cancelled or not (apart from the column CANCELLED)
# most of those cols contain null values, if the flight is cancelled

        classify_df = remove_null_df.drop("unnamed", 
                        "carrier_delay", 
                        "weather_delay",
                        "nas_delay",
                        "security_delay",
                        "late_aircraft_delay",
                        "cancellation_code",
                        "dep_time",
                        "dep_delay",
                        "taxi_out",
                        "wheels_off",
                        "wheels_on",
                        "taxi_in",
                        "arr_time",
                        "arr_delay",
                        "actual_elapsed_time", 
                        "air_time")


# numerical timestamp column
        classify_df = classify_df.withColumn("fl_date", F.unix_timestamp("fl_date"))


# select subsample of positive samples
        # pos_df = classify_df.filter(F.col('cancelled').isin(1)).sample(fraction=0.1)
        pos_df = classify_df.filter(F.col('cancelled') == 1).sample(fraction=0.1)

# select an equal amount of negative samples (number of neg samples == number of pos samples)
        neg_df = classify_df.filter(F.col('cancelled') == 0).orderBy(F.rand()).limit(pos_df.count())


# creating a balanced subset
        classify_df = pos_df.union(neg_df).sample(fraction=1.0).cache()

