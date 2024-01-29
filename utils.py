import os
from functools import reduce

airline_fields = [
    "FL_DATE",
    "OP_CARRIER",
    "OP_CARRIER_FL_NUM",
    "ORIGIN",
    "DEST",
    "CRS_DEP_TIME",
    "DEP_TIME",
    "DEP_DELAY",
    "TAXI_OUT",
    "WHEELS_OFF",
    "WHEELS_ON",
    "TAXI_IN",
    "CRS_ARR_TIME",
    "ARR_TIME",
    "ARR_DELAY",
    "CANCELLED",
    "CANCELLATION_CODE",
    "DIVERTED",
    "CRS_ELAPSED_TIME",
    "ACTUAL_ELAPSED_TIME",
    "AIR_TIME",
    "DISTANCE",
    "CARRIER_DELAY",
    "WEATHER_DELAY",
    "NAS_DELAY",
    "SECURITY_DELAY",
    "LATE_AIRCRAFT_DELAY",
    "UNNAMED",
]

def get_file_names(directory_path):
    try:
        # Get the list of files in the specified directory
        filenames = os.listdir(directory_path)
        return filenames

    except OSError:
        print(f"Error reading directory: {directory_path}")
        return None

def list_to_string(input_list, delimiter):
    result = reduce(lambda x, y: str(x) + delimiter + str(y), input_list)
    return str(result)