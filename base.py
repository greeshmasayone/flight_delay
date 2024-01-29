from spark_config import get_spark_session


class Base(object):
    def __init__(self):
        self.spark_session = get_spark_session()

    @property
    def spark(self):
        return self.spark_session