import requests
from pyspark.sql.datasource import SimpleDataSourceStreamReader, DataSource
from pyspark.sql.types import (
    StructType, StructField, BinaryType, StringType
)
import time


class GTFSRTSimpleStreamReader(SimpleDataSourceStreamReader):

    def initialOffset(self):
        return {"offset": int(time.time())}

    def __init__(self, schema: StructType, options: dict):
        """Initialize with schema and options."""
        super().__init__()
        self.schema = schema
        self.url = options.get("url", "")
        self.frequency = options.get("frequency", "minutely")

    def read(self, start: dict):
        """Reads data starting from the given offset."""
        data = []
        new_offset = {}
        gtrfsrt = self._fetch_gtfs_rt_data(self.url)
        data.append((self.url, gtrfsrt))
        new_offset.update({"offset": int(time.time())})
        return (data, new_offset)

    @staticmethod
    def _fetch_gtfs_rt_data(url):        
        try:
            # Send a GET request to the endpoint
            response = requests.get(url)
            response.raise_for_status()  # Raise an exception for HTTP errors (e.g., 404, 500)
            return response.content
        except requests.exceptions.RequestException as e:
            raise e

class GTFSRTDataSource(DataSource):
    @classmethod
    def name(cls):
        """Returns the name of the data source."""
        return "gtfsrt"

    def __init__(self, options):
        """Initialize with options provided."""
        self.options = options

    def schema(self):
        """Returns the schema of the data source."""
        return StructType([
            StructField("url", StringType(), False),
            StructField("binary_proto", BinaryType(), True)
        ])


    def simpleStreamReader(self, schema: StructType):
        """Returns an instance of the reader for this data source."""
        return GTFSRTSimpleStreamReader(schema, self.options)
    
def test():
    return 1