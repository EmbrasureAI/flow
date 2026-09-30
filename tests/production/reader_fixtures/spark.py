"""Execute a finite SQL plan using the stock Spark/Iceberg runtime in the container."""
import datetime
import decimal
import json
from pathlib import Path
import sys

from pyspark.sql import SparkSession


def scalar(value):
    if isinstance(value, decimal.Decimal):
        return str(value)
    if isinstance(value, datetime.datetime):
        return value.isoformat(timespec="microseconds")
    if isinstance(value, datetime.date):
        return value.isoformat()
    if isinstance(value, (bytes, bytearray)):
        return value.hex()
    raise TypeError(type(value).__name__)


spark = SparkSession.builder.getOrCreate()
spark.sparkContext.setLogLevel("WARN")
try:
    for query in json.loads(Path(sys.argv[1]).read_text()):
        frame = spark.sql(query["sql"])
        result = {"name": query["name"], "columns": frame.columns, "rows": [list(row) for row in frame.collect()]}
        print("FLOW_READER_RESULT=" + json.dumps(result, default=scalar), flush=True)
finally:
    spark.stop()
