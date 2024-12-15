from hudi import HudiTableBuilder
import pyarrow as pa
from pyspark.sql import SparkSession

if __name__ == "__main__":
    spark = SparkSession.builder.appName("MyPySparkJob").getOrCreate()

    # Your PySpark code goes here
    df = spark.createDataFrame([
        (1, "John"), (2, "Jane"), (3, "Bob")
    ], ["id", "name"])

    df.show()

    spark.stop()




