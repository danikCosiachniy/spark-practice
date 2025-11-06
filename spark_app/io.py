from pyspark.sql import SparkSession, DataFrame
from spark_app.config import get_config
from spark_app.session import get_spark

def read_table(spark : SparkSession, table_name: str)-> DataFrame:
    config = get_config()

    reader = (spark.read.format("jdbc")\
             .option("url", config["db_url"])\
             .option("dbtable", table_name)\
             .option("user", config["user"])\
             .option("password", config["password"])\
             .option("driver", "org.postgresql.Driver"))
    return reader.load()

def write_df():
    pass

