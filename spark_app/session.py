from pyspark.sql import SparkSession

def get_spark(session_name: str) -> SparkSession: 
    return (SparkSession.builder.appName(session_name)\
            .config("spark.jars.packages", "org.postgresql:postgresql:42.7.4")\
            .config("spark.sql.shuffle.partitions", "8")\
            .getOrCreate() 
            )