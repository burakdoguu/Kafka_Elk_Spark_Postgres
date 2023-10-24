from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
import sys
from pyspark import SparkContext, SparkConf

if __name__ == "__main__":

    csv_file=sys.argv[1]

    spark = SparkSession \
     .builder \
     .appName("File Streaming Demo") \
     .getOrCreate() 


    schema_ = StructType() \
        .add('name', StringType(),True) \
        .add('company', StringType(),True) \
        .add('msg', StringType(),True) \
        .add('remote_ip', StringType(),True) \
        .add('user_agent', StringType(),True) \
        .add('date', StringType(),True)


    raw_df = spark.read \
        .format('csv')\
        .option("cleanSource", "delete") \
        .option("header","true") \
        .schema(schema_) \
        .load("/opt/bitnami/spark/resources/data/information_creation.csv")

    #print(raw_df.printSchema())

    #query_df = raw_df.select("name", "company", "msg", "remote_ip", "user_agent", "date")
    query_df = raw_df.select("remote_ip", "user_agent", "date")
    
    query_df.write.format("jdbc") \
        .option("url", "jdbc:postgresql://{your-ip}:5432/postgres") \
        .option("dbtable", "new_person_2") \
        .option("user", "postgres")\
        .option("password", "postgres070201")\
        .mode("append")\
        .save()