from pyspark.sql import SparkSession
from pyspark.sql.types import StructType,StructField,StringType,IntegerType,LongType
from pyspark.sql import functions as func 

spark=SparkSession.builder.appName("Popular Movie").getOrCreate()

movieSchema=StructType([
    StructField("userId",IntegerType(),True),
    StructField("movieId",IntegerType(),True),
    StructField("rating",IntegerType(),True),
    StructField("timestamp",LongType(),True)

])

movieDf=spark.read.option("sep", "\t").schema(movieSchema).csv("file:///home/user/sparkcourse/ml-100k/u.data")

topmovies=movieDf.groupBy("movieId").count().orderBy(func.desc("count"))

topmovies.show(10)