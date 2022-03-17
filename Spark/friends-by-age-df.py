from pyspark.sql import SparkSession
from pyspark.sql import Row
from pyspark.sql import functions as func

spark = SparkSession.builder.appName("SparkSQL").getOrCreate()

friendsdf=spark.read.option("inferSchema","true").csv("fakefriends.csv")


age_friends=friendsdf.select("_c2","_c3").groupBy("_c2").avg("_c3").sort("_c2")


age_friends.groupBy("_c2").agg(func.round(func.avg("_c3"),2)).sort("_c2").show()