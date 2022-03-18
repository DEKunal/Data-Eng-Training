from pyspark.sql import SparkSession
from pyspark.sql.types import StructType,StructField,StringType,IntegerType,FloatType,LongType
from pyspark.sql import functions as func
import codecs

def loadMovieName():
    movieNames={}
    with codecs.open("/home/user/sparkcourse/ml-100k/u.item","r", encoding='ISO-8859-1', errors='ignore') as f:
        for line in f:
            fields=line.split('|')
            movieNames[int(fields[0])]=fields[1]
        return movieNames

spark=SparkSession.builder.appName("PopularMovies").getOrCreate()

nameDict=spark.sparkContext.broadcast(loadMovieName())

schema=StructType([
    StructField("userId",IntegerType(),True),
    StructField("movieId",IntegerType(),True),
    StructField("rating",IntegerType(),True),
    StructField("timestamp",LongType(),True)
])

movieDf=spark.read.option("sep","\t").schema(schema).csv("file:///home/user/sparkcourse/ml-100k/u.data")


movieCounts=movieDf.groupBy("movieId").count()

def lookupName(movieId):
    return nameDict.value[movieId]

lookupNameUDF=func.udf(lookupName)

movieWithNames=movieCounts.withColumn("movieTitle",lookupNameUDF(func.col("movieId")))

sortedMovieWithNames= movieWithNames.orderBy(func.desc("count"))

sortedMovieWithNames.show(10)

spark.stop()