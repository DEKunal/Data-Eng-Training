from pyspark.sql import SparkSession
from pyspark.sql import functions as func
from pyspark.sql.types import StructField,StructType,StringType,FloatType,IntegerType

spark = SparkSession.builder.appName("TotalSpentByCustomer").getOrCreate()

customerSchema=StructType([
    StructField("cust_id",IntegerType(),True),
    StructField("item_id",IntegerType(),True),
    StructField("amount_spent",FloatType,True)
])

customerDf=spark.read.schema(customerSchema).option("header","true").csv("customer-orders.csv")

totalByCustomer=customerDf.groupBy("cust_id").agg(func.round(func.sum("amount_spent"),2))

totalCustomerSorted=totalByCustomer.sort("amount_spent")

totalCustomerSorted.show()