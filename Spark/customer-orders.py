from pyspark import SparkConf,SparkContext
import collections

conf=SparkConf().setMaster("local").setAppName("Customer-Orders")
sc=SparkContext(conf=conf)

def custord(line):
    fields=line.split(',')
    custid=int(fields[0])
    dollar=float(fields[2])
    return (custid,dollar)
    
lines = sc.textFile("file:///home/user/sparkcourse/customer-orders.csv")

parsedline=lines.map(custord)


addamount=parsedline.reduceByKey(lambda x,y:x+y)

addamountSorted = addamount.map(lambda x: (x[1], x[0])).sortByKey()

results=addamountSorted.collect()

for result in results:
    print(result)