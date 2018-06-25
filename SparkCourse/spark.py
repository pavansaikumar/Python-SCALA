from pyspark import SparkConf, SparkContext


conf = SparkConf().setMaster("local").setAppName("FirstAssignment")
sc = SparkContext(conf = conf)

def parseLine(line):
    fields = line.split(',')
    CustomerID = int(fields[0])
    Amount = float(fields[2])
    return (CustomerID, Amount)

lines = sc.textFile("file:///SparkCourse/customer-orders.csv")
parsedlines = lines.map(parseLine)
totalCostForFriend = parsedlines.reduceByKey(lambda n,m: n+m)
sortedResults = totalCostForFriend.map(lambda x: (x[1], x[0])).sortByKey()
results = sortedResults.collect()


for result in results:
    print(str(result[0]) + "\t \t" + str(result[1]))

