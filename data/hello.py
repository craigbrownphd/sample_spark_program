from pyspark import SparkContext

sc = SparkContext("spark://spark-master:7077", "PopularItems")

data = sc.textFile("/tmp/data/access.log", 2)     # each worker loads a piece of the data file

def line_to_list(line):
    return line.split(" ")

def list_to_pair(pair):
    return (pair[1], 1)

def sum_x_y(x,y):
    return x+y

pairs = data.map(line_to_list)      # tell each worker to split each line of it's partition: ()
pages = pairs.map(lambda pair: (pair[1], 1))     # re-layout the data to ignore the user id
count = pages.reduceByKey(sum_x_y)  # shuffle the data so that each key is only on one worker
                                    # and then reduce all the values by adding them together

output = count.collect()            # bring the data back to the master node so we can print it out
for page_id, count in output:
    print ("page_id %s count %d" % (page_id, count))
print ("Popular items done")

sc.stop()
