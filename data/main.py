from pyspark import SparkContext
import itertools

def print_rdd(rdd, desc):
    for t in rdd.collect():
        print(t)
    print(desc)

sc = SparkContext("spark://spark-master:7077", "PopularItems")

# 0. Read data in
# each worker loads a piece of the data file
data = sc.textFile("/tmp/data/access.log", 2)

# 1. line -> (user_id, item_id clicked on by the user)
step1 = data.map(lambda line: line.split(" "))

# 2. Group data into (user_id, list of item ids they clicked on)
step2 = step1.groupByKey()

# 3. Transform into (user_id, (item1, item2)) where item1 and item2 are pairs of items the user clicked on
# NOTE: if a user only liked 1 item, then that user's items will not be considered.
def combine(s2):
    arr = list(s2[1])
    out = []
    for combination in itertools.combinations(arr, 2):
        out.append((s2[0], combination))
    return out
step3 = step2.flatMap(combine)

# 4. Transform into ((item1, item2), list of user1, user2 etc) where users are all the ones who co-clicked (item1, item2)
def reverse(s3):
    return (s3[1], s3[0])
step4 = step3.map(reverse).groupByKey()

# 5. Transform into ((item1, item2), count of distinct users who co-clicked (item1, item2)
step5 = step4.map(lambda s4: (s4[0], len(s4[1])))
# print_rdd(step5, "step5: ((item1, item2), len([user_id]))")

# 6. Filter out any results where less than 3 users co-clicked the same pair of items
step6 = step5.filter(lambda s5: s5[1]>2)
# print_rdd(step6, "step6 (filtered): ((item1, item2), len([user_id]))")

# output results
for t in step6.collect():
    print("OUTPUT: Items {} and {} were co-clicked {} times.".format(t[0][0], t[0][1], t[1]))

sc.stop()
