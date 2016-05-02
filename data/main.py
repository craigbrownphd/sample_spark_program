from pyspark import SparkContext
from pyspark.resultiterable import ResultIterable
import itertools

def print_rdd(rdd, desc):
    for t in rdd.collect():

        if type(t[1]) == ResultIterable:
            list_str = ""
            for r in t[1]:
                list_str += r + ","
            print("({},[{}])".format(t[0], list_str))
        else:
            print(t)
    print(desc)

sc = SparkContext("spark://spark-master:7077", "PopularItems")

# 0. Read data in
# each worker loads a piece of the data file
data = sc.textFile("/tmp/data/access3.log", 2)

# 1. line -> (user_id, item_id clicked on by the user)
step1 = data.map(lambda line: line.split(" "))
print_rdd(step1, "step1: (user_id, item_id)")

# 2. Group data into (user_id, list of item ids they clicked on)
step2 = step1.groupByKey()
print_rdd(step2, "step2: (user_id, [item])")

# 3. Transform into (user_id, (item1, item2)) where item1 and item2 are pairs of items the user clicked on
# NOTE: if a user only liked 1 item, then that user's items will not be considered.
def combine(s2):
    arr = list(set(s2[1]))
    arr = sorted(arr)
    out = []
    for i in range(0, len(arr)):
        for j in range(i+1, len(arr)):
            out.append((s2[0], (arr[i], arr[j])))
    return out
step3 = step2.flatMap(combine)
# print_rdd(step3, "step3: (user_id, (item1, item2))")

# 4. Transform into ((item1, item2), list of user1, user2 etc) where users are all the ones who co-clicked (item1, item2)
def reverse(s3):
    return (s3[1], s3[0])
step4 = step3.map(reverse).groupByKey()
# print_rdd(step4, "step4: ((item1, item2), [user])")

# 5. Transform into ((item1, item2), count of distinct users who co-clicked (item1, item2)
# step51 = step4.map(reverse).distinct()
# printrdd_rdd(step51, "")
# step52 = step51.map(reverse)
# NOTE: need to sort the pair
step5 = step4.map(lambda s4: (s4[0], len(s4[1])))
print_rdd(step5, "step5: ((item1, item2), len([user_id]))")

# 6. Filter out any results where less than 3 users co-clicked the same pair of items
step6 = step5.filter(lambda s5: s5[1]>2)
# print_rdd(step6, "step6 (filtered): ((item1, item2), len([user_id]))")

# Output results
for t in step6.collect():
    print("\nOUTPUT: Items {} and {} were co-clicked {} times.\n".format(t[0][0], t[0][1], t[1]))
    # step6.saveAsTextFile("/tmp/data/results.output")

sc.stop()
