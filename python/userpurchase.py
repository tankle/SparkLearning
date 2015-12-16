# -*- coding: utf-8 -*-
from pyspark import SparkContext

# 创建一个sparkContext
sc = SparkContext("local[2]","first SparkApp")

#
data = sc.textFile("data/UserPurchaseHistory.csv").map(lambda line: line.split(",")).map(lambda record: (record[0], record[1], record[2]))

# 购买次数
numPurchases = data.count()

# 购买人数
numUsers = data.map(lambda record: record[0]).distinct().count()

# 总收入
totalRevenue = data.map(lambda record: float(record[2])).sum()

# 最畅销的物品
products = data.map(lambda record:(record[1], 1.0)).reduceByKey(lambda a,b:a + b).collect()

mostPopular = sorted(products, key=lambda x:x[1] , reverse=True)[0]

print("Total purchases: %d" % numPurchases)
print("Total users: %d" % numUsers)
print("Total Revenue: %d" % totalRevenue)
print("Most Popular product: %s buy %d products" % (mostPopular[0],mostPopular[1]))


