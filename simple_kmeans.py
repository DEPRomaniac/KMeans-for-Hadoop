import math
import sys
from random import shuffle
from time import time

def readtxt(fileName):
    f = open(fileName,'r')
    lines = f.read().splitlines()
    f.close()
    items = []
    for i in range(0,len(lines)):
        line = lines[i].split(',')
        itemFeatures = []
        for j in range(len(line)):
            v = float(line[j])
            itemFeatures.append(v) 
        items.append(itemFeatures)
    # shuffle(items)
    return items


def initMeans(items,k):
    
    f = len(items[0])
    means = [[0 for i in range(f)] for j in range(k)]
    
    for i in range(len(means)):
        means[i] = items[i] 

    return means

def eucDis(x,y):
    dis = 0
    for i in range(len(x)):
        dis += math.pow(x[i]-y[i],2)

    return math.sqrt(dis)

def updateMean(n,mean,item):
    for i in range(len(mean)):
        m = mean[i]
        m = (m*(n-1)+item[i])/float(n)
        mean[i] = round(m,3)
    
    return mean

def findClusters(means,items):
    clusters = [[] for i in range(len(means))]
    for item in items:
        index = Classify(means,item)
        clusters[index].append(item)
    return clusters


def Classify(means,item):
    
    minimum = sys.maxsize
    index = -1
    for i in range(len(means)):
        dis = eucDis(item,means[i])
        if(dis < minimum):
            minimum = dis
            index = i
    
    return index

def calcMeans(k,items):
    
    maxIter=100000

    means = initMeans(items,k)
    
    clusterSizes = [0 for i in range(len(means))]

    belongsTo = [0 for i in range(len(items))]

    for e in range(maxIter):
        noChange = True
        for i in range(len(items)):
            item = items[i]
            index = Classify(means,item)
            clusterSizes[index] += 1
            means[index] = updateMean(clusterSizes[index],means[index],item)
            if(index != belongsTo[i]):
                noChange = False

            belongsTo[i] = index
        if(noChange):
            break

    return means



time1 = time()

items = readtxt('data.txt')
k = 5
means = calcMeans(k,items)
clusters = findClusters(means,items)
print(means)

time2 = time()
print("elapsed time: ", time2-time1, 'sec')
# print(clusters)

