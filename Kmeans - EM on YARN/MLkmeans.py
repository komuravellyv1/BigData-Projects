

from pyspark.mllib.clustering import KMeans, KMeansModel
from numpy import array
from math import sqrt
from pyspark import SparkContext
# Load and parse the data
sc = SparkContext()

data = sc.textFile("/user/hduser/venkat/iris.txt")
print data.first()
parsedData = data.map(lambda line: array([float(x) for x in line.split(' ')]))

# Build the model (cluster the data)
clusters = KMeans.train(parsedData, 2, maxIterations=10, initializationMode="random")
prediction = clusters.predict(parsedData)
print clusters.centers
# Evaluate clustering by computing Within Set Sum of Squared Errors
def error(point):
    center = clusters.centers[clusters.predict(point)]
    return sqrt(sum([x**2 for x in (point - center)]))

WSSSE = parsedData.map(lambda point: error(point)).reduce(lambda x, y: x + y)
print("Within Set Sum of Squared Error = " + str(WSSSE))

# Save and load model
myModelPath = "/user/hduser/output/kmeans_output"
clusters.save(sc, myModelPath)
sameModel = KMeansModel.load(sc, myModelPath)
