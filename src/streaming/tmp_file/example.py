#
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

from __future__ import print_function

from pyspark import SparkContext
from pyspark.streaming import StreamingContext
# $example on$
from pyspark.mllib.linalg import Vectors
from pyspark.mllib.regression import LabeledPoint
from pyspark.mllib.clustering import StreamingKMeans
import numpy
from numpy import array
# $example off$

if __name__ == "__main__":
    sc = SparkContext(appName="StreamingKMeansExample")  # SparkContext
    ssc = StreamingContext(sc, 1)

    # $example on$
    # we make an input stream of vectors for training,
    # as well as a stream of vectors for testing
    def parse(lp):
        label = float(lp[lp.find('(') + 1: lp.find(')')])
        vec = Vectors.dense(lp[lp.find('[') + 1: lp.find(']')].split(','))

        return LabeledPoint(label, vec)

    trainingData = sc.textFile("hdfs://ec2-52-10-138-212.us-west-2.compute.amazonaws.com:9000/streamscript/data/train_data_5.txt")\
        .map(lambda line: Vectors.dense([float(x) for x in line.strip().split(' ')]))

    testingData = sc.textFile("hdfs://ec2-52-10-138-212.us-west-2.compute.amazonaws.com:9000/streamscript/data/test_data_5.txt").map(parse)

    trainingQueue = [trainingData]
    testingQueue = [testingData]

    trainingStream = ssc.queueStream(trainingQueue)
    testingStream = ssc.queueStream(testingQueue)

    # We create a model with random clusters and specify the number of clusters to find
    model = StreamingKMeans(k=2, decayFactor=1.0).setInitialCenters([Vectors.dense([110,14]).toArray(),Vectors.dense([111,10]).toArray()], array([1]))

    # Now register the streams for training and testing and start the job,
    # printing the predicted cluster assignments on new data points as they arrive.
    model.trainOn(trainingStream)
    print("Intermediate centers: " + str(model.latestModel().centers))

    result = model.predictOnValues(testingStream.map(lambda lp: (lp.label, lp.features)))
    print("predictOnValues result:")
    result.pprint()

    ssc.start()
    ssc.stop(stopSparkContext=True, stopGraceFully=True)
    # $example off$

    print("Final centers: " + str(model.latestModel().centers))