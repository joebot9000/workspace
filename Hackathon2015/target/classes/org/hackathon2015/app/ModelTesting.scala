package org.hackathon2015.app

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.regression._
import org.apache.spark.mllib.classification._
import org.apache.spark.SparkContext
import org.apache.log4j._
import org.apache.spark.mllib._

Logger.getLogger("org").setLevel(Level.OFF)
Logger.getLogger("akka").setLevel(Level.OFF)


object modelTesting{
 
def main(args: Array[String]) {

  
val sparkConf = new SparkConf().setAppName("Store Logistic Regression")
val sc = new SparkContext(sparkConf)

var transTraining = sc.textFile("/data/model/incoming/trainingData/")
val transPredicting = sc.textFile("/data/model/incoming/predictingData/")

val trainingData = transTraining.map { line =>
      val parts = line.split("\t")
      val label = parts(0).toDouble
      val features = parts.tail.map(x => x.toDouble).toArray
      LabeledPoint(label, Vectors.dense(features.toArray))
    }


    // Run training algorithm to build the model
    val numIterations = 1000
    val model = LogisticRegressionWithSGD.train(trainingData, numIterations)   
    
    
    
    val predictingData: org.apache.spark.rdd.RDD[org.apache.spark.mllib.regression.LabeledPoint] = sc.parallelize(transTraining.take(1000).map { line =>
      val parts = line.split("\\t")
      val label = parts(0).toDouble
      val features = parts.tail.map(x => x.toDouble).toArray
      LabeledPoint(label, Vectors.dense(features.toArray))
    })

    
    
    var correct = 0
    val lblPreds = predictingData.map { p =>
      val prediction = model.predict(p.features)
      if(p.label == prediction) {correct = 1} else {correct = 0}
      (p.label, prediction, correct)
    }
    
    println("total:" + lblPreds.count + " correct: " + correct + " model accuracy: " + correct.toDouble / lblPreds.count.toDouble * 100)
        
    
    lblPreds.saveAsTextFile("/user/user40/hackathon/working/lblPreds/")

    sc.stop()

}




