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


object logisticRegression{
 
def main(args: Array[String]) {
 
val sparkConf = new SparkConf().setAppName("Store Logistic Regression")
val sc = new SparkContext(sparkConf)

val transTraining = sc.textFile("/data/model/incoming/trainingData/")
val transTesting = sc.textFile("/data/model/incoming/testingData/")

val trainingData = transTraining.map { line =>
      val parts = line.split("\\|")
      val label = parts(0).toDouble
      val features = parts.tail.map(x => x.toDouble).toArray
      LabeledPoint(label, Vectors.dense(features.toArray))
    }

val testingData = transTesting.map { line =>
      val parts = line.split("\\|")
      val label = parts(0).toDouble
      val features = parts.tail.map(x => x.toDouble).toArray
      LabeledPoint(label, Vectors.dense(features.toArray))
    }

    // Run training algorithm to build the model
    val numIterations = 20
    val model = LogisticRegressionWithSGD.train(trainingData, numIterations)   
        
    
    val lblPreds = testingData.map { p =>
      val prediction = model.predict(p.features)
      if(p.label == prediction) {correct = 1} else {correct = 0}
      (p.label, prediction, correct)
    }
    
    println("total:" + lblPreds.count + " correct: " + correct + " model accuracy: " + correct.toDouble / lblPreds.count.toDouble * 100)
        
    
    lblPreds.saveAsTextFile("/user/user40/hackathon/working/lblPreds/")

    sc.stop()

}



