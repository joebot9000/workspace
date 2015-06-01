package com.twcable.eim

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.regression._
import org.apache.spark.mllib.classification._

case class Person2(rating: String, income: Double, age: Int)

object personLinearRegression{
 
def main(args: Array[String]) {
 
val sparkConf = new SparkConf().setAppName("People Linear Regression")
val sc = new SparkContext(sparkConf)

def prepareFeatures(people: Seq[Person2]): Seq[org.apache.spark.mllib.linalg.Vector] = {
  val maxIncome = people map(_ income) max
  val maxAge = people map(_ age) max

  people map (p =>
    Vectors dense(if (p.rating == "A") 0.7 else if (p.rating == "B") 0.5 else 0.3,
      p.income / maxIncome,
      p.age.toDouble / maxAge))
}

def prepareFeaturesWithLabels(features: Seq[org.apache.spark.mllib.linalg.Vector]): Seq[LabeledPoint] =
  (0d to 1 by (1d / features.length)) zip(features) map(l => LabeledPoint(l._1, l._2))

  
val people = sc.textFile("/user/root/data/peopleLoans/input").map(_.split(",")).map(p => Person2(p(0), p(1).trim.toInt, p(2).trim.toInt))

//val joe = Person2("A", 500, 60)
//val joe = Person2("A", 500, 60)
//val tom = Person2("B",600,50)
//val tom = Person2("B",600,50)
//val people = Seq(joe, tom)

val data = sc.parallelize(prepareFeaturesWithLabels(prepareFeatures(people.collect())))

val splits = data randomSplit Array(0.8, 0.2)

val training = splits(0) cache
val test = splits(1) cache

val algorithm = new LinearRegressionWithSGD()
val model = algorithm run training
val prediction = model predict(test map(_ features))

val predictionAndLabel = prediction zip(test map(_ label))

predictionAndLabel.foreach((result) => println(s"predicted label: ${result._1}, actual label: ${result._2}"))


//people_jobs.saveAsTextFile("/user/root/data/people_jobs/output/")

sc.stop()

}
}
