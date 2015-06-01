package com.twcable.eim

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.sql.SQLContext

case class Person(name: String, age: Int, jobId: Int)
case class Job(jobId: Int, jobName: String)

object scalaApp{
 
def main(args: Array[String]) {
  
val sparkConf = new SparkConf().setAppName("RDDRelation")
val sc = new SparkContext(sparkConf)
val sqlContext = new SQLContext(sc)

import sqlContext.createSchemaRDD
import sqlContext._

val jobs = sc.textFile("/user/root/data/jobs/input/jobs.txt").map(_.split(",")).map(p => Job(p(0).trim.toInt, p(1)))
val people = sc.textFile("/user/root/data/people/input/people.txt").map(_.split(",")).map(p => Person(p(0), p(1).trim.toInt, p(2).trim.toInt))

people.registerTempTable("people")
jobs.registerTempTable("jobs")

val people_jobs = sqlContext.sql("""SELECT p.name, j.jobName from jobs j join people p on j.jobId = p.jobId""")

people_jobs.saveAsTextFile("/user/root/data/people_jobs/output/")

sc.stop()

}
}
