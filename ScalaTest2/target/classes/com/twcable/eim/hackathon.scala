import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.mllib.clustering.KMeans
import org.apache.spark.mllib.linalg.Vectors
import org.apache.log4j._
import org.apache.spark.mllib._

Logger.getLogger("org").setLevel(Level.OFF)
Logger.getLogger("akka").setLevel(Level.OFF)

case class TransactionSalesDetails(customer: Int, tier: Int, elFlag: Int, sales: Double, discounts: Double)
case class TransactionProductDetails(customer: Int, tier: Int, elFlag: Int, Double, upc: String, description: String, mupc: String, subcategory: String, subcat_num: String, category: String,cat_num: Int, department: String, dept_num: String, quantity: Int)
case class TransactionLocationDetails(customer: Int, tier: Int, homeStore: String, homeStoreLat: Double, homeStoreLon: Double, distance: Double, elFlag: Int, store: String, storeLat: Double,storeLon: Double)

object hackathon{    
             def main(args: Array[String]) {
                          
                        val sparkConf = new SparkConf().setAppName("hackathon kmeans")
                        val sc = new SparkContext(sparkConf)
                        val numClusters = 2
                        val numIterations = 20
                        val salesCol = 22
                        val discCol = 23
                                                         
                        //load up all the trans
                        //val trans = sc.textFile("/data/hackathon/raw/").filter(line => !line.contains("customer")).map(_.split("\\|"))
                        //val trans = sc.textFile("/data/customer_sample/customer_sample.bsv").filter(line => !line.contains("customer")).map(_.split("\\|"))
                        
                        val trans = sc.textFile("/data/sample_no_header/").map(_.split("\\|"))
                        
                        //work on transaction sales model
                        //val tsdTrainingData = trans.filter(e => e(7) == "1").map(item => item(7).toDouble + "," + item(21).toDouble  + "," + item(22).toDouble).map(s => Vectors.dense(s.split(',').map(_.toDouble))).cache()
                        //val tsdPerdictData = trans.filter(e => e(7) == "0").map(item => item(7).toDouble + "," + item(21).toDouble + "," + item(22).toDouble).map(s => Vectors.dense(s.split(',').map(_.toDouble))).cache()
                      
                         val tsdData = trans.map(item => item(8).toDouble + "," +  (
                             if(item(salesCol).toDouble > 0 && item(salesCol).toDouble <= 100) 
                             {0.1} 
                             else if(item(salesCol).toDouble > 100 && item(salesCol).toDouble <= 300)
                             {0.3}
                             else if(item(salesCol).toDouble > 500 && item(salesCol).toDouble <= 700)
                             {0.6}
                             else if(item(salesCol).toDouble > 700 && item(salesCol).toDouble <= 1000)
                             {0.8}
                             else if(item(salesCol).toDouble > 1000)
                             {1}
                             else 
                             {0}
                             )
                             + "," + 
                             (
                              if(item(discCol).toDouble > 0 && item(discCol).toDouble <= 5) 
                             {0.1} 
                             else if(item(discCol).toDouble > 5 && item(discCol).toDouble <= 10)
                             {0.3}
                             else if(item(discCol).toDouble > 10 && item(discCol).toDouble <= 15)
                             {0.6}
                             else if(item(discCol).toDouble > 15 && item(discCol).toDouble <= 20)
                             {0.8}
                             else if(item(discCol).toDouble > 20)
                             {1}
                             else 
                             {0}
                              )
                         ).map(s => Vectors.dense(s.split(',').map(_.toDouble))).cache()
                        
                        val tsdModel = KMeans.train(tsdData, numClusters, numIterations)

                        val clusterCenters = tsdModel.clusterCenters.map(_.toArray)
                        
                        val cost = tsdModel.computeCost(tsdData)
                        println("Cost: " + cost)
                        
                        //for (i <- 0 until numClusters) {
                          //println(s"\nCLUSTER $i:")
                        //  val tsdOutput = tsdData.foreach { t =>
                         //   if (tsdModel.predict(t) == i) {
                           //   println(t)
                           // }
                         // }
                        //}
                             
                        
                       val tsdPredictions = tsdData.map(t => tsdModel.predict(t) + "," + t.toString.replace("[", "").replace("]", ""))
                       
                       //Save predictions
                       tsdPredictions.saveAsTextFile("/user/user40/hackathon/working/tsdPredications/")
                       
                       //Save output
                       lblPreds.map(t => t.toString.replace("(", "").replace(")", ""))saveAsTextFile("/user/user40/hackathon/working/pred/")
                       
                       sc.stop()
                    
                    }
            }
}