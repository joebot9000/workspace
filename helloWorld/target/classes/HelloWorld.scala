import scala.io.Source
import java.io._
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf

object ReadFile {
 def main(args: Array[String]) {
//    val filename = "/join_lookup_test/EDW_CMN_REF_LOOKUPS_CODE_TO_280.dat"
//    val lines = Source.fromFile(filename).getLines.toList filter{ x => x.toUpperCase().contains("SERVICE")}
//    
//    lines.foreach { println }
    
    
    val conf = new SparkConf().setAppName(appName).setMaster(master) 
    new SparkContext(conf)
    
    val distFile = sc.textFile("/join_lookup_test/EDW_CMN_REF_LOOKUPS_CODE_TO_280.dat")
    val lineLengths = lines.map(s => s.length)
    
    lineLenghts.println();
    
    
    //println(lines)
    
    
//    for (line <- Source.fromFile(filename).getLines().filter{ x => x.toUpperCase().contains("SERVICE")})
//    {
//      println(line)
//    }
  }
}