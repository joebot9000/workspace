import scala.io.Source
import java.io._
import Array._

object Test {
   def main(args: Array[String]) {
      var myList = Array(1.9, 2.9, 3.4, 3.5)
      
      // Print all the array elements
      for ( x <- myList ) {
         println( x )
      }

      // Summing all elements
      var total = 0.0;
      for ( i <- 0 to (myList.length - 1)) {
         total += myList(i);
      }
      println("Total is " + total);

      // Finding the largest element
      var max = myList(0);
      for ( i <- 1 to (myList.length - 1) ) {
         if (myList(i) > max) max = myList(i);
      }
      println("Max is " + max);
    
      var min = myList(0);
      for ( x <- myList ) {
       if( x <= min) min = x;   
      }
      println("Min is " + min )
      
      
      //concat arrays
      var myList1 = Array(1.9, 2.9, 3.4, 3.5)
      var myList2 = Array(8.9, 7.9, 0.4, 1.5)

      var myList3 =  concat( myList1, myList2)
      
      // Print all the array elements
      for ( x <- myList3 ) {
         println("concat array: " + x )
      }
   }
}