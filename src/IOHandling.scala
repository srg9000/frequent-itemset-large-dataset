import java.io.{File, PrintWriter}

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

/**
  * Handles reading and writing of data.
  * The reader requires a Spark Context as input as it reads the file paralelly/
  *
  */

object IOHandling {

    def writeOutput(frequent_itemsets: Array[List[Int]], filename: String): Unit ={
        val pw = new PrintWriter(new File(filename))

        var old_size = 1
        var first = true
        for(itemset <-frequent_itemsets){
            if(old_size != itemset.size){
                pw.write("\n")
                old_size = itemset.size
            }else if(!first){
                pw.write(", ")
            }else{
                first = false
            }
            pw.write(itemset.mkString("(",",",")"))
        }
        pw.write("\n")
        pw.close()
    }

    def readInput(input: String, sparkContext: SparkContext): RDD[String] ={
        sparkContext.textFile(input)
    }
}
