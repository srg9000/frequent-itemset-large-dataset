import org.apache.spark.{SparkConf, SparkContext}

/**
  * Builds spark context
  */

object SparkContextBuilder {

    val sparkConf: SparkConf = new SparkConf().setAppName("SON Algorithm").setMaster("local[*]")
    val sparkContext: SparkContext = new SparkContext(sparkConf)

    def getOrCreate(): SparkContext ={
        if(sparkContext != null)
            return sparkContext
        else{
            val sparkConf = new SparkConf().setAppName("SON Algorithm").setMaster("local[*]")
            new SparkContext(sparkConf)
        }

    }
}
