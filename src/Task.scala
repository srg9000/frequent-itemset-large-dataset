import scala.math.Ordering.Implicits._

object Task {

    val RATINGS_USER_ID_INDEX = 0
    val RATINGS_MOVIE_ID_INDEX = 1
    val RATINGS_RATING_ID_INDEX = 2
    val RATINGS_TIMESTAMP_ID_INDEX = 3

    val USERS_USER_ID_INDEX = 0
    val USERS_GENDER_INDEX = 1
    val USERS_AGE_INDEX = 2
    val USERS_OCCUPATION_INDEX = 3
    val USERS_ZIPCODE_INDEX = 4

    def main(args: Array[String]): Unit = {

        //Getting configuration
        val ratingsFile = args(0)
        val usersFile = args(1)
        val support = args(2).toInt
        val sparkContext = SparkContextBuilder.getOrCreate()

        //Reading input files
        val ratings = IOHandling.readInput(ratingsFile, sparkContext)
            .map(x => x.split("::")) // Splitting input
            .map(x => (x(RATINGS_USER_ID_INDEX), x)) //Creating PairRDD

        val users = IOHandling.readInput(ratingsFile, sparkContext)
            .map(x => x.split("::")) //Splitting input
            .filter(x => x(USERS_GENDER_INDEX) == "M") //Filtering out Males
            .map(x => (x(USERS_USER_ID_INDEX), x)) //creating PairRDD

        val male_users_rating_join = users.join(ratings).map {
            case (key, (users_tuple, ratings_tuple)) => (users_tuple(USERS_USER_ID_INDEX), ratings_tuple(RATINGS_MOVIE_ID_INDEX).toInt)
        }

        val transactions = male_users_rating_join.groupByKey().map(x => x._2)

        val frequentItems = new SONAlgorithm().son(transactions , support)

        IOHandling.writeOutput(frequentItems.sortWith(lengthSort), "Sriram_Baskaran_SON.case1_" + support + ".txt")

    }

    def lengthSort(seq1: List[Int], seq2: List[Int]): Boolean = {
        if(seq1.size != seq2.size)
            seq1.size < seq2.size
        else
            Ordering[List[Int]].compare(seq1, seq2) <= 0
    }
}
