import org.apache.spark.rdd.RDD


import collection.mutable.ListBuffer
import scala.collection.mutable

class SONAlgorithm {

    /**
      * Function that performs distributed apriori algorithm.
      *
      * @param transactions RDD of buckets
      * @param support input support
      * @return list of frequent itemsets.
      */

    def son(transactions: RDD[Iterable[Int]], support: Int): Array[List[Int]] = {
        val totalTransactions = transactions.count().toInt
        val partitionResults = transactions.mapPartitions(x => apriori(x, support, totalTransactions), preservesPartitioning = true)
        val candidateItemsets = partitionResults.map(itemset => (itemset, 1)).reduceByKey((itemset1, _) => itemset1).map(x => x._1)
        val candidateItemsetsCollected = candidateItemsets.collect()

        transactions
            .flatMap(x => for (itemset <- candidateItemsetsCollected if itemset.toSet.subsetOf(x.toSet)) yield (itemset, 1))
            .reduceByKey((x, y) => x + y)
            .filter(itemset_count_tuple => itemset_count_tuple._2 >= support).keys.collect()
    }

    /**
      * Returns itemsets that are frequent in a given set of transactions.
      *
      * @param transactions
      * @param itemsets
      * @param min_support
      * @return
      */

    def getFilteredItemsets(transactions: ListBuffer[Set[Int]], itemsets: Set[Set[Int]], min_support: Double): (Set[List[Int]], Set[Int]) ={

        val filteredItemsets = itemsets.map{
            itemset => (itemset, transactions.count(transaction => itemset.subsetOf(transaction)))
        }.filter(x => x._2 > min_support).map(x => x._1.toList)

        val frequent_items = filteredItemsets.map(x => x.toSet).reduce((x, y) => x ++ y)

        (filteredItemsets, frequent_items)
    }

    /**
      * Apriori algorithm on on one partition of the transactions
      * @param transactionSet transations in the partition
      * @param support original support
      * @param totalTransactions total number of transactions
      * @return
      */

    def apriori(transactionSet: Iterator[Iterable[Int]], support: Int, totalTransactions: Int): Iterator[List[Int]] = {

        var transactions : ListBuffer[Set[Int]] = ListBuffer()
        val singleton_counts :mutable.Map[Int, Int] = mutable.Map()
        var size = 0
        for(transaction <- transactionSet){
            val items_set = transaction.toSet
            transactions += items_set
            size = size + 1
            for(item <- items_set){
                if(!singleton_counts.contains(item)){
                    singleton_counts(item) = 0
                }
                singleton_counts(item) = singleton_counts(item) + 1
            }
        }
        val partition_support = support.toDouble/totalTransactions * size

        val frequent_item_counts = singleton_counts.filter( x => x._2 >= partition_support)
        var frequent_items = frequent_item_counts.keys.toList.sorted

        var frequent_itemsets: List[List[Int]] = List()
        frequent_itemsets ++= frequent_item_counts.map(x => List(x._1))
        var k : Int = 2 // starting to generate from pairs

        //Generate all other size itemsets. Loop runs till no new candidates are generated.
        while(frequent_items.nonEmpty){
            val temp_itemsets = frequent_items.toSet.subsets(k)
            val temp_frequent_itemsets = getFilteredItemsets(transactions, temp_itemsets.toSet, partition_support)
            frequent_items = temp_frequent_itemsets._2.toList.sorted
            frequent_itemsets ++= temp_frequent_itemsets._1.toList.map(x => x.sorted)
            k = k + 1
        }

        frequent_itemsets.iterator
    }
}
