# Identification of Frequent Itemsets in Large datasets

This identifies frequent itemsets on a large dataset. This uses Apache Spark for complete distributed processing of the input data and identifying frequent itemsets. This implements the SON algorithm for frequent itemset identification.

The repo is organized in the following way,

```
src
|- SONAlgorithm.scala
|- IOHandler.scala
|- SparkContextBuilder.scala
|- Task.scala (which contains the main function)
```

This uses movie lens dataset provided as `ratings.dat` and `users.dat`. The code is flexible to use any bigger datasets of movie lens. The data set can be found [here](https://grouplens.org/datasets/movielens/).

The class `SONAlgorithm` contains the distributed implementation of the frequent itemset identification. `Task` is a tester code that identifies frequently watched movies by all `male` users in the movie lens dataset. The `IOHandler` and `SparkContextBuilder` are helper classes for data processing and context initialization.

## SON Algorithm
The SON algorithm is a 2 pass algorithm and works in the following way.
1. It takes a large set of buckets that are distributed across `p` clusters and a support value `s` to determine frequent itemset.
1. An `Apriori` algorithm is run on each cluster separately with a reduced support of `s/p`.
1. The itemsets generated from each cluster are candidate itemsets in the entire dataset. An itemset is candidate itemset if it is frequent in any one of the `p` clusters.
1. Count the candidate itemsets in the original dataset and filter those that are more than the original support threshold `s`.
1. Return the generated itemsets.

## Apriori Algorithm
This is the base algorithm for identifying frequent itemsets in a particular batch. The intuition behind apriori is that `any itemset will not be frequent unless all of its subsets are frequent`.

## Remarks
The distributed nature of the SON algorithm allows you to easily identify frequent itemsets in a distributed way. The main bottle neck in implementing frequent itemsets is identifying frequent pairs. This algorithm allows you to find frequent pairs in a distributed way which saves a lot of time. This method relies on the partitioning methodology of the map reduce algorithm.

CAUTION: There can be cases when the partitions are created in a skewed way which creates all itemsets as frequent which doesn't give us any advantage. It is essential that the transactions (or buckets) are randomized before partitioning them.

NOTE: This contains only scala code and not a runnable component. This is part of a much larger project and the code has been extracted and anonymized. Feel free to import it into personal projects and use it for learning purpose.
