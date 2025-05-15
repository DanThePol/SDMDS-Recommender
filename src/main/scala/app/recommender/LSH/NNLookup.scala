package app.recommender.LSH

import org.apache.spark.rdd.RDD

/**
 * Class for performing LSH lookups
 *
 * @param lshIndex A constructed LSH index
 */
class NNLookup(lshIndex: LSHIndex) extends Serializable {

  /**
   * Lookup operation for queries
   *
   * @param input The RDD of keyword lists
   * @return The RDD of (keyword list, result) pairs
   */
  def lookup(queries: RDD[List[String]])
  : RDD[(List[String], List[(Int, String, List[String])])] = {

    val buckets = lshIndex.getBuckets()
    val bucketedQueries = lshIndex.hash(queries)

    bucketedQueries.leftOuterJoin(buckets).mapValues{
      case (query, Some(ls)) => (query, ls)
      case (query, None) => (query, List.empty[(Int, String, List[String])])
    }.map{ case(id, ls) => ls }
  }
}
