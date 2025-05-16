package app.recommender.LSH

import org.apache.spark.HashPartitioner
import org.apache.spark.rdd.RDD
import org.apache.spark.rdd.RDD.rddToPairRDDFunctions

import scala.reflect.ClassTag

/**
 * Class for indexing the data for LSH
 *
 * @param data The title data to index
 * @param seed The seed to use for hashing keyword lists
 */
class LSHIndex(data: RDD[(Int, String, List[String])], seed : IndexedSeq[Int]) extends Serializable {
  private val minhash = new MinHash(seed)
  private val state: RDD[(IndexedSeq[Int], List[(Int, String, List[String])])] = {
    // data: (mvID, mvName, genres) -> RDD[(bucket, List[(mvID, mvName, genres)])]

    val bucketedData = data.map { case (mvID, mvName, genres) => (minhash.hash(genres), (mvID, mvName, genres)) }
    val buckets = bucketedData.aggregateByKey(List.empty[(Int, String, List[String])])(
      { (acc, v) => v :: acc }, { (ls1, ls2) => ls1 ::: ls2 })

    // Step 1: (bucket, List((mvID, mvName, genres), ...)
    data.partitioner match
      case Some(partitioner) => buckets.partitionBy(partitioner)
      case None => buckets
  }

  /**
   * Hash function for an RDD of queries.
   *
   * @param input The RDD of keyword lists
   * @return The RDD of (signature, keyword list) pairs
   */
  def hash(input: RDD[List[String]]): RDD[(IndexedSeq[Int], List[String])] = {
    input.map(x => (minhash.hash(x), x))
  }

  /**
   * Return data structure of LSH index for testing
   *
   * @return Data structure of LSH index
   */
  def getBuckets()
  : RDD[(IndexedSeq[Int], List[(Int, String, List[String])])] = state


  /**
   * Lookup operation on the LSH index
   *
   * @param queries The RDD of queries. Each query contains the pre-computed signature
   *                and a payload
   * @return The RDD of (signature, payload, result) triplets after performing the lookup.
   *         If no match exists in the LSH index, return an empty result list.
   */
  def lookup[T: ClassTag](queries: RDD[(IndexedSeq[Int], T)])
  : RDD[(IndexedSeq[Int], T, List[(Int, String, List[String])])] = {

    // queries is [signature, payload]
    queries.leftOuterJoin(state).mapValues {
      case (pl, Some(bucket)) => (pl, bucket)
      case (pl, None) => (pl, List.empty[(Int, String, List[String])])
    }.map { case (sig, (pl, bucket)) => (sig, pl, bucket) }
  }
}
