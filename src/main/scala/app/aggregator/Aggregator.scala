package app.aggregator

import org.apache.spark.{HashPartitioner, SparkContext}
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel

/**
 * Class for computing the aggregates
 *
 * @param sc The Spark context for the given application
 */
class Aggregator(sc: SparkContext) extends Serializable {

  // state = (mvID, (mvTitle, genres, (sum of all ratings of movie mvID [Double], number of ratings for movie mvID))) => mvID is the key
  private var state: RDD[(Int, (String, List[String], (Double, Long)))] = _
  private var partitioner: HashPartitioner = _

  /**
   * Use the initial ratings and titles to compute the average rating for each title.
   * The average rating for unrated titles is 0.0
   *
   * @param ratings The RDD of ratings in the file
   *        format: (user_id: Int, title_id: Int, old_rating: Option[Double], rating: Double, timestamp: Int)
   * @param title   The RDD of titles in the file
   */
  def init(
            ratings: RDD[(Int, Int, Option[Double], Double, Int)],
            title: RDD[(Int, String, List[String])]
          ): Unit = {

    // Idk wtf to do with this thing
    partitioner = new HashPartitioner(sc.defaultParallelism)

    // Title: (mvID, mvName, genres)
    // Rating: (uID, mvID, oldRt, rt, t)

    val titlesByKey = title.map{case(mvID, mvName, genres) => (mvID, (mvName, genres))}
    // Keep only most recent (uID, mvID)-keyed ratings (and then remove uID and timestamp information since useless
    val ratingsByKey = ratings.map { case (uID, mvID, _, rt, time) => ((mvID, uID), (rt, time)) }
      .reduceByKey((p1, p2) => if p1._2 > p2._2 then p1 else p2).map { case ((mvID, _), (rt, _)) => (mvID, rt) }

    val noRating = (0.0, 0L)
    val avgRatings = ratingsByKey.aggregateByKey(noRating, partitioner)(
      (acc, rt)  => (acc._1 + rt, acc._2 + 1),
      (a, b) => (a._1  + b._1, a._2  + b._2))

    state = titlesByKey.leftOuterJoin(avgRatings, partitioner).mapValues{
      case ((name, genres), Some((sum, cnt))) => (name, genres, (sum, cnt))
      case ((name, genres), None) => (name, genres, noRating)
    }.persist(StorageLevel.MEMORY_AND_DISK)
  }

  /**
   * Return pre-computed title-rating pairs.
   *
   * @return The pairs of titles and ratings
   */
  def getResult(): RDD[(String, Double)] = {
    state.map{
      case (_, (mvName, _, (tr, n))) =>
        if n > 0 then (mvName, tr / n) else (mvName, 0.0)
    }.partitionBy(partitioner)
  }

  /**
   * Compute the average rating across all (rated titles) that contain the
   * given keywords.
   *
   * @param keywords A list of keywords. The aggregate is computed across
   *                 titles that contain all the given keywords
   * @return The average rating for the given keywords. Return 0.0 if no
   *         such titles are rated and -1.0 if no such titles exist.
   */
  def getKeywordQueryResult(keywords: List[String]): Double = {

    val titles = state.filter{
      // Filter out all titles that do not contain any of the given keywords
      case (_, (_, genres, (_, _))) => keywords.forall(genres.contains(_)) // || mvName.contains(kw) (?)
    }

    // Return -1 if no corresponding titles are found
    if titles.isEmpty() then -1 else

      val ratedTitles = titles.filter{case (_, (_, _, (_, cnt))) => cnt > 0}

      if ratedTitles.isEmpty() then 0 else

        val res = ratedTitles.aggregate(0.0)(
          (acc, rt) => acc + (rt._2._3._1 / rt._2._3._2),
          (a, b) => a + b)

        res / ratedTitles.count()
  }


  /**
   * Use the "delta"-ratings to incrementally maintain the aggregate ratings
   *
   *  @param delta_ Delta ratings that haven't been included previously in aggregates
   *        format: (user_id: Int, title_id: Int, old_rating: Option[Double], rating: Double, timestamp: Int)
   */
  def updateResult(delta_ : Array[(Int, Int, Option[Double], Double, Int)]): Unit = {

    val delta = sc.parallelize(delta_, partitioner.numPartitions)
    val deltaByKey = delta.map{case(_, mvID, oldRt, rt, _) => (mvID, (oldRt, rt))}.partitionBy(partitioner)

    // Use mutable.Map to avoid constant memory allocations
    val noRating = (0.0, 0L)
    val countedDelta = deltaByKey.aggregateByKey(noRating, partitioner)(
      {
        (acc, rt) => rt._1 match
          // If user has already rated a movie, we need to add that delta of his rating to the total rating
          case Some(oldRt) => (acc._1 + (rt._2 - oldRt), acc._2)
          // Else if there was no pre-existing rating, simply add rating
          case None => (acc._1 + rt._2, acc._2 + 1)
      },
      (a, b) => (a._1  + b._1, a._2  + b._2))

    val newState = state.leftOuterJoin(countedDelta, partitioner).mapValues{
      case ((name, genres, (sum, cnt)), Some((deltaSum, deltaN))) => (name, genres, (sum + deltaSum, cnt + deltaN))
      case ((name, genres, (sum, cnt)), None) => (name, genres, (sum, cnt))
    }.persist()

    state.unpersist()
    state = newState
  }

}
