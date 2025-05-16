package app.recommender

import app.recommender.baseline.BaselinePredictor
import app.recommender.collaborativeFiltering.CollaborativeFiltering
import app.recommender.LSH.{LSHIndex, NNLookup}
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

/**
 * Class for performing recommendations
 */
class Recommender(sc: SparkContext, index: LSHIndex, ratings: RDD[(Int, Int, Option[Double], Double, Int)]) extends Serializable {
  private val nn_lookup = new NNLookup(index)
  private val collaborativePredictor = new CollaborativeFiltering(10, 0.1, 0, 4)
  collaborativePredictor.init(ratings)

  private val baselinePredictor = new BaselinePredictor()
  baselinePredictor.init(ratings)

  // watchedMvs(uID) = all movies watched by user uID
  private val watchedMvs = ratings.map{ case (uID, mvID, _, _, _) => (uID, mvID) }
    .aggregateByKey(Set.empty[Int])({(acc, mvID) => acc + mvID}, {(a, b) => a ++ b})
    .collectAsMap().toMap

  /**
   * Returns the top K recommendations for movies similar to the List of genres
   * for userID using the BaseLinePredictor
   */
  def recommendBaseline(userId: Int, genre: List[String], K: Int): List[(Int, Double)] = {

    val genreRDD = sc.parallelize(List(genre))
    val watchedByUsr = watchedMvs.getOrElse(userId, Set.empty[Int])

    // lookup(genreRDD) = ((List(queryN, ...), List((mvID, mvName, genres), ...))
    // moviesToPred = all movie IDs returned by lookup that have not been watched by user userId
    val moviesToPred = nn_lookup.lookup(queries = genreRDD).map{
      case (queries, List((mvID, mvName, genres))) => mvID }.filter(!watchedByUsr.contains(_))

    baselinePredictor.predict(userId = userId, movieIds = moviesToPred)
      .sortBy(pred => pred._2).take(K).toList
  }

  /**
   * The same as recommendBaseline, but using the CollaborativeFiltering predictor
   */
  def recommendCollaborative(userId: Int, genre: List[String], K: Int): List[(Int, Double)] = {

    val genreRDD = sc.parallelize(List(genre))
    val watchedByUsr = watchedMvs.getOrElse(userId, Set.empty[Int])

    // lookup(genreRDD) = ((List(queryN, ...), List((mvID, mvName, genres), ...))
    // moviesToPred = all movie IDs returned by lookup that have not been watched by user userId
    val moviesToPred = nn_lookup.lookup(queries = genreRDD).map {
      case (queries, List((mvID, mvName, genres))) => mvID
    }.filter(!watchedByUsr.contains(_))

    collaborativePredictor.predict(userId = userId, movieIds = moviesToPred)
      .sortBy(pred => pred._2).take(K).toList
  }
}