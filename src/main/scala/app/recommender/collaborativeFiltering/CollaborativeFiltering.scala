package app.recommender.collaborativeFiltering

import org.apache.spark.mllib.recommendation.{ALS, MatrixFactorizationModel, Rating}
import org.apache.spark.rdd.RDD


class CollaborativeFiltering(rank: Int,
                             regularizationParameter: Double,
                             seed: Long,
                             n_parallel: Int) extends Serializable {

  // NOTE: set the parameters according to the project description to get reproducible (deterministic) results.
  private val maxIterations = 20
  private var model: MatrixFactorizationModel = _

  def init(ratingsRDD: RDD[(Int, Int, Option[Double], Double, Int)]): Unit = {

    val ratings = ratingsRDD.map{ case (uID, mvID, oldRt, rt, t) => Rating(uID, mvID, rt) }
    model = ALS.train(
      ratings = ratings,
      rank = rank,
      iterations = maxIterations,
      lambda = regularizationParameter,
      blocks = n_parallel,
      seed = seed
    )
  }

  def predict(userId: Int, movieIds: RDD[Int]): RDD[(Int, Double)] = {
    val usrMvs = movieIds.map(movieId => (userId, movieId))
    model.predict(usrMvs).map(rating => (rating.product, rating.rating))
  }

}