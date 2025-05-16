package app.recommender.baseline

import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel


class BaselinePredictor() extends Serializable {

  // (mvID, avgMvDev)
  private var state: Map[Int, Double] = _
  // Average movie ratings
  private var avgUsrRt: Map[Int, Double] = _
  // Global average movie rating (Double), used when a user has no ratings
  private var globalMvRtAvg: Double = -1.0



  private def scale(x: Double, usrAvg: Double): Double =
    if x > usrAvg then 5.0 - usrAvg else if x < usrAvg then usrAvg - 1.0 else 1.0

  private def normalisedDev(rt: Double, usrAvg: Double): Double = {
    (rt - usrAvg) / scale(rt, usrAvg)
  }

  private def ratingPred(usrAvg: Double, mvAvgDev: Double): Double = {
    usrAvg + mvAvgDev * scale(usrAvg + mvAvgDev, usrAvg)
  }



  def init(ratingsRDD: RDD[(Int, Int, Option[Double], Double, Int)]): Unit = {

    val ratings = ratingsRDD.map{ case (uID, mvID, oldRt, rt, t) => ((uID, mvID), rt) }

    val emptyAvg = (0.0, 0L)

    val globalRt = ratings.aggregate(emptyAvg)(
      { (acc, rt) => (acc._1 + rt._2, acc._2 + 1) }, { (a, b) => (a._1 + b._1, a._2 + b._2) })
    globalMvRtAvg = if globalRt._2 > 0L then globalRt._1 / globalRt._2 else 0.0

    val avgUsrRtRDD = ratings.map { case ((uID, _), rt) => (uID, rt) }.aggregateByKey(emptyAvg)(
      {(acc, rt) => (acc._1 + rt, acc._2 + 1)}, {(a, b) => (a._1 + b._1, a._2 + b._2)})
      .mapValues{ case (tot, cnt) => if cnt == 0L then 0.0 else tot / cnt }
    avgUsrRt = avgUsrRtRDD.collectAsMap().toMap

    val devs = ratings.map{ case ((uID, mvID), rt) => (uID, (mvID, rt)) }.leftOuterJoin(avgUsrRtRDD)
      .map{
        case (uID, ((mvID, rt), Some(avg))) => ((uID, mvID), (rt - avg) / scale(rt, avg))
        case (uID, ((mvID, rt), None)) => ((uID, mvID), 0.0) } // Shouldn't happen

    state = devs.map { case((_, mvID), dev) => (mvID, dev) }.aggregateByKey(emptyAvg)(
      {(acc, dev) => (acc._1 + dev, acc._2 + 1)}, {(a, b) => (a._1 + b._1, a._2 + b._2)})
      .mapValues(avgDev => if avgDev._2 > 0L then avgDev._1 / avgDev._2 else 0.0).collectAsMap().toMap
  }

  def predict(userId: Int, movieIds: RDD[Int]): RDD[(Int, Double)] = {

    val usrAvg = avgUsrRt.getOrElse(userId, globalMvRtAvg)

    movieIds.map(mvID =>
      val dev = state.getOrElse(mvID, 0.0)
      val pred = usrAvg + (dev * scale((usrAvg + dev), usrAvg))
      (mvID, pred)
    )
  }
}
