package app.loaders

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel

import java.io.File

/**
 * Helper class for loading the input
 *
 * @param sc The Spark context for the given application
 * @param path The path for the input file. For tests, this is within the resources directory, e.g. path = "/ratings_small.csv".
 * @note You can use getClass.getResource(path).getPath to get the full path to a file in the resources directory
 */
class RatingsLoader(sc : SparkContext, path : String) extends Serializable {

  /**
   * Read the rating file in the given path and convert it into an RDD
   *
   * @return The RDD for the given ratings
   */
  def load() : RDD[(Int, Int, Option[Double], Double, Int)] = {

    // Need to return:
    val allRatingsPath = getClass.getResource(path).getPath
    val allRatings = sc.textFile(allRatingsPath)
    val sep = '|'

    val rddFinal = allRatings.map({ rdd =>
      val line = rdd.replace("\"", "").split("\\|")
      
      val ret = (line(0).toInt, line(1).toInt, Option.empty[Double], line(2).toDouble, line(3).toInt)
      ret
    })

    rddFinal.persist(StorageLevel.MEMORY_ONLY)
    rddFinal
  }
}