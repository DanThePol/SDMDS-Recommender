package app.loaders

import breeze.io.CSVReader
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel

import java.io.File

/**
 * Helper class for loading the input
 *
 * @param sc   The Spark context for the given application
 * @param path The path for the input file. For tests, this is within the resources directory, e.g. path = "/movies_small.csv".
 * @note You can use getClass.getResource(path).getPath to get the full path to a file in the resources directory
 */
class MoviesLoader(sc: SparkContext, path: String) extends Serializable {

  /**
   * Read the title file in the given path and convert it into an RDD
   *
   * @return The RDD for the given titles
   */
  def load(): RDD[(Int, String, List[String])] = {

    // Need to return: ID, title, genres
    val allMoviesPath = getClass.getResource(path).getPath
    val allMovies = sc.textFile(allMoviesPath)
    val sep = '|'

    val rddFinal = allMovies.map( { rdd =>
      val line = rdd.replace("\"", "").split("\\|")
      val ret = (line(0).toInt, line(1), line.drop(2).toList)
      ret
    })

    rddFinal.persist(StorageLevel.MEMORY_ONLY)
    rddFinal
  }
}

