package app.loaders

import breeze.io.CSVReader
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

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

    // ID,
    val allMovies = sc.textFile(path)
    val sep = '|'

    allMovies.map( { rdd =>

      val line = rdd.mkString.split("\\|")
      (line(0).toInt, line(1), line.drop(2).toList)

//      val ret = rdd.lines().map(line =>
//        val lineTuple = line.split("\\|")
//        (lineTuple(0), lineTuple(1), lineTuple.drop(2)))
//
//      ret.toList

    } )
  }
}

