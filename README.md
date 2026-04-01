# SDMDS-Recommender

A scalable movie recommender system built with **Apache Spark** and **Scala 3**. The system loads movie and ratings datasets, computes aggregated statistics, and produces personalized recommendations using three different approaches: a baseline predictor, collaborative filtering (ALS), and Locality-Sensitive Hashing (LSH) for genre-based nearest-neighbor lookup.

---

## Table of Contents

- [Project Structure](#project-structure)
- [Architecture](#architecture)
- [Data Format](#data-format)
- [Prerequisites](#prerequisites)
- [Building](#building)
- [Running Tests](#running-tests)
- [Usage](#usage)
- [Components](#components)
  - [Loaders](#loaders)
  - [Aggregator](#aggregator)
  - [Recommender](#recommender)
    - [Baseline Predictor](#baseline-predictor)
    - [Collaborative Filtering](#collaborative-filtering)
    - [LSH Index & Nearest-Neighbor Lookup](#lsh-index--nearest-neighbor-lookup)

---

## Project Structure

```
.
├── build.sbt                          # SBT build definition
├── project/
│   ├── build.properties               # SBT version (1.9.0)
│   └── plugins.sbt                    # sbt-assembly plugin
└── src/
    ├── main/
    │   ├── resources/
    │   │   ├── movies_small.csv       # Sample movies dataset
    │   │   └── ratings_small.csv      # Sample ratings dataset
    │   └── scala/app/
    │       ├── Main.scala             # Application entry point
    │       ├── aggregator/
    │       │   └── Aggregator.scala   # Incremental rating aggregation
    │       ├── loaders/
    │       │   ├── MoviesLoader.scala # Loads movie data into an RDD
    │       │   └── RatingsLoader.scala# Loads ratings data into an RDD
    │       └── recommender/
    │           ├── Recommender.scala  # Top-level recommendation orchestrator
    │           ├── baseline/
    │           │   └── BaselinePredictor.scala
    │           ├── collaborativeFiltering/
    │           │   └── CollaborativeFiltering.scala
    │           └── LSH/
    │               ├── LSHIndex.scala
    │               ├── MinHash.scala
    │               └── NNLookup.scala
    └── test/
        └── scala/
            └── MainTest.scala         # Test suite
```

---

## Architecture

```
MoviesLoader  ──┐
                ├──► Aggregator  ──►  getResult() / getKeywordQueryResult()
RatingsLoader ──┤
                │
                └──► LSHIndex  ──► NNLookup ──┐
                                               ├──► Recommender
                └──► BaselinePredictor ────────┤
                                               │
                └──► CollaborativeFiltering ───┘
                         (ALS)
```

1. **Loaders** read pipe-delimited CSV files into typed Spark RDDs.
2. **Aggregator** maintains per-movie average ratings and supports incremental updates.
3. **LSHIndex** builds a MinHash-based bucket index over movie genres for fast approximate nearest-neighbor search.
4. **BaselinePredictor** predicts ratings using normalised per-user and per-movie average deviations.
5. **CollaborativeFiltering** trains a matrix-factorisation model (ALS) and predicts ratings for unseen movies.
6. **Recommender** combines the LSH lookup with either predictor to return the top-`K` personalized recommendations.

---

## Data Format

Both dataset files are **pipe-separated** (`|`) CSV files stored under `src/main/resources/`.

### Movies (`movies_small.csv`)

```
<movieId>|<title>|<genre1>|<genre2>|...
```

| Field     | Type   | Description                        |
|-----------|--------|------------------------------------|
| `movieId` | Int    | Unique movie identifier            |
| `title`   | String | Movie title                        |
| `genre*`  | String | One or more genre tags (0 or more) |

### Ratings (`ratings_small.csv`)

```
<userId>|<movieId>|<rating>|<timestamp>
```

| Field       | Type   | Description                              |
|-------------|--------|------------------------------------------|
| `userId`    | Int    | Unique user identifier                   |
| `movieId`   | Int    | Movie being rated                        |
| `rating`    | Double | Rating value                             |
| `timestamp` | Int    | Unix timestamp of the rating event       |

The `RatingsLoader` exposes ratings as `RDD[(userId, movieId, Option[Double], rating, timestamp)]`, where the `Option[Double]` field holds a previous rating (used for incremental updates in the `Aggregator`).

---

## Prerequisites

| Requirement | Version |
|-------------|---------|
| JDK         | 21      |
| Scala       | 3.3.1   |
| Apache Spark| 3.5.1   |
| SBT         | 1.9.0   |

> **Note:** The build enforces JDK 21 at startup — using a different JDK version will cause an assertion error.

The `.sbtopts` file already configures the required `--add-opens` JVM flags so that Spark runs correctly under JDK 21.

---

## Building

Compile the project:

```bash
sbt compile
```

Produce a fat JAR (excluding Spark/Hadoop from the assembly):

```bash
sbt assembly
```

The resulting JAR is written to `target/scala-3.3.1/Project_2-assembly-0.1.1.jar`.

---

## Running Tests

```bash
sbt test
```

---

## Usage

Add your Spark logic inside `src/main/scala/app/Main.scala`:

```scala
object Main {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("app").setMaster("local[*]")
    val sc = SparkContext.getOrCreate(conf)

    // Load data
    val movies  = new MoviesLoader(sc, "/movies_small.csv").load()
    val ratings = new RatingsLoader(sc, "/ratings_small.csv").load()

    // Aggregate average ratings
    val aggregator = new Aggregator(sc)
    aggregator.init(ratings, movies)
    aggregator.getResult().take(10).foreach(println)

    // Build LSH index and get recommendations
    val seed  = (0 until 12).toIndexedSeq
    val index = new LSHIndex(movies, seed)
    val recommender = new Recommender(sc, index, ratings)

    val topK = recommender.recommendBaseline(userId = 1, genre = List("Action"), K = 5)
    topK.foreach { case (movieId, score) => println(s"Movie $movieId -> $score") }
  }
}
```

Submit to a running Spark cluster:

```bash
spark-submit \
  --class app.Main \
  --master local[*] \
  target/scala-3.3.1/Project_2-assembly-0.1.1.jar
```

---

## Components

### Loaders

#### `MoviesLoader`

```scala
class MoviesLoader(sc: SparkContext, path: String)
def load(): RDD[(Int, String, List[String])]
// Returns: (movieId, title, genres)
```

Reads the movie CSV from the classpath resource `path`, parses each pipe-delimited line, and persists the RDD in memory.

#### `RatingsLoader`

```scala
class RatingsLoader(sc: SparkContext, path: String)
def load(): RDD[(Int, Int, Option[Double], Double, Int)]
// Returns: (userId, movieId, previousRating, rating, timestamp)
```

Reads the ratings CSV from the classpath resource `path` and persists the RDD in memory. The `previousRating` field is always `None` on initial load; it is populated during incremental updates.

---

### Aggregator

```scala
class Aggregator(sc: SparkContext)

def init(ratings: RDD[...], title: RDD[...]): Unit
def getResult(): RDD[(String, Double)]
def getKeywordQueryResult(keywords: List[String]): Double
def updateResult(delta: Array[...]): Unit
```

- **`init`** – Joins ratings with movie titles and computes the average rating for each movie. Movies with no ratings receive an average of `0.0`. The state is persisted on disk and memory for fault tolerance.
- **`getResult`** – Returns `(title, averageRating)` pairs for all movies.
- **`getKeywordQueryResult`** – Returns the mean average rating over all movies whose genre list contains **all** of the provided keywords. Returns `-1.0` if no matching movies exist, or `0.0` if matching movies exist but none have been rated.
- **`updateResult`** – Incrementally updates the aggregated state given a batch of new or revised ratings, avoiding a full recomputation.

---

### Recommender

```scala
class Recommender(sc: SparkContext, index: LSHIndex, ratings: RDD[...])

def recommendBaseline(userId: Int, genre: List[String], K: Int): List[(Int, Double)]
def recommendCollaborative(userId: Int, genre: List[String], K: Int): List[(Int, Double)]
```

Both methods:
1. Use the **LSH nearest-neighbor lookup** to find candidate movies that share the same genre bucket as `genre`.
2. Filter out movies already watched by `userId`.
3. Predict a rating for each candidate using either the **baseline** or **ALS** predictor.
4. Return the top-`K` `(movieId, predictedRating)` pairs, sorted by descending predicted rating.

---

#### Baseline Predictor

```scala
class BaselinePredictor()

def init(ratingsRDD: RDD[...]): Unit
def predict(userId: Int, movieIds: RDD[Int]): RDD[(Int, Double)]
```

Implements the standard baseline prediction formula:

```
prediction(u, i) = avgUser(u) + avgDev(i) * scale(avgUser(u) + avgDev(i), avgUser(u))
```

where `scale(x, avg)` normalises deviations to `[1, 5]`, and `avgDev(i)` is the mean normalised deviation of all users' ratings for movie `i`.

---

#### Collaborative Filtering

```scala
class CollaborativeFiltering(rank: Int, regularizationParameter: Double, seed: Long, n_parallel: Int)

def init(ratingsRDD: RDD[...]): Unit
def predict(userId: Int, movieIds: RDD[Int]): RDD[(Int, Double)]
```

Wraps **Spark MLlib's ALS** (Alternating Least Squares) matrix factorisation:

| Parameter               | Value |
|-------------------------|-------|
| `rank`                  | 10    |
| `regularizationParameter` | 0.1 |
| `seed`                  | 0     |
| `n_parallel` (blocks)   | 4     |
| `maxIterations`         | 20    |

---

#### LSH Index & Nearest-Neighbor Lookup

```scala
class LSHIndex(data: RDD[(Int, String, List[String])], seed: IndexedSeq[Int])

def hash(input: RDD[List[String]]): RDD[(IndexedSeq[Int], List[String])]
def getBuckets(): RDD[(IndexedSeq[Int], List[(Int, String, List[String])])]
def lookup[T](queries: RDD[(IndexedSeq[Int], T)]): RDD[(IndexedSeq[Int], T, List[...])]
```

```scala
class NNLookup(lshIndex: LSHIndex)

def lookup(queries: RDD[List[String]]): RDD[(List[String], List[(Int, String, List[String])])]
```

```scala
class MinHash(seed: IndexedSeq[Int])

def hash(data: List[String]): IndexedSeq[Int]
```

The LSH pipeline:
1. **`MinHash.hash`** computes a MinHash signature (one minimum hash per seed) for a list of genre keywords.
2. **`LSHIndex`** groups movies into buckets by their MinHash signature.
3. **`NNLookup`** hashes a query genre list, then retrieves all movies that land in the same bucket — these are approximate nearest neighbors in genre space.
