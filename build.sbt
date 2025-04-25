val sparkVersion = "3.5.1"
ThisBuild / scalaVersion := "3.3.1"
ThisBuild / organization := "ch.epfl"
ThisBuild / scalaVersion := "3.3.1"
ThisBuild / version := "0.1.1"

lazy val root = project.in(file("."))
  .settings(
    name := "Project_2",
    initialize := {
      val _ = initialize.value
      val required = "21"
      val current = sys.props("java.specification.version")
      assert(current == required, s"Unsupported JDK. Using $current but requires $required")
    },
    assembly / assemblyMergeStrategy := {
      case PathList("META-INF", xs@_*) => MergeStrategy.discard
      case x => MergeStrategy.first
    },
    assembly / test := {},
    assembly / assemblyExcludedJars := {
      val cp = (assembly / fullClasspath).value
      cp filter { f =>
        f.data.getName.contains("junit") ||
          f.data.getName.contains("test") ||
          f.data.getName.contains("scalactic") ||
          f.data.getName.contains("hadoop") ||
          f.data.getName.contains("spark")
      }
    },
    libraryDependencies ++= Seq(
      ("org.apache.spark" %% "spark-core" % sparkVersion % "provided").cross(CrossVersion.for3Use2_13),
      ("org.apache.spark" %% "spark-mllib" % sparkVersion % "provided").cross(CrossVersion.for3Use2_13),
      "org.scala-lang" % "scala3-library_3" % "3.3.1",

      "org.slf4j" % "slf4j-api" % "1.7.13",
      "org.slf4j" % "slf4j-log4j12" % "1.7.13",

      "org.scalactic" %% "scalactic" % "3.2.17",

      "org.junit.jupiter" % "junit-jupiter-api" % "5.3.1" % Test,
      "org.junit.jupiter" % "junit-jupiter-params" % "5.3.1" % Test,
      // junit tests (invoke with `sbt test`)
      "com.novocode" % "junit-interface" % "0.11" % "test"
    ),
    Test / testOptions += Tests.Argument("-q")
  )
