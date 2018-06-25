name := "ubi-journeyData-validation"

version := "1.0"

scalaVersion := "2.10.5"


libraryDependencies ++= {
  val sparkVer = "1.6.3"
  Seq(
    "org.apache.spark" %% "spark-core" % "1.6.3",
    "org.apache.spark" %% "spark-sql" % "1.6.3",
    "org.apache.spark" %% "spark-hive" % "1.6.3",
    "org.elasticsearch" %% "elasticsearch-spark-13" % "5.6.9",
    //"org.elasticsearch" %% "elasticsearch-spark-13" % "5.2.0",
    "org.spark-project.hive" % "hive-cli" % "1.2.1.spark",
    "org.spark-project.hive" % "hive-metastore" % "1.2.1.spark",
    "org.spark-project.hive" % "hive-exec" % "1.2.1.spark",
    "org.apache.calcite" % "calcite-core" % "1.2.0-incubating",
    "org.pentaho" % "pentaho-aggdesigner" % "5.1.5-jhyde" pomOnly(),
    "org.pentaho" % "pentaho-aggdesigner-algorithm" % "5.1.5-jhyde" % Test

  )
}

resolvers += Resolver.mavenLocal
resolvers += "Cascading repo" at "http://conjars.org/repo"


assemblyMergeStrategy in assembly := {
  case PathList("META-INF", xs @ _*) => MergeStrategy.discard
  case x => MergeStrategy.first
}



