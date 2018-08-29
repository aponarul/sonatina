name := "ubi-journeyData-validation"

version := "1.1"

//scalaVersion := "2.10.5"
scalaVersion := "2.11.8"




libraryDependencies ++= {
  val sparkVer = "2.3.0"
  //val sparkVer = "1.6.3"
  Seq(
    "org.apache.spark" %% "spark-core" % sparkVer,
    "org.apache.spark" %% "spark-sql" % sparkVer,
    "org.apache.spark" %% "spark-hive" % sparkVer,
    "org.elasticsearch" %% "elasticsearch-spark-20" % "5.6.9",
    //"org.elasticsearch" %% "elasticsearch-spark-13" % "5.2.0",
    "org.apache.calcite" % "calcite-core" % "1.2.0-incubating",
    "org.pentaho" % "pentaho-aggdesigner" % "5.1.5-jhyde" pomOnly(),
    "org.pentaho" % "pentaho-aggdesigner-algorithm" % "5.1.5-jhyde" % Test,
    "org.scalatest" %% "scalatest" % "3.0.5" % "test",
    "com.typesafe" % "config" % "1.3.2"

  )
}


resolvers += Resolver.mavenLocal
resolvers += "Cascading repo" at "http://conjars.org/repo"
resolvers += "Artima Maven Repository" at "http://repo.artima.com/releases"


assemblyMergeStrategy in assembly := {
  case PathList("META-INF", xs @ _*) => MergeStrategy.discard
  case x => MergeStrategy.first
}



