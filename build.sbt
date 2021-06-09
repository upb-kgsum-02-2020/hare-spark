name := "hare-spark"

version := "0.1"

scalaVersion := "2.12.10"

idePackagePrefix := Some("io.kohpai")

val sansaVersion = "0.7.2-SNAPSHOT"
val sparkVersion = "3.1.2"

// | Resolvers
resolvers ++= Seq(
  "AKSW Maven Snapshots" at "https://maven.aksw.org/repository/snapshots"
)

// | SANSA Layers
libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % sparkVersion % "provided",
  "org.apache.spark" %% "spark-sql" % sparkVersion % "provided",
  "net.sansa-stack" %% "sansa-rdf-spark" % sansaVersion,
)

ThisBuild / assemblyMergeStrategy := {
  case PathList("META-INF", _*) => MergeStrategy.discard
  case _ => MergeStrategy.first
}
