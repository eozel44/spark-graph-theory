name := "datagraph"

version := "0.1"

scalaVersion := "2.11.12"

resolvers +="cloudera" at "https://repository.cloudera.com/artifactory/cloudera-repos"

libraryDependencies ++={
  val sparkVersion = "2.3.0.cloudera3"
  val sparkOrg = "org.apache.spark"
  Seq(
    sparkOrg %% "spark-sql" % sparkVersion % "provided",
    sparkOrg %% "spark-graphx" % sparkVersion % "provided",
    "org.scalatest" %% "scalatest" % "3.0.5" % "test",
      sparkOrg %% "spark-streaming" % sparkVersion % "provided",
  "com.redislabs" % "spark-redis" % "2.3.0",
  "org.scalatest" %% "scalatest" % "3.0.5" % "test"
  )
}