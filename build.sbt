name := "datagraph"

version := "0.1"

scalaVersion := "2.11.12"

resolvers +="cloudera" at "https://repository.cloudera.com/artifactory/cloudera-repos"

libraryDependencies ++={
  val sparkVersion = "2.3.0.cloudera3"
  val sparkOrg = "org.apache.spark"
  Seq(
    sparkOrg %% "spark-sql" % sparkVersion % "provided",
    "org.scalatest" %% "scalatest" % "3.0.5" % "test"
  )
}