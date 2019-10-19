import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import datapreparation._
import modelgraph.modelgraph

object Main {
  /**
   * @author eren özel
   */

  def main(args:Array[String]):Unit={
    datapreparation()
    val model = new modelgraph();
    model.modelGraph()
  }


  def datapreparation():Unit ={
    val spark = SparkSession
      .builder
      .appName("datagraph")
      .config(new SparkConf())
      .getOrCreate()

    val sc = spark.sparkContext
    sc.setLogLevel("WARN")
    import spark.implicits._

    val edgeVertexIntrabank = getRawDataIntrabankMoneyTransfer(spark).map(transformEdgeIntrabank).persist
    val edgeVertexKasoutgoing = getRawDataKasoutgoing(spark).map(transformEdgeKasoutgoing).persist
    /*..
    *
    * */

    def transformEdge(r:(Vertexdata,Vertexdata,Edgedetail)):Edgedata = {
      Edgedata(r._1.vertexid,r._2.vertexid,r._3.weight,r._3.resource,r._3.resourceid)
    }

    val edges = edgeVertexIntrabank.map(transformEdge)
      .union(edgeVertexKasoutgoing.map(transformEdge)).filter(r=>r.sourceid !=0 && r.targetid!=0)

    def transformVertex(r:(Vertexdata,Vertexdata,Edgedetail)):List[Vertexdata]={
      List(r._1,r._2)
    }

    val vertexs = edgeVertexIntrabank.flatMap(transformVertex)
      .union(edgeVertexKasoutgoing.flatMap(transformVertex)).filter(_.vertexid!=0)

    saveEdgeData(spark,edges)
    saveVertexData(spark,vertexs)
    spark.stop()

  }


}
