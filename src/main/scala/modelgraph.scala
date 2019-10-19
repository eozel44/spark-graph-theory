package modelgraph

import java.util
import java.util.function

import com.redislabs.provider.redis.{RedisConfig, RedisEndpoint, RedisNode}
import org.apache.spark.{SparkConf, graphx}
import org.apache.spark.graphx.{Edge, Graph}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Dataset, SparkSession}
import redis.clients.jedis.{Jedis, Pipeline}
import scala.concurrent.duration._

case class EdgeData(sourceid:Long,targetid:Long,weight:Double)
class modelgraph {

  val config = Map(
    "redis.post" -> "localhost",
    "redis.port" -> "6379",
    "redis.db"-> "1"
  )
  def modelGraph():Unit ={
    val spark = SparkSession
      .builder
      .appName("modelgraph")
      .config(new SparkConf()
        .set("redis.post",config("redis.host"))
        .set("redis.port",config("redis.port"))
        .set("redis.db",config("redis.db")))
      .getOrCreate()
    import spark.implicits._

    val query = s"select sourceid, targetid, weight from edge"
    val edges = spark.sql(query).as[EdgeData].map{r=>Edge(r.sourceid,r.targetid,r.weight)}.rdd
    val vertex = edges.flatMap(r=>List(r.srcId,r.dstId)).distinct().map((_,("isim","isim")))
    val graph = Graph(vertex,edges)

    val cc = graph.connectedComponents.vertices
    val rank = graph.pageRank(0.0001).vertices
    val hub = graph.connectedComponents.triangleCount.vertices.filter(r=>r._2>0)

    cc.map(r=>(r._1,r._2)).toDF().createGlobalTempView("cc")
    spark.sql(s"insert overwrite table cc from select * from cc")

    rank.map(r=>(r._1,r._2)).toDF().createGlobalTempView("rank")
    spark.sql(s"insert overwrite table rank from select * from rank")

    hub.map(r=>(r._1,r._2)).toDF().createGlobalTempView("hub")
    spark.sql(s"insert overwrite table hub from select * from hub")


    val redisConfig = new RedisConfig(new RedisEndpoint(spark.sparkContext.getConf))
    val connComponents = cc.groupBy(_._2).mapValues(r=>r.map(_._1).toList)
    connComponents.foreachPartition{r=>
      val connectionMap = new util.HashMap[RedisNode,(Jedis,Pipeline)]()
      r.foreach{
        case(componentid,list) =>
          val result = (componentid,componentid) :: list.map(v=>(v,componentid))
          result.foreach{
            case (key,value) => val host = redisConfig.getHost(key.toString)
              val (_,pipeline:Pipeline) = connectionMap.computeIfAbsent(host,new function.Function[RedisNode,(Jedis,Pipeline)] {
                def apply(t:RedisNode):(Jedis,Pipeline)={
                  val jedis = t.connect
                  val pipeline = jedis.pipelined()
                  (jedis,pipeline)
                }
              })
              pipeline.set(key.toString,value.toString)
          }
      }
      import scala.collection.JavaConverters._
      connectionMap.asScala.foreach{case(_,(connection,_)) => connection.close}
    }
    spark.close()
  }
}
