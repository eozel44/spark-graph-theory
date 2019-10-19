import org.apache.spark.sql.{SparkSession,Dataset}

  case class Intrabankdata(senderaccountnumber:Int,receiveraccountnumber:Int, receivermoneytransfertype:Int,sendermoneytransfertype:Int,
                           receivertaxnumber:String,receiverphone:String,receivername:String,totalamount:String,
                           sendertaxnumber:String,senderphone:String,sendername:String,moneytransferid:Int,isactive:Int)
  case class Kasoutgoingdata(paymentaccountnumber:Int, state:Int, receiveraccountnumber:String,
                             receiveridentitynumber:String,receivername:String,amount:String,
                             sendername:String, outgoindid:Int, senderaccountnumber:String,senderidentitynumber:String)
  case class Kasincomingdata(paymentaccountnumber:Int,state:Int,senderaccountnumber:String,
                             senderidentitynumber:String,sendername:String,amount:String,receivername:String,incomingid:Int,receiveraccountnumber:String,
                             receiveridenditynumber:String)
  case class Outgoinggolddata(senderaccountnumber:Int,status:Int,receiveraccountnumber:String,
                              receivertaxnumber:String,receivername:String,amount:String,sendername:String,outgoinggoldid:Int)
  case class Incominggolddata(paymentaccountnumber:Int,status:Int,senderaccount:String,sendercustomerid:String,
                              sendertaxnumber:String,sendercustomername:String,amount:String,receivercustomername:String,incominggoldid:Int)
  case class Moneygramdata(producttype:String,receiverid:Int,receiveraccountnumber:Int,receiverfirstname:String,receiverlastname:String,
                           senderid:Int,senderaccountnumber:Int,senderfirstname:String,senderlastname:String,
                           transactiondate:String,amount:String,sendcurrency:String,trandate:String,effectivebid:String,feccode:String,transactionid:Int)
  case class Chequedata(chequeid:Int, accountnumber:Int,draweraccountno:String,drawetaxnumber:String,
                        drawername:String,fec:Int,amount:String,effectivebid:String)

  case class Edgedetail(weight:Double,resource:String,resourceid:Int)
  case class Edgedata(sourceid:Long,targetid:Long,weight:Double,resource:String,resourceid:Int)
  case class Vertexdata(vertexid:Long,vertexhasaccount:Int,vertexaccountnumber:Long,vertexname:String)

  object datapreparation{

    def getRawDataIntrabankMoneyTransfer(spark:SparkSession):Dataset[Intrabankdata] ={
      import spark.implicits._
      val query = s""
      spark.sql(query)
        .na.fill(0,Seq("senderaccountnumber")
      ).as[Intrabankdata]
    }

    def getRawDataKasoutgoing(spark:SparkSession):Dataset[Kasoutgoingdata] ={
      import spark.implicits._
      val query = s""
      spark.sql(query)
        .na.fill(0,Seq("state")
      ).as[Kasoutgoingdata]
    }

    def getRawDataKasincoming(spark:SparkSession):Dataset[Kasincomingdata] ={
      import spark.implicits._
      val query = s""
      spark.sql(query)
        .na.fill(0,Seq("state")
      ).as[Kasincomingdata]
    }

    def getRawDataOutgoinggold(spark:SparkSession):Dataset[Outgoinggolddata] ={
      import spark.implicits._
      val query = s""
      spark.sql(query)
        .na.fill(0,Seq("status")
      ).as[Outgoinggolddata]
    }

    def getRawDataIncominggold(spark:SparkSession):Dataset[Incominggolddata] ={
      import spark.implicits._
      val query = s""
      spark.sql(query)
        .na.fill(0,Seq("status")
      ).as[Incominggolddata]
    }

    def getRawDataCheque(spark:SparkSession):Dataset[Chequedata] ={
      import spark.implicits._
      val query = s""
      spark.sql(query)
        .na.fill(0,Seq("accountnumber")
      ).as[Chequedata]
    }

    def getRawDataMoneygram(spark:SparkSession):Dataset[Moneygramdata] ={
      import spark.implicits._
      val query = s""
      spark.sql(query)
        .na.fill(0,Seq("receiverid")
      ).as[Moneygramdata]
    }

    /*transforms*/
    implicit class myString(val str:String) extends AnyVal{
      def convertToLong():Long = try{
        str.toLong
      }catch{
        case e:Exception =>0L
      }
      def convertToDouble():Double=try{
        str.toDouble
      }catch{
        case e:Exception =>0.0
      }
      def hashConversation():Long={
        if(str!=null && !str.isEmpty){
          val seed=0xf7ca7fd2
          val a = scala.util.hashing.MurmurHash3.stringHash(str,seed)
          val b = scala.util.hashing.MurmurHash3.stringHash(str.reverse.toString,seed)
          val c:Long = a.toLong << 32 | (b & 0xffffffffL)
          c
        }else
          0L
      }
    }
    implicit  class myInt(val i:Int) extends AnyVal{
      def convertToLong():Long=try{
        i.toLong
      }catch {
        case e:Exception => 0
      }
    }

    implicit  class myLong(val i:Long) extends AnyVal{

      def hashConversation():Long={
        val str = i.toString
        if(str!=null && !str.isEmpty){
          val seed=0xf7ca7fd2
          val a = scala.util.hashing.MurmurHash3.stringHash(str,seed)
          val b = scala.util.hashing.MurmurHash3.stringHash(str.reverse.toString,seed)
          val c:Long = a.toLong << 32 | (b & 0xffffffffL)
          c
        }else
          0L
      }
    }

    val hasAccount = 1
    val hasNotAccount = 0
    val defaultAccountNumber = 0L
    val emptyVertexData = Vertexdata(0L,hasNotAccount,defaultAccountNumber,null)

    sealed trait Resource
    case object Intrabank extends Resource
    case object Kasoutgoing extends Resource
    case object Kasincoming extends Resource
    case object Outgoinggold extends Resource
    case object Incominggold extends Resource
    case object Cheque extends Resource
    case object Moneygram extends Resource

    def getUniqueId(id:Long,resource: Resource):Long={
      val chc = id.hashConversation
      if(chc != 0)
        (resource.toString + id.toString).hashConversation
      else
        chc
    }

    def getUniqueId(id:String,resource: Resource):Long={
      val chc = id.hashConversation
      if(chc != 0)
        (resource.toString + id).hashConversation
      else
        chc
    }

    def transformEdgeIntrabank(r:Intrabankdata):(Vertexdata,Vertexdata,Edgedetail)={
      val vertexSource =(r.receiveraccountnumber,r.receivertaxnumber.convertToLong,r.receiverphone.convertToLong,r.receivername)match{
        case(a,_,_,_) if a != 0 => Vertexdata(getUniqueId(a,Intrabank),hasAccount,defaultAccountNumber,r.receivername)
        case(_,a,_,_) if a != 0 => Vertexdata(getUniqueId(a,Intrabank),hasNotAccount,defaultAccountNumber,r.receivername)
        case(_,0,a,_) if a != 0 => Vertexdata(getUniqueId(a,Intrabank),hasNotAccount,defaultAccountNumber,r.receivername)
        case(_,0,0,a) if a != 0 => Vertexdata(getUniqueId(a,Intrabank),hasNotAccount,defaultAccountNumber,r.receivername)
        case _ => emptyVertexData
      }
      val vertexTarget =(r.senderaccountnumber,r.sendertaxnumber.convertToLong,r.senderphone.convertToLong,r.sendername)match{
        case(a,_,_,_) if a != 0 => Vertexdata(getUniqueId(a,Intrabank),hasAccount,defaultAccountNumber,r.sendername)
        case(_,a,_,_) if a != 0 => Vertexdata(getUniqueId(a,Intrabank),hasNotAccount,defaultAccountNumber,r.sendername)
        case(_,0,a,_) if a != 0 => Vertexdata(getUniqueId(a,Intrabank),hasNotAccount,defaultAccountNumber,r.sendername)
        case(_,0,0,a) if a != 0 => Vertexdata(getUniqueId(a,Intrabank),hasNotAccount,defaultAccountNumber,r.sendername)
        case _ => emptyVertexData
      }
      val sourceTarget =(r.sendermoneytransfertype,r.receivermoneytransfertype) match{
        case(1,_)=>(vertexTarget,vertexSource)
        case(_,1)=>(vertexTarget,vertexSource)
        case _=>(emptyVertexData,emptyVertexData)
      }
      val edgedetail =Edgedetail(r.totalamount.convertToDouble,Intrabank.toString,r.moneytransferid)
      (sourceTarget._1,sourceTarget._2,edgedetail)
    }

    def transformEdgeKasoutgoing(r:Kasoutgoingdata):(Vertexdata,Vertexdata,Edgedetail)={

      val sourceVertex =(r.state,r.paymentaccountnumber.convertToLong,r.senderaccountnumber.convertToLong,r.senderidentitynumber.convertToLong,r.sendername)match{
        case(2,a,_,_,_) if a != 0 => Vertexdata(getUniqueId(a,Intrabank),hasAccount,r.paymentaccountnumber.convertToLong,r.sendername)
        case(2,0,a,_,_) if a != 0 => Vertexdata(getUniqueId(a,Intrabank),hasNotAccount,defaultAccountNumber,r.sendername)
        case(2,0,0,a,_) if a != 0 => Vertexdata(getUniqueId(a,Intrabank),hasNotAccount,defaultAccountNumber,r.sendername)
        case(2,0,0,0,a) if a != 0 => Vertexdata(getUniqueId(a,Intrabank),hasNotAccount,defaultAccountNumber,r.sendername)
        case _ => emptyVertexData
      }

      val targetVertex =(r.state,r.paymentaccountnumber.convertToLong,r.receiveraccountnumber.convertToLong,r.receiveridentitynumber.convertToLong,r.receivername)match{
        case(2,a,_,_,_) if a != 0 => Vertexdata(getUniqueId(a,Intrabank),hasAccount,r.paymentaccountnumber.convertToLong,r.receivername)
        case(2,0,a,_,_) if a != 0 => Vertexdata(getUniqueId(a,Intrabank),hasNotAccount,defaultAccountNumber,r.receivername)
        case(2,0,0,a,_) if a != 0 => Vertexdata(getUniqueId(a,Intrabank),hasNotAccount,defaultAccountNumber,r.receivername)
        case(2,0,0,0,a) if a != 0 => Vertexdata(getUniqueId(a,Intrabank),hasNotAccount,defaultAccountNumber,r.receivername)
        case _ => emptyVertexData
      }

      val edgedetail =Edgedetail(r.amount.convertToDouble,Kasoutgoing.toString,r.outgoindid)
      (sourceVertex,targetVertex,edgedetail)
    }

    def transformEdgeKasincoming(r:Kasincomingdata):(Vertexdata,Vertexdata,Edgedetail)={
      /**/
      (emptyVertexData,emptyVertexData,Edgedetail(r.amount.convertToDouble,Kasincoming.toString,r.incomingid))

    }

    def transformEdgeOutgoinggold(r:Outgoinggolddata):(Vertexdata,Vertexdata,Edgedetail)={
      /**/
      (emptyVertexData,emptyVertexData,Edgedetail(r.amount.convertToDouble,Outgoinggold.toString,r.outgoinggoldid))
    }

    def transformEdgeIncominggold(r:Incominggolddata):(Vertexdata,Vertexdata,Edgedetail)={
      /**/
      (emptyVertexData,emptyVertexData,Edgedetail(r.amount.convertToDouble,Incominggold.toString,r.incominggoldid))
    }

    def transformEdgeCheque(r:Chequedata):(Vertexdata,Vertexdata,Edgedetail)={
      /**/
      (emptyVertexData,emptyVertexData,Edgedetail(r.amount.convertToDouble,Cheque.toString,r.chequeid))
    }

    def transformEdgeMoneygram(r:Moneygramdata):(Vertexdata,Vertexdata,Edgedetail)={
      /**/
      (emptyVertexData,emptyVertexData,Edgedetail(r.amount.convertToDouble,Moneygram.toString,r.transactionid))
    }

    /*target*/

    def saveEdgeData(spark:SparkSession,data:Dataset[Edgedata]):Unit={
      data.createOrReplaceTempView("edge")
      spark.sql(s"insert overwrite table boatransactions.graphedge select * from edge")
      println("edge Size "+ data.count)
    }

    def saveVertexData(spark:SparkSession,data:Dataset[Vertexdata]):Unit={
      data.createOrReplaceTempView("vertex")
      spark.sql(s"insert overwrite table boatransactions.graphvertex select a.vertexid,a.vertexthasaccount,a.vertexaccountnumber,a.vertexname " +
        s"from vertex(select vertexid, vertexhasaccount,vertexaccountnumber,vertexname,count(1)" +
        s"from vertex group by vertexid, vertexhasaccount,vertexaccountnumber,vertexname) a")

    }
  }

