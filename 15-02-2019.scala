package com.telenor.spark

import org.apache.spark.sql.SparkSession
import java.io._
import scala.collection.mutable.ListBuffer
import org.apache.spark.sql.functions._
import org.apache.spark.sql._
import scala.io.Source._
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.fs.Path
import java.text.SimpleDateFormat
import org.apache.spark.sql._
import org.apache.spark.sql.types._
import java.text.SimpleDateFormat
import java.text.SimpleDateFormat
import scala.reflect.api.materializeTypeTag
import java.io.File
import org.apache.spark.sql.{Row, SaveMode, SparkSession}
import scala.collection.mutable.StringBuilder



class DuplicateCheckClass (spark : SparkSession) extends  Serializable
{
	import spark.implicits._
	import spark.sql
	spark.sqlContext.setConf("hive.exec.dynamic.partition", "true")
	spark.sqlContext.setConf("hive.exec.dynamic.partition.mode", "nonstrict")

	val hadoopfs: FileSystem = FileSystem.get(spark.sparkContext.hadoopConfiguration)
	var collDupRcdsBuf = new ListBuffer [String] ()
	val sep =","
	val key = "0"
	val partitionKey  =List(s"3:yyyyMMdd|4:hhmmss")
	val defaultDir =  "hdfs://quickstart.cloudera:8020/user/cloudera/"


	def addingUniqueKey(path :String): DataFrame = 	{
			val srcPath = path
					println("inside the addinguniquekey") 

					println("inside the isDirExist ifstatement") 
					val keySplit=key.split(",")
					var splitBuffer=""
					for(i<-keySplit) {  
						splitBuffer = splitBuffer + "nvl(_c" +i+ ",''),"
					}
			println("splitBuffer" + splitBuffer) 
			import spark.implicits._

			val conCatedNewCol= "concat("+ splitBuffer.substring(0,splitBuffer.length()-1) +") as joiningKeyColumn"
			println(conCatedNewCol+"Concated column")
			val dfRead  = spark.read.format("csv").option("sep",sep).option("inferSchema", "true").option("mode", "DROPMALFORMED").load(srcPath )

			dfRead.selectExpr(conCatedNewCol,"*")

	}


	def partitionColumnSplitter( partColList :List[String] ) : Array[(String, String)] =
		{

				val parttioncolRdd= spark.sparkContext.parallelize(partColList)
						val colSplit = parttioncolRdd.flatMap(m=>m.split(",")).map(l=>l.split(":")).collect
						val colsplitWrtLen=colSplit.map(m=>(m(0),m(1)))

						colsplitWrtLen
		}


	def addingPartitionKey(srcDf:DataFrame ): DataFrame={

			val partitionColsplit= partitionColumnSplitter(List(s"1:yyyyMMdd,2:hhmmss"))	

					var partSplitBuffer=""
					for(i<-partitionColsplit)
					{  
						partSplitBuffer = partSplitBuffer + "nvl(_c" +i._1+ ",'')," 

					}
			val conCatedCol= "concat("+ partSplitBuffer.substring(0,partSplitBuffer.length()-1) +") as PartKeyColumn"
					srcDf.selectExpr(conCatedCol,"*")
	}


	def HivePartitionKey(srcDf:DataFrame ): DataFrame={

			val partitionColsplit= partitionColumnSplitter(List(s"1:yyyyMMdd,2:hhmmss"))	

					var partSplitBuffer=""
					for(i<-partitionColsplit)
					{  
						partSplitBuffer = partSplitBuffer + "nvl(c" +i._1+ ",'')," 

					}
			val conCatedCol= "concat("+ partSplitBuffer.substring(0,partSplitBuffer.length()-1) +") as PartKeyColumn"
					srcDf.selectExpr(conCatedCol,"*")
	}


	def createTablestmt(dateAndTimeArray : Array[(String, String)],columnNameArray:Array[String],timeCol:String) = {
				
					var collDupRcdsBuf =""

					val removingTimeCol=  columnNameArray.filter(f=>f != "_c"+timeCol)

					for (j<- removingTimeCol)
					{
						collDupRcdsBuf +=  j.substring(1).toString() + " STRING ,".toString() 
					}

			val removingComma = collDupRcdsBuf.substring(0,collDupRcdsBuf.length()-1)

					for (i<- dateAndTimeArray)
					{
						val queryString =   "create table IF NOT EXISTS "+ "`"+i._1 +"`"+ " ( "  + removingComma   + " ) PARTITIONED BY ( `c"  + timeCol  +  "="+ i._1 +"` String)  ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' STORED AS Textfile"

						spark.sql(queryString) 

					}

	}
	def callingActualTable(hiveTable:DataFrame,uniqDate :String ,dateCol:String ) :DataFrame =
		{

				val keySplit=key.split(",")   //common for the class
						var splitBuffer=""
						for(i<-keySplit) 
						{  
							splitBuffer = splitBuffer + "nvl(c" +i+ ",''),"
						}

				import spark.implicits._

				val conCatedNewCol= "concat("+ splitBuffer.substring(0,splitBuffer.length()-1) +") as joiningKeyColumn"
				val hivTableWithUniqueKey= hiveTable.selectExpr(conCatedNewCol,"*")


				//  val hiveTblWithUniqPartKey = HivePartitionKey(hivTableWithUniqueKey)
				// val hiveTblUniqPartKeyfiltPrtKey=

				val uniqueKeyWithDateFilter=  hivTableWithUniqueKey.where(hivTableWithUniqueKey("c"+dateCol)===uniqDate)


				uniqueKeyWithDateFilter
		}


	def processing (addingUniqkey:DataFrame,addingPartKey :DataFrame ,  partitionColsplit : Array[(String, String)] , sourceTableName :String) =

		{


						val dateCol= partitionColsplit(0)._1     //3
						val timeCol= partitionColsplit(1)._1        //4
							addingUniqkey.createOrReplaceTempView("tblWithUniqueKey")  
						val uniquePartitionList =  addingPartKey.select(addingPartKey("PartKeyColumn")).distinct
						val partitionList=  uniquePartitionList.rdd.map(m=>m.mkString).collect()
						val actualFileTable = addingUniqkey.drop(addingUniqkey.col("joiningKeyColumn"))

						val dfSelectingUniqueDateCol=actualFileTable.select("_c"+dateCol).distinct()
					  val  IterationFrmFile  = dfSelectingUniqueDateCol.rdd.collect().map(m=>m.mkString)

						for (dateFilter <- IterationFrmFile)

						{  
									val parsingString=" Select * from "+ sourceTableName  +  " where c" + dateCol + " = " + dateFilter
								
									val tblPartition = spark.sql(parsingString) 

									val dfAddingUniqueAndPartKey= callingActualTable( tblPartition,dateFilter,dateCol) 

									val partitionColsplit= partitionColumnSplitter(List(s"1:yyyyMMdd,2:hhmmss"))

									dfAddingUniqueAndPartKey.createOrReplaceTempView("tblActualTable")

									val unmatchedRecords=  addingUniqkey.join(dfAddingUniqueAndPartKey, addingUniqkey("joiningKeyColumn") ===   dfAddingUniqueAndPartKey("joiningKeyColumn"), "left_outer")
									.where(dfAddingUniqueAndPartKey.col("joiningKeyColumn").isNull).filter("_c"+ dateCol + "=" + dateFilter).select(addingUniqkey.col("*"))

									val dfallColumn =   unmatchedRecords.withColumn("dateColumn",unmatchedRecords("_c"+dateCol )).withColumn("timeColumn",unmatchedRecords("_c" + timeCol))

									val dfAfterDroping =   dfallColumn.drop( ("_c" + dateCol ), ("_c" + timeCol),	 ("PartKeyColumn"),("joiningKeyColumn") )    

									val dfactualRenamedUnmatchedRecords=  dfAfterDroping.withColumnRenamed("dateColumn","_c" + dateCol).withColumnRenamed("timeColumn","_c" + timeCol)  

									val unMatchedArrayTuple= dfactualRenamedUnmatchedRecords.select ("_c" + dateCol,"_c" + timeCol).rdd.collect.map(m=>(m(0).toString,m(1).toString))

									dfactualRenamedUnmatchedRecords.createOrReplaceTempView("source_table") 


									import spark.implicits._
									import spark.sql


									val columnNameArray= dfactualRenamedUnmatchedRecords.columns.toArray.map(m=>m)

									val queryInsertString=   "insert into table parttable1 partition(c" + dateCol+ "," +"c" + timeCol+ ") select * from source_table"   

									println("queryInsertString"+queryInsertString)
									sql(queryInsertString)
									
									
                  }
						
				
						
		val dfDictinctOfDateInSourc=	sql("select distinct c"+ dateCol +" from "+sourceTableName)
						
			//		val intersectionOfDf= IterationFrmFile.intersect(dfDictinctOfDateInSourc)
					
						
				//		res17.filter(!(res17.col("_c4").isin(res22: _*))).show
						
val unmatchedRecFrm=dfSelectingUniqueDateCol.except(dfDictinctOfDateInSourc)

println("unmatchedRecFrm.show"+unmatchedRecFrm)

unmatchedRecFrm.show()
									
val unmatchedDtInSourceTable=	actualFileTable.join(unmatchedRecFrm,unmatchedRecFrm.col("_c"+ dateCol) === actualFileTable.col("_c"+ dateCol)).select(actualFileTable.col("*"))
	
unmatchedDtInSourceTable.createOrReplaceTempView("unmatchedDatesInSourceFile")

val insertingUnmatchedDateFile=   "insert into table parttable1 partition(c" + dateCol+ "," +"c" + timeCol+ ") select * from unmatchedDatesInSourceFile"   
	
sql(insertingUnmatchedDateFile)

	
updateLogMySQL.updateLogTbl(logId,srcCnt,processedCnt,jobStatus,errorDesc,errorReason) 

(args,srcCnt,processedCnt,jobStatus,errorDesc,errorReason)
		}    
}
object DuplicateCheck {

      var updateLogMySQL = new UpdateLogMySQL()
      
      

	def main(args: Array[String]): Unit = {

			val warehouseDir ="/user/hive/wareouse"
					val spark = SparkSession
					.builder()
					.appName("Spark Hive Example")
					.config("spark.sql.warehouse.dir", warehouseDir).config("hive.exec.dynamic.partition", "true").config("hive.exec.dynamic.partition.mode", "nonstrict").enableHiveSupport()
					.getOrCreate()

					val sc = new DuplicateCheckClass(spark)
			
					val sourceSystem ="Incoming_CDR"
					val sourceFeed = "MSS_CDR"
					val hiveTableDirectory= "hdfs://quickstart.cloudera:8020/user/cloudera/dir/retailer"

					val addingUniqkey = sc.addingUniqueKey( "/user/ramesh/fetchingFile.txt" )

					println("showing unique partition")
					//	addingUniqkey.show()

					val partitionColsplit= sc.partitionColumnSplitter(List(s"1:yyyyMMdd,2:hhmmss"))
					val addingPartKey = sc.addingPartitionKey(addingUniqkey)
					println("showing addingPartKey")
					//addingPartKey.show()

					sc.processing (addingUniqkey,addingPartKey  , partitionColsplit  , "sourcetbl")

	}

}

