//package com.telenor.spark

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
import java.time._
import java.time.format._
import java.time.LocalDate
import scala.collection.mutable.ArrayBuffer




class DuplicateCheckClass (spark : SparkSession) extends  Serializable
{
       import spark.implicits._
       import spark.sql
       spark.sqlContext.setConf("hive.exec.dynamic.partition", "true")
       spark.sqlContext.setConf("hive.exec.dynamic.partition.mode", "nonstrict")

  	  val hadoopfs: FileSystem = FileSystem.get(spark.sparkContext.hadoopConfiguration)
  	  var collDupRcdsBuf = new ListBuffer [String] ()
			val sep =","
			val key = "0|1"
			val partitionKey  =List(s"3:yyyyMMdd|4:hhmmss")
			val defaultDir =  "hdfs://quickstart.cloudera:8020/user/cloudera/"
			val columnArray =Array[String]()
			
			
			def last30Days() : Array[String]= {
					    var arrdate = ArrayBuffer[String]()
							val formatter = DateTimeFormatter.ofPattern("yyyyMMdd")
							val currDate=java.time.LocalDate.now.toString()replaceAll("-", "")
							val runDay = LocalDate.parse(currDate, formatter)
							println(currDate)
							var i = 0
							while (i <30)
							{
								arrdate+=  runDay.minusDays(i).format(formatter).toString()
										i=i+1
							}
					arrdate.toArray

        }
       

			
	    def addingUniqueKey(path :String): DataFrame = 	{
	    	    	val srcPath = path
	    				println("inside the addinguniquekey") 
	    		
	    					println("inside the isDirExist ifstatement") 
	    					val keySplit=key.split("[|]")
	    					var splitBuffer=""
	    					for(i<-keySplit) {  
	    						splitBuffer = splitBuffer + "nvl(_c" +i+ ",''),"
	    					}
	    					println("splitBuffer" + splitBuffer) 
	    					import spark.implicits._

	    					val conCatedNewCol= "concat("+ splitBuffer.substring(0,splitBuffer.length()-1) +") as joiningKeyColumn"
	    					println(conCatedNewCol+"Concated column")
	    					
	    			
	    					  	val dfRead  = spark.read.format("csv").option("sep",sep).option("inferSchema", "true").option("mode", "DROPMALFORMED").load(srcPath )
	    					  
	    					//val columnArray=dfRead.columns

	    					
	    					dfRead.selectExpr(conCatedNewCol,"*")
	    
	    }
	    
	    
	    
	    
	    def addingUniqueKeyForTbl(tblDf : DataFrame): DataFrame=
	    
	        {
	    				println("inside the addinguniquekey") 
	    		
	    					println("inside the isDirExist ifstatement") 
	    					val keySplit=key.split("[|]")
	    					var splitBuffer=""
	    					for(i<-keySplit) {  
	    						splitBuffer = splitBuffer + "nvl(_c" +i+ ",''),"
	    					}
	    					println("splitBuffer" + splitBuffer) 
	    					import spark.implicits._

	    					val conCatedNewCol= "concat("+ splitBuffer.substring(0,splitBuffer.length()-1) +") as joiningKeyColumn"
	    					println(conCatedNewCol+"Concated column")
	            
	    					tblDf.selectExpr(conCatedNewCol,"*")
	    
}
  	  
       
       
  	  
  	  def partitionColumnSplitter( partColList :List[String] ) : Array[(String, String)] =
  	  {
  	      	    
  	         	val parttioncolRdd= spark.sparkContext.parallelize(partColList)
	    				val colSplit = parttioncolRdd.flatMap(m=>m.split("[|]")).map(l=>l.split(":")).collect
	    				val colsplitWrtLen=colSplit.map(m=>(m(0),m(1)))
  	    
  	    colsplitWrtLen
  	  }
  	  
  	  
  	  
  	  
  	  

	    def addingPartitionKey(srcDf:DataFrame): DataFrame={

	         		val parttioncolRdd= spark.sparkContext.parallelize(partitionKey)
	    				val colSplit = parttioncolRdd.flatMap(m=>m.split("\\|")).map(l=>l.split(":")).collect
	    				val colsplitWrtLen=colSplit.map(m=>(m(0),m(1)))
	    				var partSplitBuffer=""

	    				for(i<-colsplitWrtLen)
	    				{  
	    					partSplitBuffer = partSplitBuffer + "nvl(_c" +i._1+ ",'')," 

	    				}
	        	println("partSplitBuffer"+partSplitBuffer) 	
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
	      
	    
	    
	    
	    
	    
	    
	    
	    


 def createTablestmt(partDateCol:String,columnNameArray:Array[String],timeCol:String) = {
   
      	     
      	       var collDupRcdsBuf =""
      	       
          val removingTimeCol=  columnNameArray.filter(f=>f != "_c"+timeCol)
           
   	    	  for (j<- removingTimeCol)
   	    	     	    {
   	    	     	         collDupRcdsBuf +=  j.substring(1).toString() + " STRING ,".toString() 
    	    	     	  }
      	 
           val removingComma = collDupRcdsBuf.substring(0,collDupRcdsBuf.length()-1)
         
           
           
           val queryString =   "create table IF NOT EXISTS "+ partDateCol + " ( "  + removingComma   + " ) PARTITIONED BY ( c"  + timeCol  +  " String)  ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' STORED AS Textfile"
          
          // println("printing the query string=>  " +queryString)
           spark.sql(queryString) 
           
           
   	    	     	 
 }
 
 

 

 def callingActualTable(hiveTable:DataFrame,uniqDate :String ,dateCol:String ) :DataFrame =
 {
   
                val keySplit=key.split(",")   //common for the class
	    					var splitBuffer=""
	    					for(i<-keySplit) 
	    					{  
	    						splitBuffer = splitBuffer + "nvl(c" +i+ ",''),"
	    					}
	    					println("splitBuffer" + splitBuffer) 
	    					import spark.implicits._

	    					val conCatedNewCol= "concat("+ splitBuffer.substring(0,splitBuffer.length()-1) +") as joiningKeyColumn"
	    					val hivTableWithUniqueKey= hiveTable.selectExpr(conCatedNewCol,"*")

	    					
       //  val hiveTblWithUniqPartKey = HivePartitionKey(hivTableWithUniqueKey)
        // val hiveTblUniqPartKeyfiltPrtKey=
          
        val uniqueKeyWithDateFilter=  hivTableWithUniqueKey.where(hivTableWithUniqueKey("c"+dateCol)===uniqDate)
       

	    					
	    					
        
        
        uniqueKeyWithDateFilter
 }
 
 

 

    			
			def processing (addingUniqkey:DataFrame,addingPartKey :DataFrame , sourceTableName :String) =
				{
			          val partition= 	partitionColumnSplitter(partitionKey)
			          
			          val dateCol= partition(0)._1     //3
					    	val timeCol= partition(1)._1   
			          
					    	  println("dateCol"+dateCol)
			           println("timeCol"+timeCol)
					    	val dfAfterDroping =  addingUniqkey.drop("joiningKeyColumn")    

								val origlSrcCols = dfAfterDroping.columns

								val   df = spark.sql("Select * from "+ sourceTableName )
								
								val sourceTblCols= df.columns
								
								val firstPartitionColName = sourceTblCols(dateCol.toInt)
								val secondPartitionColName = sourceTblCols(timeCol.toInt)
								

								val sourceTablWithCols=  df.toDF(origlSrcCols:_*)
								
								val srcTblWithUniqKey = addingUniqueKeyForTbl(sourceTablWithCols)    //sourcehivetbl
								
								val last30DaysArr = last30Days
								
								last30DaysArr.foreach(println)
								
							val srcFileFilt30Days=	addingUniqkey.filter( col("_c"+dateCol).isin(last30DaysArr:_*))			//sourcehivetbl		
							
										
							//spark.sql("select filteredFile.* from filteredFile left join hivetbl on(filteredFile.joiningKeyColumn= hivetbl.joiningKeyColumn) where hivetbl.joiningKeyColumn is null").show()
							
					
							val unmatchedRecords=  srcFileFilt30Days.join(srcTblWithUniqKey, srcFileFilt30Days("joiningKeyColumn") ===   srcTblWithUniqKey("joiningKeyColumn"), "left_outer")
									.where(srcTblWithUniqKey.col("joiningKeyColumn").isNull).select(srcFileFilt30Days.col("*"))
							
									
						    	val dfunmatchedrecds=		unmatchedRecords.drop("joiningKeyColumn")
							
									val dfallColumn =   dfunmatchedrecds.withColumn("dateColumn",dfunmatchedrecds("_c"+dateCol )).withColumn("timeColumn",dfunmatchedrecds("_c" + timeCol))
									
				        	val dfAfterCleanUp =   dfallColumn.drop( ("_c" + dateCol ), ("_c" + timeCol) ) 
									
									
									dfAfterCleanUp.createOrReplaceTempView("unmatchedRecords")
									
									
					spark.sql("insert into table "+  sourceTableName +" Partition("+ firstPartitionColName +"," +""+secondPartitionColName +" ) select * from unmatchedRecords")
								

				}    



      

}
        
        




        
        
        

  object DuplicateCheck {
    
      
	     def main(args: Array[String]): Unit = {
	       
	          val warehouseDir ="/user/hive/warehouse"
	          val spark = SparkSession
                .builder()
                .appName("Spark Hive Example")
                .config("spark.sql.warehouse.dir", warehouseDir).config("hive.exec.dynamic.partition", "true").config("hive.exec.dynamic.partition.mode", "nonstrict")
                .getOrCreate()
                
       

       		val sc = new DuplicateCheckClass(spark)
					val sourceSystem ="Incoming_CDR"
					val sourceFeed = "MSS_CDR"
        	val addingUniqkey = sc.addingUniqueKey( "hdfs://quickstart.cloudera:8020/user/cloudera/fetcher.txt" )
				    
					println("showing unique partition")
				//	addingUniqkey.show()
				   
			//	  val partitionColsplit= sc.partitionColumnSplitter(List(s"1:yyyyMMdd,2:hhmmss"))
				  
				  			   
					val addingPartKey = sc.addingPartitionKey(addingUniqkey)
							println("showing addingPartKey")
				//	addingPartKey.show()

          sc.processing (addingUniqkey,addingPartKey   , "tbl")


					
	}
      
  }
  
