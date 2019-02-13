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

  	  val hadoopfs: FileSystem = FileSystem.get(spark.sparkContext.hadoopConfiguration)
  	  var collDupRcdsBuf = new ListBuffer [String] ()
			val sep ="|"
			val key = "4|5|6|7|8|9|10|11|14|17|18|19|24"
			val partitionKey  =List(s"3:yyyyMMdd|4:hhmmss")
			val defaultDir =  "hdfs://quickstart.cloudera:8020/user/cloudera/"

			
	    def addingUniqueKey(sourceSystem: String,sourceFeed : String): DataFrame = 	{
	    		val srcPath = sourceSystem + "/" + sourceFeed
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
	    					val dfRead  = spark.read.format("csv").option("sep",sep).option("inferSchema", "true").option("mode", "DROPMALFORMED").load(srcPath + "/*.dat" )
	    					dfRead.selectExpr(conCatedNewCol,"*")
	    
	    }
  	  
  	  
  	  
  	  def partitionColumnSplitter( partColList :List[String] =  List(s"3:yyyyMMdd|4:hhmmss") ) : Array[(String, String)] =
  	  {
  	      	    	    	  
	    				val parttioncolRdd= spark.sparkContext.parallelize(partColList)
	    				val colSplit = parttioncolRdd.flatMap(m=>m.split("[|]")).map(l=>l.split(":")).collect
	    				val colsplitWrtLen=colSplit.map(m=>(m(0),m(1)))
  	    
  	    colsplitWrtLen
  	  }
  	  
  	  
  	  
  	  
  	  

	    def addingPartitionKey(srcDf:DataFrame,colsplitter: Array[(String, String)]): DataFrame={

	        				
	    				var partSplitBuffer=""
	    				for(i<-colsplitter)
	    				{  
	    					partSplitBuffer = partSplitBuffer + "nvl(_c" +i._1+ ",'')," 

	    				}
	    val conCatedCol= "concat("+ partSplitBuffer.substring(0,partSplitBuffer.length()-1) +") as PartKeyColumn"
	    		srcDf.selectExpr(conCatedCol,"*")
	    }
	    
	    
	    
    	
  
	    def isDirExist(path: String): Boolean = {
	    		val p12 = new Path(path)
	    		hadoopfs.exists(p12) && hadoopfs.getFileStatus(p12).isDirectory
	    }
	    
	    
	       def testfileExist(path: String): Boolean = {
	    		val p = new Path(path)
	    		hadoopfs.exists(p) && hadoopfs.getFileStatus(p).isFile()
   		}
	       
	       
	       
	       

	    def dfToList(joinKeyPartKeyAppend:DataFrame) :List[String]= {
	    		val columnNameArray= joinKeyPartKeyAppend.columns.toArray.map(m=>m.substring(1))
	    				val noOfColumnsInDf= columnNameArray.size   //25
	    				val makeRdd = joinKeyPartKeyAppend.rdd
	    				val linearry: Array[String]  = makeRdd.map(_.mkString(",")).collect
	    				linearry.toList
	    }
	    
	    
	    
 def createTablestmt(partDateCol:String,columnNameArray:Array[String],timeCol:String) = {
   
   	    	  for (j<- columnNameArray.length)
   	    	     	    {
   	    	     	        queryColStr = queryColStr +=  j + " STRING ," 
    	    	     	  }
           val removingComma = queryColStr.substring(0,queryColStr.length()-1)
           val queryString = "create table"+ partDateCol + "( "  + removingComma   + " ) PARTITIONED BY ( "  + timeCol  +  "String) STORED AS Textfile ;"
           spark.sql(queryString) 
   	    	     	   }
 
 
 def callingActualTable(hiveTable:DataFrame,partitionedFilter :String ) :DataFrame =
 {
   
                val keySplit=key.split("[|]")   //common for the class
	    					var splitBuffer=""
	    					for(i<-keySplit) 
	    					{  
	    						splitBuffer = splitBuffer + "nvl(_c" +i+ ",''),"
	    					}
	    					println("splitBuffer" + splitBuffer) 
	    					import spark.implicits._

	    					val conCatedNewCol= "concat("+ splitBuffer.substring(0,splitBuffer.length()-1) +") as joiningKeyColumn"
	    					val hivTableWithUniqueKey= hiveTable.selectExpr(conCatedNewCol,"*")

        val hiveTblWithUniqPartKey =addingPartitionKey(hivTableWithUniqueKey)
        val hiveTblUniqPartKeyfiltPrtKey= hiveTblWithUniqPartKey.where(dfWithPartitionedKey("PartKeyColumn")===partitionedFilter)
       
        hiveTblUniqPartKeyfiltPrtKey
 }
 
 
 
	    
    			
 def processing (addingUniqkey:DataFrame,addingPartKey :DataFrame ,sourceFileList :List[String],
     partitionColsplit : Array[(String, String)]  ,hiveTabledir :String,sourceSystem :String,sourceFeed:String,tableName :String
       )
 
    {
          
        addingUniqkey.registerTempTable("tblWithUniqueKey")   //registered the file table    --> tblWithUniqueKey
   
        val dateCol= partitionColsplit(0)._1 
   	    
   	    val timeCol= partitionColsplit(1)._1 

   	    val uniquePartitionList=  addingPartKey.select(addingPartKey("PartKeyColumn")).distinct

   	   val partitionList=  uniquePartitionList.rdd.map(m=>m.mkString).collect()
   	   
   	   var queryColStr = new StringBuilder
   	   
	for (partifilter <- partitionList)
	
	        {  //20160211254585
   	    	  
   	    	                      	   
   	    //taking the data from the actual table 
   	    	//callingActualTable(sourcesystem : String,sourcefeed: String,partitionedFilter :String ) 
   	 
	       val tblPartition = spark.sql(" Select * from "+ tableName  +  " where " + dateCol + " = " + partifilter.substring(1,8) ) //20160211
	      
	   if (tblPartition.rdd.isEmpty())
   	    	     	        {
	       	     	        
	       	     val actualFileTable = addingUniqkey.drop(addingUniqkey.col("joiningKeyColumn"))
   	           val columnNameArray= actualFileTable.columns.toArray.map(m=>m)
   	           
           	val unmatchedRecords = sql.( " select tblWithUniqueKey.* from  tblWithUniqueKey  left join "+
   	             " tblActualTable on (tblActualTable.joiningKeyColumn =  tblWithUniqueKey.joiningKeyColumn "+) 
                  "where tblWithUniqueKey.PartKeyColumn = "+ filter +" and tblActualTable.joiningKeyColumn  is null ")
        
                  createTablestmt(partifilter.substring(1,8),columnNameArray,timeCol)  
	       	          
	       	        sql.( "insert into table"+ table_name + "select * from insert_data")
	       	     	          
   	    	     	        }
	   
	   else
	   
	   {
	   
       val sourceTbl = callingActualTable(tblPartition,filter)      
   	    
   	   sourceTbl.registerTempTable("tblActualTable") //registered hivepartfilteredtable    --> tblActualTable
   	    
   	    
   	   val unmatchedRecords = sql.( " select tblWithUniqueKey.* from  tblWithUniqueKey  left join "+
   	       " tblActualTable on (tblActualTable.joiningKeyColumn =  tblWithUniqueKey.joiningKeyColumn "+) 
              "where tblWithUniqueKey.PartKeyColumn = "+ filter +" and tblActualTable.joiningKeyColumn  is null ")
              
       val nonInsertedRecordsDropUniKey=  unmatchedRecords.drop(unmatchedRecords.col("joiningKeyColumn")) 
       val acutualStrct =  nonInsertedRecordsDropUniKey.drop(unmatchedRecords.col("PartKeyColumn")) 
       
         acutualStrct.registerTempTable("insert_data")
         sql.( "INSERT INTO TABLE "+ table_name + " PARTITION( "+ column_name "=" partDateCol.substring(1,8) +    " ) select * from insert_data")
       
         INSERT INTO TABLE t2 PARTITION(country) SELECT * from T1;
          	    
   	  // val outer_join = a.join(b, df1("id") === df2("id"), "left_outer") 
   	    	                        
 
	    
      
  }    
      

}

  object DuplicateCheck {
    
  }      
	     def main(args: Array[String]): Unit = {
	       
	           val warehouseDir ="/user/hive/wareouse"
	          val spark = SparkSession
                .builder()
                .appName("Spark Hive Example")
                .config("spark.sql.warehouse.dir", warehouseDir)
                .enableHiveSupport()
                .getOrCreate()

       		val sc = new DuplicateCheckClass(spark)
					val sourceSystem ="Incoming_CDR"
					val sourceFeed = "MSS_CDR"
				  val hiveTableDirectory= "hdfs://quickstart.cloudera:8020/user/cloudera/dir/retailer"
				  
					val addingUniqkey = sc.addingUniqueKey( sc.defaultDir+sourceSystem, sourceFeed)
				   addingUniqkey.show()
				   
				  val partitionColsplit= 	partitionColumnSplitter(List(s"3:yyyyMMdd|4:hhmmss"))
				  
					val addingPartKey = sc.addingPartitionKey(addingUniqkey,partitionColsplit)
					//addingPartKey.show()
					val WithUniqPartList= sc.dfToList(addingPartKey)
					println("hivedirfiles")
					
					sc.processing(addingUniqkey,addingPartKey,WithUniqPartList,partitionColsplit,hiveTableDirectory,sourceSystem,sourceFeed)
	}
      
       
    
       // println("viewing the hive table ")
		 
            
		 
	  //  val tableName = "SELECT COUNT(*) FROM "+ sourceSystem + sourceFeed+ "where "+ partition_col_name = string )    //name==ramesh                        
    
   //  val lol = sql(tableName)
     
         

	}
}
