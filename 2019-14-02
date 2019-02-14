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
	      
	    
	    
	    
	    
	    
	    
	    
	    


 def createTablestmt(partDateCol:String,columnNameArray:Array[String],timeCol:String) = {
   
      	     
      	       var collDupRcdsBuf =""
      	       
          val removingTimeCol=  columnNameArray.filter(f=>f != "_c"+timeCol)
           
   	    	  for (j<- removingTimeCol)
   	    	     	    {
   	    	     	         collDupRcdsBuf +=  j.substring(1).toString() + " STRING ,".toString() 
    	    	     	  }
      	 
           val removingComma = collDupRcdsBuf.substring(0,collDupRcdsBuf.length()-1)
         
           
           
           val queryString =   "create table IF NOT EXISTS "+ partDateCol + " ( "  + removingComma   + " ) PARTITIONED BY ( c"  + timeCol  +  " String)  ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' STORED AS Textfile"
          
           println("printing the query string=>  " +queryString)
           spark.sql(queryString) 
           
           
   	    	     	 
 }
 
 

 

 def callingActualTable(hiveTable:DataFrame,partitionedFilter :String ) :DataFrame =
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

	    					
        val hiveTblWithUniqPartKey = HivePartitionKey(hivTableWithUniqueKey)
        val hiveTblUniqPartKeyfiltPrtKey= hiveTblWithUniqPartKey.where(hiveTblWithUniqPartKey("PartKeyColumn")===partitionedFilter)
       
        hiveTblUniqPartKeyfiltPrtKey
 }
 
 

 

    			
  def processing (addingUniqkey:DataFrame,addingPartKey :DataFrame ,  partitionColsplit : Array[(String, String)] , sourceTableName :String) =
  {
   
      
         val dateCol= partitionColsplit(0)._1     //3
   	    
   	     val timeCol= partitionColsplit(1)._1        //4
   	    

          
        addingUniqkey.createOrReplaceTempView("tblWithUniqueKey")   //registered the file table    --> 1st method of the file 
           println("showing the tblWithUniqueKey")
           
           spark.sql("select * from tblWithUniqueKey").show
     
   	       val uniquePartitionList =  addingPartKey.select(addingPartKey("PartKeyColumn")).distinct
   	       
   	       println("printing the uniquePartitionList")
          //  uniquePartitionList.show()

   	       val partitionList=  uniquePartitionList.rdd.map(m=>m.mkString).collect()
   	   
   	       println("showing the distinct partitionList  ")
   	       
   	         //  partitionList.foreach(println)
   	       
   	       val actualFileTable = addingUniqkey.drop(addingUniqkey.col("joiningKeyColumn"))
	       	     
   	       val columnNameArray= actualFileTable.columns.toArray.map(m=>m)
   	       
     
   	 	       
    
   	       
   	       
          
          
	  for (partifilter <- partitionList)
	
	        {  //20160211254585
	    
	      println("printing the partition filter = "+partifilter)

      println("entering into the for loop")
	       
   	    	                      	   
   	    //taking the data from the actual table 
   	  
	       val parsingString=" Select * from "+ sourceTableName  +  " where c" + dateCol + " = " + partifilter.substring(0,8)
	       
	       println("after filtering the source table=>"+  parsingString)
   	 
	       val tblPartition = spark.sql(parsingString) //20160211
	      
	       println("after parsing the source table")
	       
	       	val dfAddingUniqueAndPartKey= callingActualTable( tblPartition,partifilter.substring(0,8) )  //unique key with partition filter and 
	       	
	       	  println("after callingActualTable")
	       	
	       	val partitionColsplit= partitionColumnSplitter(List(s"1:yyyyMMdd,2:hhmmss"))
	       	
	       	 dfAddingUniqueAndPartKey.createOrReplaceTempView("tblActualTable")
	       	
	       	  println("printing source table")
	       	 dfAddingUniqueAndPartKey.show
	       	 	  println("dataframe leftouter")
	       	 dfAddingUniqueAndPartKey.show
	       	 
	       	 	       	  println("dataframe leftouter")

	       	 //
	   //  val lol=    addingUniqkey.join(dfAddingUniqueAndPartKey, addingUniqkey("joiningKeyColumn") ===   dfAddingUniqueAndPartKey("joiningKeyColumn"), "left_outer").where(dfAddingUniqueAndPartKey.col("joiningKeyColumn").isNull).select(addingUniqkey.col("*"))
	        
	      val lol=  dfAddingUniqueAndPartKey.join(addingUniqkey, dfAddingUniqueAndPartKey("joiningKeyColumn") ===   addingUniqkey("joiningKeyColumn"), "left_outer").where(addingUniqkey.col("joiningKeyColumn").isNull).where(dfAddingUniqueAndPartKey("PartKeyColumn") === partifilter).select(dfAddingUniqueAndPartKey.col("*"))
	   	       	   println("showing lol")
  
	   	       	   
	   	  val lol1=  dfAddingUniqueAndPartKey.join(addingUniqkey, dfAddingUniqueAndPartKey("joiningKeyColumn") ===   addingUniqkey("joiningKeyColumn"))
	   	       	   
	   	     	  println("inner join")  
	   	       lol1.show
	   	  println("showing lol")       	   
	   	       	   
	     lol.show
	         //
	       println("after createOrReplaceTempView tblActualTable")
	       	
	      println("printingtblActualTable")
	      
	      sql("select * from tblActualTable").show

	   /*   
	      val unmatched_parser = " SELECT tblWithUniqueKey.* FROM  tblWithUniqueKey  LEFT JOIN "+  
	    " tblActualTable ON (tblActualTable.joiningKeyColumn =  tblWithUniqueKey.joiningKeyColumn) " + 
	    "WHERE tblActualTable.PartKeyColumn = "+ partifilter +" AND tblActualTable.joiningKeyColumn  IS NULL"
	     
   	val unmatchedRecords = spark.sqlContext.sql(unmatched_parser )
   	* 
   	*/
   	
  
   println("done" ) 
	   
	        }
	  
     	/*     
         
    val dfallColumn =   unmatchedRecords.withColumn("dateColumn",unmatchedRecords("_c"+dateCol )).withColumn("timeColumn",unmatchedRecords("_c" + timeCol))

    val dfAfterDroping =   dfallColumn.drop( ("_c" + dateCol ), ("_c" + timeCol),	 ("PartKeyColumn"),("joiningKeyColumn") )       
                   
   
   dfAfterDroping.createOrReplaceTempView("source_table") 
   
   
   
    sql("Insert into table"+  sourceTableName +" partition( c"+ dateCol +",c"+ timeCol +" ) select * from source_table")   

		
	           

    createTablestmt(partifilter.substring(8),columnNameArray, timeCol)   
   
	         
    
    
    sql("Insert into table c"+ dateCol  +" partition( c"+ timeCol +" ) select * from source_table")   
    
    

    

    
    
    
    
    
    
        
	         
	   if (tblPartition.rdd.isEmpty())
   	    	     	       
	   {
	     

	  val dfWithPartKey  =  addingPartKey.where(addingPartKey("PartKeyColumn") === partifilter )
	     	
	     	
	  val dfallColumn =   dfWithPartKey.withColumn("dateColumn",dfWithPartKey("_c"+dateCol )).withColumn("dfWithPartKey",dfWithPartKey("_c" + timeCol))
	     	
	     	 
    val dfAfterDroping =   dfallColumn.drop( ("_c" + dateCol ), ("_c" + timeCol),	 ("PartKeyColumn"),("joiningKeyColumn") )

	   
    dfAfterDroping.registerTempTable("tblWithUniqueKey")   
	   
	   
    sql("Insert into table"+  sourceTableName +" partition( c"+ dateCol +",c"+ timeCol +" ) select * from tblWithUniqueKey")
	    
    
	     
	   }
		   
	     
	       
	       

	       
	       
	       
       createTablestmt(partifilter.substring(8),columnNameArray, timeCol)    
	       
	     
	     
	        	    
   	   val unmatchedRecords = spark.sql(" select tblWithUniqueKey.* from  tblWithUniqueKey  left join "+   " tblActualTable on (tblActualTable.joiningKeyColumn =  tblWithUniqueKey.joiningKeyColumn) " +  "where tblWithUniqueKey.PartKeyColumn = "+ filter +" and tblActualTable.joiningKeyColumn  is null" )
              
 
       acutualStrct.drop
       acutualStrct.registerTempTable("insert_data")
       spark.sql( "INSERT INTO TABLE "+ table_name + " PARTITION( "+ column_name + "=" +partDateCol.substring(1,8) +  " ) select * from insert_data" )
       
       INSERT INTO TABLE t2 PARTITION(country) SELECT * from T1;
          	    
	   
	         
	         
	     
	     
	     
	     
	     
	     
	     
	     
	     
	     
	     
	     
	   
	        }
	     
	     
      
   	           val dfWithPartKey=  addingPartKey.where(addingPartKey("PartKeyColumn")===partitionedFilter)
   	           
   	           
   	           
   	              	           
   	           val nonInsertedRecordsDropUniKey=  dfWithPartKey.drop(unmatchedRecords.col("joiningKeyColumn")) 
   	           
               val acutualStrct =  dfWithPartKey.drop(unmatchedRecords.col("PartKeyColumn")) 
   	           
   	           acutualStrct.registerTempTable("tblWithUniqueKey") 
   	           
   	           
 
               val addingPartCol = df.withColumn("partCol", "_c" + timeCol )
               
               val acutualStrct =  addingPartCol.drop(addingPartCol.col("_c" + timeCol)) 
	       	          
               acutualStrct.registerTempTable("insert_data1")
               spark.sql( "INSERT INTO TABLE "+ partifilter.substring(1,8) + " PARTITION( _c" + timeCol + " ) select * from insert_data1")
	          	     	          
   }
	   

	   {
	   
       val sourceTbl = callingActualTable(tblPartition,filter)      
   	    
   	   sourceTbl.registerTempTable("tblActualTable") //registered hivepartfilteredtable    --> tblActualTable
   	    
   	    
   	   val unmatchedRecords = spark.sql(" select tblWithUniqueKey.* from  tblWithUniqueKey  left join "+   " tblActualTable on (tblActualTable.joiningKeyColumn =  tblWithUniqueKey.joiningKeyColumn) " +  "where tblWithUniqueKey.PartKeyColumn = "+ filter +" and tblActualTable.joiningKeyColumn  is null" )
              
       val nonInsertedRecordsDropUniKey=  unmatchedRecords.drop(unmatchedRecords.col("joiningKeyColumn")) 
       val acutualStrct =  nonInsertedRecordsDropUniKey.drop(unmatchedRecords.col("PartKeyColumn")) 
       
       acutualStrct.drop
       acutualStrct.registerTempTable("insert_data")
       spark.sql( "INSERT INTO TABLE "+ table_name + " PARTITION( "+ column_name + "=" +partDateCol.substring(1,8) +  " ) select * from insert_data" )
       
       INSERT INTO TABLE t2 PARTITION(country) SELECT * from T1;
          	    
   	  // val outer_join = a.join(b, df1("id") === df2("id"), "left_outer") 
   	    	                        
 
	*/  
       
     
         
      
  }    



      

}
        
        
          
        
        
        

  object DuplicateCheck {
    
      
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
				  
					val addingUniqkey = sc.addingUniqueKey( "/user/ramesh/fetchingFile.txt" )
				    
					println("showing unique partition")
					//addingUniqkey.show()
				   
				  val partitionColsplit= sc.partitionColumnSplitter(List(s"1:yyyyMMdd,2:hhmmss"))
				  
				  			   
					val addingPartKey = sc.addingPartitionKey(addingUniqkey)
							println("showing addingPartKey")
					//addingPartKey.show()

            sc.processing (addingUniqkey,addingPartKey  , partitionColsplit  , "sourcetbl")

					
	}
      
  }
  
