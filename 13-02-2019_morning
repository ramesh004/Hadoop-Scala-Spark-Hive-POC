package demopack

import org.apache.spark.sql.SparkSession
import java.io._
import scala.collection.mutable.ListBuffer
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql._
import scala.io.Source._
import org.apache.spark.sql.SparkSession
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.fs.Path
import java.text.SimpleDateFormat
import org.apache.spark.sql._
import org.apache.spark.sql.types._
import java.text.SimpleDateFormat
import java.text.SimpleDateFormat
import scala.reflect.api.materializeTypeTag
import java.io.IOException;
import java.util.List;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;


class DuplicateCheckClass (spark : SparkSession) extends  Serializable
{
	    var collDupRcdsBuf = new ListBuffer [String] ()
			val sep ="|"
			val key = "4|5|6|7|8|9|10|11|14|17|18|19|24"
			val partitionKey  =List(s"3:yyyyMMdd|4:hhmmss")
			val defaultDir =  "hdfs://quickstart.cloudera:8020/user/cloudera/"
			
			def dateFormat(actualDateFrmt:String,datefun:String):String={
					val e = new SimpleDateFormat(datefun)
					val d= e.parse(actualDateFrmt); 
					val formatDay = new SimpleDateFormat("dd");
					val formatMonth = new SimpleDateFormat("MM");
					val formatYear = new SimpleDateFormat("YYYY");

					val currentDay =   formatDay.format(d);
					val currentMonth = formatMonth.format(d);
					val currentYear = formatYear.format(d);
					val formatedDate = currentYear.toString() + currentMonth.toString() +  currentDay.toString()
					formatedDate
	    }

	    spark.sqlContext.udf.register("dateFun", dateFormat(_:String,_:String) :String )
	    
	    	  
	    
	    
	    
	    
	    
	    def readingTheExistingFile(sourceSystem: String,sourceFeed : String): DataFrame = 	{
	      println("reading the file")
	    		val srcPath = sourceSystem + "/" + sourceFeed
	    				println("inside the addinguniquekey") 
	    				if (testfileExist(srcPath))
	    				{
	    					println("inside readingTheExistingFile") 
	    					val keySplit=key.split("[|]")
	    					var dummyBuffer=""
	    					for(i<-keySplit) {  
	    						dummyBuffer = dummyBuffer + "nvl(_c" +i+ ",''),"
	    					}
	    					println("splitBuffer" + dummyBuffer) 
	    					import spark.implicits._

	    					val conCatedNewCol= "concat("+ dummyBuffer.substring(0,dummyBuffer.length()-1) +") as joiningKeyColumn"
	    					println(conCatedNewCol+"Concated column")
	    					val dfRead  = spark.read.format("csv").option("sep",sep).option("inferSchema", "true").option("mode", "DROPMALFORMED").load(srcPath)
	    					dfRead.selectExpr(conCatedNewCol,"*")
	    				}
	    				else
	    					throw new Exception(srcPath + "<= HDFS file not found")
	    }
	    
	    
	    
	    
	    

	    //private
	    def addingUniqueKey(sourceSystem: String,sourceFeed : String): DataFrame = 	{
	    		val srcPath = sourceSystem + "/" + sourceFeed
	    				println("inside the addinguniquekey") 
	    				if (isDirExist(srcPath))
	    				{
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
	    				else
	    					throw new Exception(srcPath + "<= HDFS Directory not found")
	    }

	    def addingPartitionKey(srcDf:DataFrame): DataFrame={

	    		val s:List[String] = List(s"3:yyyyMMdd|4:hhmmss") 
	    				val parttioncolRdd= spark.sparkContext.parallelize(s)
	    				val colSplit = parttioncolRdd.flatMap(m=>m.split("[|]")).map(l=>l.split(":")).collect
	    				val colsplitWrtLen=colSplit.map(m=>(m(0),m(1)))
	    				var partSplitBuffer=""
	    				for(i<-colsplitWrtLen)
	    				{  
	    					partSplitBuffer = partSplitBuffer + "nvl(_c" +i._1+ ",'')," 

	    				}
	    val conCatedCol= "concat("+ partSplitBuffer.substring(0,partSplitBuffer.length()-1) +") as PartKeyColumn"
	    		srcDf.selectExpr(conCatedCol,"*")
	    }

	    def dfToList(joinKeyPartKeyAppend:DataFrame) :List[String]= {
	    		val columnNameArray= joinKeyPartKeyAppend.columns.toArray.map(m=>m.substring(1))
	    				val noOfColumnsInDf= columnNameArray.size   //25
	    				val makeRdd = joinKeyPartKeyAppend.rdd
	    				val linearry: Array[String]  = makeRdd.map(_.mkString(",")).collect
	    				linearry.toList
	    }

	    val hadoopfs: FileSystem = FileSystem.get(spark.sparkContext.hadoopConfiguration)

	    def writing(filePath: String, data: Array[String]) = {
	        val path = new Path(filePath)
	    		val os = hadoopfs.create(path)
	    		for (i<-data) {
	    			os.DelimitedTextFileWriter(hadoopfs,filePath, CodecInfo codec, byte[] fieldDelimiter)
	   			}
	   	}

	    def mkdirs(folderPath: String): Unit = {
	    		val path = new Path(folderPath)
          println("directory_creating_path"+ folderPath)
   				if (!hadoopfs.exists(path))	{
	    				hadoopfs.mkdirs(path)
	    				println("directory_creating")
	    		}
	    }

	    def isDirExist(path: String): Boolean = {
	    		val p12 = new Path(path)
	    		hadoopfs.exists(p12) && hadoopfs.getFileStatus(p12).isDirectory
	    }


	    def testfileExist(path: String): Boolean = {
	    		val p = new Path(path)
	    		hadoopfs.exists(p) && hadoopfs.getFileStatus(p).isFile()
   		}

	    		def listingFiles(path:String) :List[String]= {

	    				var listdirbuff = new ListBuffer [String] ()
	    						var listfilebuff = new ListBuffer [String] ()

	    						if (isDirExist(path)){
	    							val fs = FileSystem.get(new Configuration())
	    									val status = fs.listStatus(new Path(path))

	    									for (i <- status)
	    									{
	    										if ( i.isFile())
	    										{
	    											listfilebuff+= i.getPath.getName //listing the status of the file  
	    										}
	    									} 
	    						}
	    				listfilebuff.toList
	    		}

	    		def listOfHivedir(path:String) :List[String] =	{
	    					var listdirbuff = new ListBuffer [String] ()
	    							println("path=>ramesh"+path)
	    							if (isDirExist(path))
	    							{
	    								val fs = FileSystem.get(new Configuration())
	    										val status = fs.listStatus(new Path(path))

	    										for (i <- status)
	    										{
	    											if ( i.isDirectory())
	    											{
	    												listdirbuff+= i.getPath.getName //listing the status of the file  

	    											}
	    										} 
	    							}
	    							else
	    							{
	    								throw new Exception(path + "<= The given hive directory doesn't exist")

	    							}
	    					listdirbuff.toList
	    			}

	    		def processing (addingUniqkey:DataFrame, sourceFileList :List[String],hiveTabledir :String,sourceSystem :String,sourceFeed:String)
	    		{

	    			for (i <- sourceFileList) {
	    				val sourcePartition = i.split(",")(0).trim
	    						val navigatingPath = hiveTabledir + "/" + sourcePartition   
	    						val mainDir = i.split(",")(0)
	    						val subDir = navigatingPath +"/" + i.split(",")(0).substring(0,8)
	    						val fileName= "Incoming_"+sourceSystem+"_"+sourceFeed+"_merge_"+ mainDir +".dat"
	    						val filepath = subDir +"/" + fileName 
	    						val sourceUniqueKey = i.split(",")(1).trim

	    						println("before parent check")
	    						if ( isDirExist(navigatingPath))	{
	    						                println("inside if 1")
	    						        	if(isDirExist(subDir))
	    					            		{
	    						        	   println("inside if 2")
	    							
	    						        	  if (testfileExist(filepath))
	    						
	    						        	  {
	    						        	    println("inside if 3")
	    									 
	    						        	    val existingDataInPatition = readingTheExistingFile(subDir,fileName)  //wrongOne
	    									
	    						        	    val recordOccurenceInTheFile= existingDataInPatition.selectExpr("joiningKeyColumn").filter(existingDataInPatition("joiningKeyColumn") === sourceUniqueKey)
	    										
	    						        	    if(recordOccurenceInTheFile.rdd.isEmpty())	{
	    						        	      println("inside if 4")
	    											
	    						        	      val existingDF = addingUniqkey.where(addingUniqkey("joiningKeyColumn") === sourceUniqueKey)
	    														val dropingColumnDF = existingDF.drop(existingDF.col("joiningKeyColumn"))
	    														
	    														val removingUniqueKeyInPartData = existingDataInPatition.drop(existingDataInPatition.col("joiningKeyColumn"))
	    														
	    														val mergedData= removingUniqueKeyInPartData.unionAll(dropingColumnDF)
	    														val arr= mergedData.rdd.collect.map(m=>m.toString)
	    														hadoopfs.delete(new Path(subDir+"/"+fileName),true)
	    														println("file Deleted Successfully")
	    														writing( filepath, arr )
	    														println("after writing") 

	    											}
	    								}
	    								else
	    								{
	    								      println("inside else1")
	    								  	  val existingDF = addingUniqkey.where(addingUniqkey("joiningKeyColumn") === sourceUniqueKey)
	    											val dropingColumnDF=   existingDF.drop(existingDF.col("joiningKeyColumn"))
	    											val arr= dropingColumnDF.rdd.collect.map(m=>m.toString)
	    											writing( filepath, arr )
	    											println("after writing") 

	    								}
	    							}
	    							else
	    							{
	    							  println("inside else2")
	    								mkdirs(subDir)
	    								val existingDF = addingUniqkey.where(addingUniqkey("joiningKeyColumn") === sourceUniqueKey)
	    								val dropingColumnDF=   existingDF.drop(existingDF.col("joiningKeyColumn"))
	    								val arr= dropingColumnDF.rdd.collect.map(m=>m.toString)
	    								writing( filepath, arr )
	    								println("after writing")  
	    							}
	    						}
	    						else
	    						{
	    						  println("inside else3")
	    							println("parent_directory_creation_path_ram" + navigatingPath)  
	    							mkdirs(navigatingPath)
	    							if (isDirExist(navigatingPath))
	    							{
	    								mkdirs(subDir)
	    								val existingDF = addingUniqkey.where(addingUniqkey("joiningKeyColumn") === sourceUniqueKey)
	    								val dropingColumnDF=   existingDF.drop(existingDF.col("joiningKeyColumn"))
	    								val arr= dropingColumnDF.rdd.collect.map(m=>m.toString)
	    								writing( filepath, arr )
	    							}    
	    							else
	    							{
	    							  println("inside else5")
	    								throw new Exception(" Parent Directory not created successfully in this path" + navigatingPath)
	    							}
	    						}
	    			}
	    		}
}

  object demo {
	  def main(args: Array[String]): Unit = {
			val spark = SparkSession.builder()
					.master("yarn-client")
					.appName("hivedir")
					.getOrCreate()
					val sc = new DuplicateCheckClass(spark)
					val sourceSystem ="Incoming_CDR"
					val sourceFeed = "MSS_CDR"
				  val hiveTableDirectory= "hdfs://quickstart.cloudera:8020/user/cloudera/dir/retailer"
					val addingUniqkey = sc.addingUniqueKey( sc.defaultDir+sourceSystem, sourceFeed)
					addingUniqkey.show()
					val addingPartKey = sc.addingPartitionKey(addingUniqkey)
					//addingPartKey.show()
					val WithUniqPartList= sc.dfToList(addingPartKey)
					println("hivedirfiles")
					sc.processing(addingUniqkey,WithUniqPartList,hiveTableDirectory,sourceSystem,sourceFeed)
	}
}
