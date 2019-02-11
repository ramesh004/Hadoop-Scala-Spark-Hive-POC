package peck
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.Row
import scala.io.Source
import java.io._
import scala.collection.mutable.ListBuffer
import java.io.PrintWriter
import java.io.FileReader
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{DataTypes, StructField, StructType}
import org.apache.spark.sql._
import org.apache.spark.{SparkConf, SparkContext}
import java.io.File
import scala.io.Source._
import org.apache.spark.sql.SparkSession
import java.io.IOException
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.util.Tool;
import java.io.BufferedInputStream;
import java.io.FileInputStream;
import java.io.InputStream;
import java.io.OutputStream;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.util.ToolRunner;
import java.time.LocalDate
import java.time.format.DateTimeFormatter
import java.text.SimpleDateFormat
import java.util.Calendar
import org.apache.spark.sql.functions.udf
import org.apache.spark.sql._
import org.apache.spark.sql.types._
import java.time.LocalDate
import java.time.format.DateTimeFormatter
import java.text.SimpleDateFormat
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Calendar



  
  class cls (spark : SparkSession) extends  Serializable
  {
         var collDupRcdsBuf = new ListBuffer [String] ()
              val sep ="|"
              val key = "4|5|6|7|8|9|10|11|14|17|18|19|24"
              val partitionKey  =List(s"3:yyyyMMdd|4:hhmmss")
              //val defaultDir = "hdfs://quickstart.cloudera:8020/user/"
            val defaultDir =  "file:\\\\\\D:\\"
            
            
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
     
    
        def Write(path:String,list:Array[String]) = 
        {

          val conf = new Configuration()
          val fs= FileSystem.get(conf)
          val output = fs.create(new Path(path))  
          //"hdfs://quickstart.cloudera:8020/user/cloudera/sample.txt"
          val writer = new PrintWriter(output)
          list.foreach(m=> writer.write(m))
          }
         
         
     //Register the function as UDF
           // spark.udf.register("dateFun", dateFormat, "String")  
    
         
       spark.sqlContext.udf.register("dateFun", dateFormat(_:String,_:String) :String )
            
      //  val dateFun = udf(dateFormat(_:String,_:String):String)
            
            
            
    
     //private
     def addingUniqueKey(sourceSystem: String,sourceFeed : String /*., .sep :String...key :String */ ): DataFrame = 
     {
          //    "resources/lolram.dat"
            val srcPath = sourceSystem + "\\" + sourceFeed
           println("inside the addinguniquekey") 
         
            if (isDirExist(srcPath))
            {
                println("inside the isDirExist ifstatement") 
              
                  val keySplit=key.split("[|]")
                  
                  var splitBuffer=""
    
                  for(i<-keySplit)
                  {  
                    
                    splitBuffer = splitBuffer + "nvl(_c" +i+ ",'')," 
                    
                  }
                
                
                println("splitBuffer" + splitBuffer) 
                
                
                               
                 import spark.implicits._

                   val conCatedNewCol= "concat("+ splitBuffer.substring(0,splitBuffer.length()-1) +") as joiningKeyColumn"
    
          //  try {
                   println(conCatedNewCol+"Concated column")
                   
                           
             val dfRead  = spark.read.format("csv").option("sep",sep).option("inferSchema", "true").option("mode", "DROPMALFORMED").load(srcPath + "//*.dat" )
                   
                    

                   //}catch {  case ex: IOException  => println("File is Missing")   }
           
                   
               

              dfRead.selectExpr(conCatedNewCol,"*")
              
            }
            
            
            else
              
              throw new Exception(srcPath + "<= HDFS Directory not found")
    
     
     
 }
     
     
     def addingPartitionKey(srcDf:DataFrame /*, partitionKey:List[String] */): DataFrame={
       
       
       val s:List[String] = List(s"3:yyyyMMdd|4:hhmmss")    //List(3:yyyyMMdd|4:hhmmss) 
       
       val parttioncolRdd= spark.sparkContext.parallelize(s)  //
       
       val colSplit = parttioncolRdd.flatMap(m=>m.split("[|]")).map(l=>l.split(":")).collect   //org.apache.spark.rdd.RDD[String] 
       
       val colsplitWrtLen=colSplit.map(m=>(m(0),m(1)))
       
               var partSplitBuffer=""
               
                 for(i<-colsplitWrtLen)
                  {  
                    partSplitBuffer = partSplitBuffer + "nvl(_c" +i._1+ ",'')," 
                    
                  }
       
          val conCatedCol= "concat("+ partSplitBuffer.substring(0,partSplitBuffer.length()-1) +") as PartKeyColumn"
       
         srcDf.selectExpr(conCatedCol,"*")
         
         
    /*   
  
      partSplitBuffer = " dateFun(_c" + colsplitWrtLen(0)._1 +   ",'" + colsplitWrtLen(0)._2 +"')," + "nvl(_c" +colsplitWrtLen(1)._1+ ",''),"
      
      //concated=>  concat( dateFun(_c3,'yyyyMMdd'),nvl(_c4,'')) as PartKeyColumn
      
         val conCatedCol= "concat("+ partSplitBuffer.substring(0,partSplitBuffer.length()-1) +") as PartKeyColumn"
       
         println("concated=>  "+ conCatedCol)
         //srcDf.selectExpr(conCatedCol,"*")

       srcDf.registerTempTable("partDataFrame")
       val ff=  spark.sqlContext.sql("select " + conCatedCol + " from partDataFrame" )
       
       ff
       * 
       * 
       */
         
         
     }
     
     
	 
	 
	 
     
       
    
     
     def dfToList(joinKeyPartKeyAppend:DataFrame) :List[String]= {
       
       
        val columnNameArray= joinKeyPartKeyAppend.columns.toArray.map(m=>m.substring(1))   //Array(c0, c1, c2, c3, c4, c5, c6, c7,....)
            
            val noOfColumnsInDf= columnNameArray.size   //25
           
            // SourceDfWithKey.show()
             val makeRdd = joinKeyPartKeyAppend.rdd
             val linearry: Array[String]  = makeRdd.map(_.mkString(",")).collect
             
             linearry.toList
     
     }
     
     
     
     

     
    val hadoopfs: FileSystem = FileSystem.get(spark.sparkContext.hadoopConfiguration)
    
     def write(filePath: String, data: Array[Byte]) = {
            val path = new Path(filePath)
             val os = hadoopfs.create(path)
             os.write(data)
             hadoopfs.close()
  }
    
    
    
    
    
    
             def mkdirs(folderPath: String): Unit = 
             {
                val path = new Path(folderPath)
                
                println("directory_creating_path"+ folderPath)
                     
                if (!hadoopfs.exists(path))
                {
                    hadoopfs.mkdirs(path)
                    println("directory_creating")
                     }
                 }
     

          //val path = "hdfs://quickstart.cloudera:8020/user/cloudera/dir"   //is directory 
     
  
           def isDirExist(path: String): Boolean = {
                    val p12 = new Path(path)
                    hadoopfs.exists(p12) && hadoopfs.getFileStatus(p12).isDirectory
                    }
    
    
           def testfileExist(path: String): Boolean = {
                    val p = new Path(path)
                  hadoopfs.exists(p) && hadoopfs.getFileStatus(p).isFile()
      
           }
           

     
           //--------------------------------------
    def listingFiles(path:String) :List[String]={

     var listdirbuff = new ListBuffer [String] ()
     var listfilebuff = new ListBuffer [String] ()

     if (isDirExist(path)){
       
       //"hdfs://quickstart.cloudera:8020/user/cloudera/dir"
     
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
    

    def listOfHivedir(path:String) :List[String] =
    {
      
      var listdirbuff = new ListBuffer [String] ()
 
      println("path=>ramesh"+path)
      
      if (isDirExist(path))
      {
       
       //"hdfs://quickstart.cloudera:8020/user/cloudera/dir"
     
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
    
    
  
   def processing (addingUniqkey:DataFrame, sourceFileList :List[String],hiveDirList :List[String],hiveTabledir :String,sourceSystem :String,sourceFeed:String)
  {
    // addingUniqueKey(sourceSystem: String,sourceFeed : String, Sep: String,key :String):
     
   ////////////////////////////////////////////////////////////////////////////  


 ////////////////////////////////////////////////////////////////////////////  
      
     // for i in WithUniqPartList
      
    //  WithUniqPartList.map(m=>m.split(",")(0).trim)
            
      
 for (i <- sourceFileList) {
		
   	val sourcePartition = i.split(",")(0).trim  //200006262615116
   	val navigatingPath = hiveTabledir + "\\" + sourcePartition   //hdfs://quickstart.cloudera:8020/user/cloudera/dir" + //200006262615116
    val mainDir = i.split(",")(0)
	  val subDir = navigatingPath +"\\" + i.split(",")(0).substring(0,8)
	  val fileName= "Incoming_"+sourceSystem+"_"+sourceFeed+"_merge_"+ mainDir +".dat"
	  val filepath = subDir +"\\" + fileName
   	val addingKeyInHiveFiles= addingUniqueKey(subDir,sourcePartition )
		val addingPartKeyInHiveFiles = addingPartitionKey(addingKeyInHiveFiles)
    val WithUniqPartHiveList= dfToList(addingPartKeyInHiveFiles)	   //List of all the records inside hive partition
                  
   		  if ( isDirExist(navigatingPath))
									{
				               
   		                  println("NavigationPath"+navigatingPath )
					              println("j=>"+sourcePartition )
				    	          println("sourcePartition=>"+sourcePartition )


					/*
									  val addingKeyInHiveFiles= addingUniqueKey(hiveTabledir,sourcePartition )
									  val addingPartKeyInHiveFiles = addingPartitionKey(addingKeyInHiveFiles)
                    val WithUniqPartHiveList= dfToList(addingPartKeyInHiveFiles)	   //List of all the records inside hive partition
            */        
                    //  WithUniqPartHiveList  contains the data inside the paritioned directory 
                    
                       for (k<- WithUniqPartHiveList)
                       {
                         val sourceUniqueKey = i.split(",")(1).trim
                         val hiveUniqueKey = k.split(",")(1).trim
                         
                                 if (sourceUniqueKey==hiveUniqueKey)
                                     {
                                       println("is equal=>Ramesh")
                                      collDupRcdsBuf += k
                                   
                                     }
                         
                         				else

														          {
                         				  
                         				    println("inside else")
															     
                         				  /*
															     
															      val filePath= navigatingPath +"\\" +sourceSystem+sourceFeed+"_merge_"+ sourceUniqueKey + ".dat"  //altered
															       println("creating an new file "+filePath)
															      val existingDF=   addingUniqkey.where(addingUniqkey("joiningKeyColumn") === sourceUniqueKey)
															      val dropingColumnDF=   existingDF.drop(existingDF.col("joiningKeyColumn")) 
															      println("1st else save area "+ navigatingPath+j)
															      
															      val newDataframe = dropingColumnDF.write.option("sep", "|")save(navigatingPath+j)
															      */
															      
															     //val newDataframe = dropingColumnDF.write.option("sep", "|")save(filePath)
															             
													          
															        }
														}
                         
                      // }
						}
              
						else
								  
						{
						  
	          mkdirs(navigatingPath)
	          
	          if (isDirExist(navigatingPath))
	          {

                 mkdirs(subDir)             
                 Write( subDir +"\\"+ fileName, /zxcscascascacawriting the list that contains matching record/ )
              
	          }    
            else
            {
             
              throw new Exception(srcPath + " Parent Directory not created successfully in this path" + navigatingPath)
              
            }
   	  /*
   	   *   println(s.substring(0,8))   //date
   println(s.substring(8,14))
   	  	println("inside 2nd else =>")
				val uniqueKey = i.split(",")(1).trim  
				val part_dir = i.split(",")(0).trim  
				val creationDir= hiveTabledir+"\\"+part_dir
				    
				   
				    /*
				    mkdirs(creationDir)
					  println("Directory created succesfully =>"+ navigatingPath)
					  println("sourceSystem =>"+ sourceSystem)
					  println("sourceFeed =>"+ sourceFeed)
					  val fileName= sourceSystem+sourceFeed+"_merge_"+ sourcePartition + uniqueKey +".dat"  //Unique key added
					  val filePath= navigatingPath +"\\"+fileName
					  val existingDF  = addingUniqkey.where(addingUniqkey("joiningKeyColumn") === uniqueKey)
		        val dropingColumnDF=   existingDF.drop(existingDF.col("joiningKeyColumn"))
		        println("elsewrite=>Ramesh "+filePath)
		        val newDataframe = dropingColumnDF.write.format("csv").option("sep", "|").save(creationDir)
            //val newDataframe = dropingColumnDF.write.format("csv").option("sep", "|").save(filePath)
                         */
			
   	  	/*
	  	 val newfile = new File(creationDir+"\\"+"part")
       println("dir created"+creationDir)
       newfile.createNewFile()
       val w = new PrintWriter(newfile)
       w.write("ramesh------------------------>Data inside the file ")
   	  
   	  */
   	  * 
   	  */
						  
						  

															             						  
						}
   	
   	
   	
   	
			}
   	
   	
   	//------------------------------------------//
   	
 
   	  
   	}
   	   
     	
   	
   	
   	
 }
						  
  }
   
						  
			
						  
	  
object scalaobj {

  println ("Initializing spark new")
  
  
        def main(args: Array[String]): Unit = 
            {
    

              val spark = SparkSession.builder()
              .master("local")
              .appName("hivedir")
              .getOrCreate()
              val sc = new cls(spark)
              val sourceSystem ="sourcesystem"
              val sourceFeed = "sourcefeed"
              //val sourceSystem ="cloudera"
              //val sourceFeed = "sourceFeed"
             // val hiveTableDirectory= "hdfs://quickstart.cloudera:8020/user/cloudera/dir/retailer"
              val hiveTableDirectory= "file:\\\\\\F:\\warehouse\\hive_table"
              val addingUniqkey = sc.addingUniqueKey( sc.defaultDir+sourceSystem, sourceFeed)
                // addingUniqkey.show()
              val addingPartKey = sc.addingPartitionKey(addingUniqkey)
               addingPartKey.show()
              val WithUniqPartList= sc.dfToList(addingPartKey)
              val hiveDirList= sc.listOfHivedir(hiveTableDirectory)
              println("hivedirfiles")
             // hiveDirList.foreach(println)
              sc.processing(addingUniqkey,WithUniqPartList,hiveDirList,hiveTableDirectory,sourceSystem,sourceFeed)
          //    println ("Printing the array")


     
  }
}
  

  
  
  
