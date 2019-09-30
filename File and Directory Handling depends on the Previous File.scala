package testmaven



object Demo {
   def main(args: Array[String]) {
      try {
         val f = new FileReader("input.txt")
      } catch {
         case ex: FileNotFoundException =>{
            println("Missing file exception")
         }
         
         case ex: IOException => {
            println("IO Exception")
         }
      }
   }
}


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




object scaleobj {
  
  private var localFilePath: File = new File(/*properties*/)
  private var dfsDirPath: String = ""

  private val NPARAMS = 2   //setting the number of parameters

  case class data1(id:Int,name:String,age:Int,date:String)


  private def readSrcFile(sourceSystem: String,sourceFeed : String): List[String] = {
    val path = sourceSystem + "/" + sourceFeed
    
    val sourceData=spark
    
    val lineList: List[String] = lineIter.toList
    lineList
}
  

  private def parseArgs(args: Array[String]): Unit = {
    if (args.length != NPARAMS) {
      System.err.println("Expected args Parameters not found")
      System.exit(1)
    }

    var i = 0

    localFilePath = new File(args(i))
    if (!localFilePath.exists) {
      System.err.println(s"Given path (${args(i)}) does not exist")
      System.exit(1)
    }

    if (!localFilePath.isFile) {
      System.err.println(s"Given path (${args(i)}) is not a file")
     // printUsage()
      System.exit(1)
    }

    i += 1
    dfsDirPath = args(i)
  }
  
   
  
  def mappingSourceFile(fileContents: List[String]): List[data1] = {
    
	val src_fil= fileContents.map(f=> data1( f.split(",")(0).toInt, f.split(",")(1), 
				    f.split(",")(2).toInt, f.split(",")(3))).toList
	     src_fil
  }
    
   
    
    
  

def main(args: Array[String]): Unit = {
  
    println("Validating arguments")
    parseArgs(args)
    val fileContents = readSrcFile(localFilePath.toString()) //
    val structSrcFileList = mappingSourceFile(fileContents)

    println("Creating SparkSession")
    val spark = SparkSession
      .builder
      .appName("DFS Read Write Test")
      .getOrCreate()
      
var listbuff = new ListBuffer [String] ()

var dirlistbuff = new ListBuffer [String] ()

var dirlistbuff2 = new ListBuffer [String] ()

var hive_data_buffer = new ListBuffer [data1] ()


		val hive_table_directory = "C:\\Users\\tamilselvan\\Documents\\ramesh\\ram"

				val d = new File(hive_table_directory)
				println("Ramesh")

				println(d.isDirectory)

			//	val src_file_Text  = Source.fromResource("Book2.csv").getLines()

				val src_fil= src_file_Text.map(f=> data1( f.split(",")(0).toInt, f.split(",")(1), 
				    f.split(",")(2).toInt, f.split(",")(3))).toList

	
		//hive table directry picking

				val  list_files_hive = new File(hive_table_directory)

				val listOfFiles = list_files_hive.listFiles()

				for (p <- listOfFiles)
				{
					if (p.isDirectory())
					{
						dirlistbuff+= p.getName()

					}
				}
	val hive_dir_lst=	dirlistbuff.toList

		//src_fil.foreach(println)

		//val list_files_hive =  new FileReader(hive_table_directory)

		//comparing the source file date with the hive directory patterns

		for (i <- src_fil) {
			println(i.id+i.name+i.age,i.date.substring(0, 10))

			for (j <- hive_dir_lst) {

				val n = i.date.substring(0, 10)

						println(i.date.substring(0, 10))

						if (  j.equals(n)) {
							val valid_directy =  new File(hive_table_directory + "\\" + j )   


									println("equals" + valid_directy)

									if ( valid_directy.isDirectory)
									{
									  //navigating into the directory 
										val dirfiles = valid_directy.listFiles()
												println("inside directory")


												//getting the list of files in the hive directory
												for (q <- dirfiles)
												{
													if (q.isFile())
													{
														dirlistbuff2+=q.getName()
																println("dirlistbuff2"+dirlistbuff2 )

													}
												}
										dirlistbuff2.toList
										//  val files_inside =  new File(valid_directy)   
										//val listing_files = valid_directy.listFiles()



										//iterating the files one by one 

										for (k <- dirlistbuff2)
										{

											val filepath =valid_directy +"\\"+k
													println("k-" + filepath)
													val opening_the_file = Source.fromFile(filepath).getLines

													//opening_the_file.foreach(println)  
													val alligned_hive_file= 
													  opening_the_file.map(f=> data1( f.split(",")(0).toInt, f.split(",")(1).toString(), 
															f.split(",")(2).toInt, f.split(",")(3).toString())).toList

													  hive_data_buffer += alligned_hive_file
													//file_data_buffer += alligned_hive_file_list.
													//
										}
								//	val hiv_file_data= hive_data_buffer.toList





												//comparing the i data with the 
									
									val source_file_unique_data = i.id+i.name+i.age

													for ( hive_file <- hiv_file_data )  {
													  val hive_file_unique_data = hive_file.
														println("inside for loop 2")
														if (hive_file == source_file_unique_data)

														{ 
															listbuff+= hive_file
																	println("listbuff")

														}

														else

														{
															println("creating an new directory")

															for (   source_file <- src_fil){
																val new_dir_name = source_file.id+ source_file.name+source_file.age+source_file.date

																		if (new_dir_name == hive_file)


																		 //val new_dir_name = source_file.date.substring(0, 10)
																			val newfile = new File(valid_directy + "//"+ new_dir_name)
																			println(newfile)
																			
																			newfile.createNewFile()
																			val w = new PrintWriter(newfile)
																			w.write(source_file.id + "," + source_file.name + "," + source_file.age + "," + source_file.date)
																			w.close()
															}
														}

													}

		

										// }

									}
						}

			}
		}
		println("printing list buffer")
		listbuff.foreach(println)

	}

}



/home/cloudera/eclipse-workspace1/new_proj/target/new_proj-0.0.1-SNAPSHOT.jar
/home/cloudera/eclipse-workspace1/new_proj/target/new_proj-0.0.1-SNAPSHOT.jar


val lol =FileSystem.get( sc.hadoopConfiguration ).listStatus( 

new Path("/FileStore/tables/")

).foreach( x => x.getPath.toString )





import org.apache.spark.{ SparkConf, SparkContext }
import org.apache.spark.sql.functions.broadcast
import org.apache.spark.sql.types._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
 
val sqlContext = new org.apache.spark.sql.SQLContext(sc)
import sqlContext.implicits._
 
 
case class Test(
  attr1:String,
  attr2:String
)
 
sc.setLogLevel("WARN")
import  org.apache.hadoop.fs.{FileSystem,Path}
val files = FileSystem.get( sc.hadoopConfiguration ).listStatus(new Path("/hadoopPath"))
 
 
def doSomething(file: String) = {
 
 println (file);
 
 // your logic of processing a single file comes here
 
 val x = sc.textFile(file);
 val classMapper = x.map(_.split("\\|"))
          .map(x => refLineID(
            x(0).toString,
            x(1).toString
          )).toDF
 
 
  classMapper.show()
 
 
}
 
files.foreach( filename => {
             // the following code makes sure "_SUCCESS" file name is not processed
             val a = filename.getPath.toString()
             val m = a.split("/")
             val name = m(10)
             println("\nFILENAME: " + name)
             if (name == "_SUCCESS") {
               println("Cannot Process '_SUCCSS' Filename")
             } else {
               doSomething(a)
             }
 
})
