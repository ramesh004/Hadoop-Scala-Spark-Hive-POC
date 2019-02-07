//Latest without any error

import scala.io.Source
import io.Source._
import java.io._
import scala.collection.mutable.ListBuffer
import java.io.PrintWriter
import java.io.FileReader



object Newobj {
  
  


case class data1(id:Int,name:String,age:Int,date:String)

def main(args: Array[String]): Unit = {
  

var listbuff = new ListBuffer [String] ()

var dirlistbuff = new ListBuffer [String] ()

		val hive_table_directory = "C:\\Users\\TR20064147\\Desktop\\eclipse\\readme"

				val d = new File(hive_table_directory)
				println("Ramesh")

				println(d.isDirectory)

				val src_file_Text  = Source.fromResource("Book1.csv").getLines()

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
						println("print n " + n + "print J "+j )

						if (  j.equals(n)) {
							val valid_directy =  new File(hive_table_directory + "\\" + j )   


									println("equals" + valid_directy)

									if ( valid_directy.isDirectory)
									{
									  //navigating into the directory 
										val dirfiles = valid_directy.listFiles()
												println("inside directory")

                var dirlistbuff2 = new ListBuffer [String] ()
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
					   	var hive_data_buffer = new ListBuffer [data1] ()				

										for (k <- dirlistbuff2)
										{

											val filepath =valid_directy +"\\"+k
													println("k-" + filepath)
													val opening_the_file = Source.fromFile(filepath).getLines

													//opening_the_file.foreach(println)  
													val  alligned_hive_file1 :List[data1] = opening_the_file.map(f=> data1( f.split(",")(0).toInt, f.split(",")(1),f.split(",")(2).toInt, f.split(",")(3))).toList
													//  hive_data_buffer = hive_data_buffer :: alligned_hive_file
														//	hive_data_buffer += alligned_hive_file1.toString()
											    	hive_data_buffer ++ alligned_hive_file1
													
										}
									val hiv_file_data= hive_data_buffer.toList

               
												//comparing the i data with the 
									
									      val source_file_unique_data = i.id+i.name+i.age

								  	    var found_bit=0
								  	    
													for ( hive_file <- hiv_file_data )  {
													  
													  val hive_file_unique_data =hive_file.id+hive_file.name+hive_file.age
													  
														println("inside for loop 2")
														if (hive_file_unique_data == source_file_unique_data)

														{ 
														  
															listbuff+= source_file_unique_data
															found_bit +=1
														}

													}
									
									        if (found_bit==0)
									        {
									          
									       // val dir = new File(hive_table_directory + "//"+ n)
									        //dir.mkdir()
													println("inside file creation")
													val newfile = new File(hive_table_directory +"//"+ n+"//"+ source_file_unique_data)
													println("newfile"+newfile)
													newfile.createNewFile()
													val w = new PrintWriter(newfile)
													w.write(i.id + "," + i.name + "," + i.age + "," + i.date)
													w.close()
									        }
									
									
												}


									}
				
						else
						{
						  
				  val dir = new File(hive_table_directory + "//"+ n)
					dir.mkdir()
					println("directory created"+dir)
				  val newfile1 = new File(dir + "//"+ i.id+ i.name + + i.age )
					println(newfile1)
					newfile1.createNewFile()
					val w = new PrintWriter(newfile1)
					w.write(i.id + "," + i.name + "," + i.age + "," + i.date)
					w.close()
				 
						  
						  
						  
						}
				
				
						}

			}
	
		println("printing list buffer")
		listbuff.foreach(println)
}

	}
