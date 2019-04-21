

--ramesh
import scala.io.Source
import io.Source._
import java.io._
import scala.collection.mutable.ListBuffer
import java.io.PrintWriter
import java.io.FileReader



object ff {

case class data1(id:Int,name:String,age:Int,date:String)

def main(args: Array[String]): Unit = {
  

var listbuff = new ListBuffer [String] ()

var dirlistbuff = new ListBuffer [String] ()

var dirlistbuff2 = new ListBuffer [String] ()

var hive_data_buffer = new ListBuffer [ff.data1] ()


		val hive_table_directory = "C:\\Users\\tamilselvan\\Documents\\ramesh\\ram"

				val d = new File(hive_table_directory)
				println("Ramesh")

				println(d.isDirectory)

				val src_file_Text  = Source.fromResource("Book2.csv").getLines()

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
