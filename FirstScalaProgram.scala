import scala.io.Source
import io.Source._
import java.io.File
import scala.collection.mutable.ListBuffer
import java.io.PrintWriter
object Newobj {
 var listbuff = new ListBuffer[String]()
 case class data1(id: Int, name: String, age: Int, date: String)
 def main(args: Array[String]): Unit = {
  val hive_table_directory = "C:\\Users\\TR20064147\\Desktop\\eclipse\\readme"
  val d = new File(hive_table_directory)
  println(d.isDirectory)
  val src_file_Text = Source.fromResource("Book1.csv").getLines()
  val src_fil = src_file_Text.map(f => data1(f.split(",")(0).toInt, f.split(",")(1), f.split(",")(2).toInt, f.split(",")(3)))
  val list_files_hive = new java.io.File(hive_table_directory).list()
  for (i < -src_fil) {
   for (j < -list_files_hive) {
    val n = i.date.substring(0, 10)
    if (j.equals(n)) {
     val valid_directy = new File(hive_table_directory + "\\" + j)
     println("equals" + valid_directy)
     if (valid_directy.isDirectory) {
      val listing_files = valid_directy.listFiles()
      for (k < -listing_files) {
       println("alligned_data")
       val opening_the_file = Source.fromFile(k).getLines
       val alligned_hive_file = opening_the_file.map(f => data1(f.split(",")(0).toInt, f.split(",")(1), f.split(",")(2).toInt, f.split(",")(3)))
       println("len" + src_fil.length)
       for (source_file < -src_fil) {
        println("inside for loop 1" + source_file.age)
        for (hive_file < -alligned_hive_file) {
         println("inside for loop 2")
         if (hive_file.id == source_file.id && hive_file.name == source_file.name && hive_file.age == source_file.age) {
          listbuff += hive_file.id + hive_file.name + hive_file.age
          println("listbuff")
         } else {
          println("creating an new directory")
          val new_dir_name = source_file.id + source_file.name + source_file.age + source_file.date
          val newfile = new File(valid_directy + "//" + new_dir_name)
          println(newfile)
          newfile.createNewFile()
          val w = new PrintWriter(newfile)
          w.write(source_file.id + "," + source_file.name + "," + source_file.age + "," + source_file.date)
          w.close()
         }
        }
       }
      }
     }
    }
   }
  }
  println("printing list buffer")
  listbuff.foreach(println)
 }
}
