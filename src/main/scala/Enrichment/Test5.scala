package Enrichment

import DataProc._

import java.io.File
import scala.reflect.io.{File => refFile}

object Test5 extends App {

//  val t0 = System.currentTimeMillis()
//  val res1 = getArrayOfSubDirectories("D:\\res\\megion\\test2")
//  val t1 = System.currentTimeMillis()
//  println(t1 - t0)
//  println(res1.sorted.mkString("Array(", ", ", ")"))

//  val t2 = System.currentTimeMillis()
//  val res2 = getSubDirs("D:\\res\\megion\\test2")
//  val t3 = System.currentTimeMillis()
//  println(t3 - t2)
//  println(res2.toArray.sorted.mkString("Array(", ", ", ")"))



//  val res11 = res1
//    .map(dir => (dir, new File(dir).listFiles.count(_.isDirectory)))
//  println("res11")
//  println(res11.mkString("Array(", ", ", ")"))
//  println()
//
//  val res12 = res1
//    .map(dir => (dir, new File(dir).listFiles.count(_.isDirectory)))
//    .filter(_._2 == 0)
//  println("res12")
//  println(res12.mkString("Array(", ", ", ")"))
//  println()

  // 1250
//  val t4 = System.currentTimeMillis()
//  val res13 = getArrayOfSubDirectories("D:\\res\\megion\\test2")
//    .map(dir => (dir, new File(dir).listFiles.count(_.isDirectory)))
//    .filter(_._2 == 0)
//    .map(_._1)
//  val t5 = System.currentTimeMillis()
//  println(t5 - t4)
//  println("res31")
//  println(res13.mkString("Array(", ", ", ")"))
//  println()

  // 1050
//  val t6 = System.currentTimeMillis()
//  val res21: List[String] = getEndFolders("D:\\res\\megion\\test2")
//  val t7 = System.currentTimeMillis()
//  println(t7 - t6)
//  println(res21.toArray.mkString("Array(", ", ", ")"))

//  val res14 = res21.toArray.map(_ + "\\_schema.txt").map(refFile(_).bufferedReader().readLine())
//  println(res14.mkString("Array(", ", ", ")"))
//  println()

//  val res22 = mergeSchemas(res21)
//  res22.foreach(println)
//  println(res22)

  val t7 = System.currentTimeMillis()
  val dirs = getArrayOfSubDirectories("/home/mike/res/tds_storage")
  val res1 = getArrayOfLeafsByArrayOfDirs(dirs)
  val t8 = System.currentTimeMillis()

  println(res1.mkString("Array(", ", ", ")"))
  println(res1.length)
  println(s"res1 = ${t8 - t7} ms")

  val t9 = System.currentTimeMillis()
  val res2 = getEndFolders("/home/mike/res/tds_storage")
  val t10 = System.currentTimeMillis()

  println(res2)
  println(res2.length)
  println(s"res2 = ${t10 - t9} ms")




}
