package org.apache.spark.util

import org.apache.spark.{SparkConf, SparkContext}
import java.io.File

object UserAddedJarUtils {
  def fetchJarFromSpark(
      urlString: String,
      targetDir: String,
      targetFileName: String,
      sparkConf: SparkConf): File = {
    System.out.println(
      s"urlString is ${urlString}, targetDir is ${targetDir}, targetFileName is ${targetFileName}")
    val targetDirHandler = new File(targetDir)
    //TODO: don't fetch when exists
    Utils.doFetchFile(urlString, targetDirHandler, targetFileName, sparkConf, null, null)
  }
}
