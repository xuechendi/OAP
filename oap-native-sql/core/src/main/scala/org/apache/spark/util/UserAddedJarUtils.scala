package org.apache.spark.util

import org.apache.spark.{SparkConf, SparkContext}
import java.io.File
import java.nio.file.Files
import java.nio.file.LinkOption
import java.nio.file.Path
import java.nio.file.Paths

object UserAddedJarUtils {
  def fetchJarFromSpark(
      urlString: String,
      targetDir: String,
      targetFileName: String,
      sparkConf: SparkConf): Unit = synchronized {
    val targetDirHandler = new File(targetDir)
    //TODO: don't fetch when exists
    val targetPath = Paths.get(targetDir + "/" + targetFileName)
    if (Files.notExists(targetPath)) {
      System.out.println(s"${targetPath} not exists, do fetch")
      Utils.doFetchFile(urlString, targetDirHandler, targetFileName, sparkConf, null, null)
    } else {
      System.out.println(s"${targetPath} exists, do not fetch")
    }
  }
}
