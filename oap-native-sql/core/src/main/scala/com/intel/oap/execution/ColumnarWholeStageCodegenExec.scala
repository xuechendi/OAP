/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.intel.oap.execution

import com.intel.oap.execution._
import org.apache.spark._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.expressions.{Attribute, SortOrder}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.plans.physical.Partitioning
import org.apache.spark.sql.execution.metric.{SQLMetric, SQLMetrics}
import org.apache.spark.sql.execution._
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.vectorized.ColumnarBatch

import org.apache.arrow.gandiva.expression._
import org.apache.arrow.vector.types.pojo.Schema

import com.intel.oap.vectorized.ExpressionEvaluator
import com.intel.oap.vectorized.BatchIterator

case class ColumnarCodegenContext(
    inputSchema: Schema,
    outputSchema: Schema,
    expressions: Seq[ExpressionTree],
    children: Seq[ColumnarCodegenContext] = null) {}

trait ColumnarCodegenSupport extends SparkPlan {

  /**
   * Whether this SparkPlan supports whole stage codegen or not.
   */
  def supportColumnarCodegen: Boolean = true

  /**
   * Returns all the RDDs of ColumnarBatch which generates the input rows.
   *
   * @note Right now we support up to two RDDs
   */
  def inputRDDs: Seq[RDD[ColumnarBatch]]

  def doCodeGen: ColumnarCodegenContext

}

case class ColumnarWholeStageCodegenExec(child: SparkPlan)(val codegenStageId: Int)
    extends UnaryExecNode
    with ColumnarCodegenSupport {

  override def output: Seq[Attribute] = child.output

  override def outputPartitioning: Partitioning = child.outputPartitioning

  override def outputOrdering: Seq[SortOrder] = child.outputOrdering

  // This is not strictly needed because the codegen transformation happens after the columnar
  // transformation but just for consistency
  override def supportColumnarCodegen: Boolean = true

  override def supportsColumnar: Boolean = true

  override lazy val metrics = Map(
    "pipelineTime" -> SQLMetrics
      .createTimingMetric(sparkContext, "duration"))

  override def otherCopyArgs: Seq[AnyRef] = Seq(codegenStageId.asInstanceOf[Integer])
  override def generateTreeString(
      depth: Int,
      lastChildren: Seq[Boolean],
      append: String => Unit,
      verbose: Boolean,
      prefix: String = "",
      addSuffix: Boolean = false,
      maxFields: Int,
      printNodeId: Boolean): Unit = {
    val res = child.generateTreeString(
      depth,
      lastChildren,
      append,
      verbose,
      if (printNodeId) "* " else s"*($codegenStageId) ",
      false,
      maxFields,
      printNodeId)
    System.out.println(s"${res}")
    res
  }

  override def nodeName: String = s"WholeStageCodegenColumnar (${codegenStageId})"
  override def inputRDDs(): Seq[RDD[ColumnarBatch]] = {
    throw new UnsupportedOperationException
  }
  override def doExecute(): RDD[InternalRow] = {
    throw new UnsupportedOperationException
  }
  override def doExecuteColumnar(): RDD[ColumnarBatch] = {
    child.executeColumnar()
  }
  override def doCodeGen: ColumnarCodegenContext = {
    child.doCodeGen
  }

  var nativeKernel: ExpressionEvaluator = _

  /**
   * Return built cpp library's signature
   */
  def doBuild: String =
    try {
      // call native wholestagecodegen build
      val resCtx = doCodeGen
      nativeKernel = new ExpressionEvaluator()
      nativeKernel.build(
        resCtx.inputSchema,
        resCtx.expressions.toList.asJava,
        resCtx.outputSchema,
        true)
    } catch {
      case e: Throwable =>
        throw e
    }
  def uploadAndListJars(signature: String): List[String] =
    if (signature != "") {
      if (sparkContext.listJars.filter(path => path.contains(s"${signature}.jar")).isEmpty) {
        val tempDir = ColumnarPluginConfig.getRandomTempDir
        val jarFileName =
          s"${tempDir}/tmp/spark-columnar-plugin-codegen-precompile-${signature}.jar"
        sparkContext.addJar(jarFileName)
      }
      sparkContext.listJars.filter(path => path.contains(s"${signature}.jar"))
    } else {
      List()
    }

}
