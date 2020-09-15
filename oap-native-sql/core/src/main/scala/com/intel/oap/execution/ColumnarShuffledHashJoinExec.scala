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

import java.util.concurrent.TimeUnit._

import com.intel.oap.vectorized._
import com.intel.oap.ColumnarPluginConfig

import org.apache.spark.TaskContext
import org.apache.spark.rdd.RDD
import org.apache.spark.util.{Utils, UserAddedJarUtils}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.catalyst.expressions.codegen._
import org.apache.spark.sql.catalyst.plans._
import org.apache.spark.sql.catalyst.plans.physical._
import org.apache.spark.sql.execution.{BinaryExecNode, CodegenSupport, SparkPlan}
import org.apache.spark.sql.execution.metric.SQLMetrics

import scala.collection.JavaConverters._
import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.expressions.BoundReference
import org.apache.spark.sql.catalyst.expressions.BindReferences.bindReference
import org.apache.spark.sql.vectorized.{ColumnarBatch, ColumnVector}
import scala.collection.mutable.ListBuffer
import org.apache.arrow.vector.ipc.message.ArrowFieldNode
import org.apache.arrow.vector.ipc.message.ArrowRecordBatch
import org.apache.arrow.vector.types.pojo.ArrowType
import org.apache.arrow.vector.types.pojo.Field
import org.apache.arrow.vector.types.pojo.Schema
import org.apache.arrow.gandiva.expression._
import org.apache.arrow.gandiva.evaluator._

import io.netty.buffer.ArrowBuf
import com.google.common.collect.Lists;

import com.intel.oap.expression._
import com.intel.oap.vectorized.ExpressionEvaluator
import org.apache.spark.sql.execution.joins.ShuffledHashJoinExec
import org.apache.spark.sql.execution.joins.{BuildLeft, BuildRight, BuildSide, HashJoin}

/**
 * Performs a hash join of two child relations by first shuffling the data using the join keys.
 */
case class ColumnarShuffledHashJoinExec(
    leftKeys: Seq[Expression],
    rightKeys: Seq[Expression],
    joinType: JoinType,
    buildSide: BuildSide,
    condition: Option[Expression],
    left: SparkPlan,
    right: SparkPlan)
    extends BinaryExecNode
    with ColumnarCodegenSupport
    with HashJoin {

  val sparkConf = sparkContext.getConf
  override lazy val metrics = Map(
    "numOutputRows" -> SQLMetrics.createMetric(sparkContext, "number of output rows"),
    "processTime" -> SQLMetrics.createTimingMetric(sparkContext, "totaltime_hashjoin"),
    "fetchTime" -> SQLMetrics.createTimingMetric(sparkContext, "time to fetch batches"),
    "buildTime" -> SQLMetrics.createTimingMetric(sparkContext, "time to build hash map"),
    "joinTime" -> SQLMetrics.createTimingMetric(sparkContext, "join time"))

  val numOutputRows = longMetric("numOutputRows")
  val totalTime = longMetric("processTime")
  val joinTime = longMetric("joinTime")
  val buildTime = longMetric("buildTime")
  val fetchTime = longMetric("fetchTime")
  val resultSchema = this.schema

  val (buildKeyExprs, streamedKeyExprs) = buildSide match {
    case BuildLeft =>
      (leftKeys, rightKeys)
    case _ =>
      (rightKeys, leftKeys)
  }

  /*protected lazy val (buildPlan, streamedPlan, buildKeys, streamKeys) = buildSide match {
    case BuildLeft => (left, right, leftKeys, rightKeys)
    case BuildRight => (right, left, rightKeys, leftKeys)
  }*/

  def getBuildPlan: SparkPlan = buildPlan
  override protected def doExecute(): RDD[InternalRow] = {
    throw new UnsupportedOperationException(
      s"ColumnarShuffledHashJoinExec doesn't support doExecute")
  }
  override def supportsColumnar = true
  override def inputRDDs(): Seq[RDD[ColumnarBatch]] = streamedPlan match {
    case c: ColumnarCodegenSupport if c.supportColumnarCodegen == true =>
      c.inputRDDs
    case _ =>
      Seq(streamedPlan.executeColumnar())
  }
  override def getHashBuildPlans: Seq[SparkPlan] = streamedPlan match {
    case c: ColumnarCodegenSupport if c.supportColumnarCodegen == true =>
      val childPlans = c.getHashBuildPlans
      childPlans :+ this
    case _ =>
      Seq(this)
  }

  override def dependentPlanCtx: ColumnarCodegenContext = {
    val inputSchema = ConverterUtils.toArrowSchema(buildPlan.output)
    ColumnarCodegenContext(
      inputSchema,
      null,
      ColumnarConditionedProbeJoin.prepareHashBuildFunction(buildKeyExprs, buildPlan.output))
  }

  override def supportColumnarCodegen: Boolean = true

  def getKernelFunction: TreeNode = {

    val buildInputAttributes = buildPlan.output.toList
    val streamInputAttributes = streamedPlan.output.toList
    // For Join, we need to do two things
    // 1. create buildHashRelation RDD ?
    // 2. create streamCodeGen and return

    ColumnarConditionedProbeJoin.prepareKernelFunction(
      buildKeyExprs,
      streamedKeyExprs,
      buildInputAttributes,
      streamInputAttributes,
      output,
      joinType,
      buildSide,
      condition)
  }

  override def doCodeGen: ColumnarCodegenContext = {
    val childCtx = streamedPlan match {
      case c: ColumnarCodegenSupport if c.supportColumnarCodegen == true =>
        c.doCodeGen
      case _ =>
        null
    }
    val outputSchema = ConverterUtils.toArrowSchema(output)
    val (codeGenNode, inputSchema) = if (childCtx != null) {
      (
        TreeBuilder.makeFunction(
          s"child",
          Lists.newArrayList(getKernelFunction, childCtx.root),
          new ArrowType.Int(32, true)),
        childCtx.inputSchema)
    } else {
      (
        TreeBuilder
          .makeFunction(
            s"child",
            Lists.newArrayList(getKernelFunction),
            new ArrowType.Int(32, true)),
        ConverterUtils.toArrowSchema(streamedPlan.output))
    }
    ColumnarCodegenContext(inputSchema, outputSchema, codeGenNode)
  }

  def getSignature: String =
    if (resultSchema.size > 0) {
      try {
        ColumnarShuffledHashJoin.prebuild(
          leftKeys,
          rightKeys,
          resultSchema,
          joinType,
          buildSide,
          condition,
          left,
          right,
          sparkConf)
      } catch {
        case e: UnsupportedOperationException
            if e.getMessage == "Unsupport to generate native expression from replaceable expression." =>
          logWarning(e.getMessage())
          ""
        case e: Throwable =>
          throw e
      }
    } else {
      ""
    }
  def getListJars: Seq[String] =
    if (signature != "") {
      if (sparkContext.listJars.filter(path => path.contains(s"${signature}.jar")).isEmpty) {
        val tempDir = ColumnarPluginConfig.getRandomTempDir
        val jarFileName =
          s"${tempDir}/tmp/spark-columnar-plugin-codegen-precompile-${signature}.jar"
        sparkContext.addJar(jarFileName)
      }
      sparkContext.listJars.filter(path => path.contains(s"${signature}.jar"))
    } else {
      Seq()
    }

  var (signature, listJars) =
    if (supportColumnarCodegen && sparkConf
          .getBoolean("spark.sql.codegen.wholeStage", defaultValue = true)) {
      (null, null)
    } else {
      (getSignature, getListJars)
    }

  override def doExecuteColumnar(): RDD[ColumnarBatch] = {
    signature = if (signature == null) getSignature else signature
    listJars = if (listJars == null) getListJars else listJars
    streamedPlan.executeColumnar().zipPartitions(buildPlan.executeColumnar()) {
      (streamIter, buildIter) =>
        ColumnarPluginConfig.getConf(sparkConf)
        val execTempDir = ColumnarPluginConfig.getTempFile
        val jarList = listJars
          .map(jarUrl => {
            logWarning(s"Get Codegened library Jar ${jarUrl}")
            UserAddedJarUtils.fetchJarFromSpark(
              jarUrl,
              execTempDir,
              s"spark-columnar-plugin-codegen-precompile-${signature}.jar",
              sparkConf)
            s"${execTempDir}/spark-columnar-plugin-codegen-precompile-${signature}.jar"
          })

        val vjoin = ColumnarShuffledHashJoin.create(
          leftKeys,
          rightKeys,
          resultSchema,
          joinType,
          buildSide,
          condition,
          left,
          right,
          jarList,
          buildTime,
          joinTime,
          totalTime,
          numOutputRows,
          sparkConf)
        TaskContext
          .get()
          .addTaskCompletionListener[Unit](_ => {
            vjoin.close()
          })
        val vjoinResult = vjoin.columnarJoin(streamIter, buildIter)
        new CloseableColumnBatchIterator(vjoinResult)
    }
  }

  override def canEqual(other: Any): Boolean = other.isInstanceOf[ColumnarShuffledHashJoinExec]

  override def equals(other: Any): Boolean = other match {
    case that: ColumnarShuffledHashJoinExec =>
      (that canEqual this) && super.equals(that)
    case _ => false
  }
}
