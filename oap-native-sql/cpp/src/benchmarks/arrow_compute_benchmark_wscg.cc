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

#include <arrow/filesystem/filesystem.h>
#include <arrow/io/interfaces.h>
#include <arrow/memory_pool.h>
#include <arrow/pretty_print.h>
#include <arrow/record_batch.h>
#include <arrow/testing/gtest_util.h>
#include <arrow/type.h>
#include <gandiva/node.h>
#include <gtest/gtest.h>
#include <parquet/arrow/reader.h>
#include <parquet/file_reader.h>

#include <chrono>

#include "codegen/code_generator.h"
#include "codegen/code_generator_factory.h"
#include "codegen/common/result_iterator.h"
#include "tests/test_utils.h"

namespace sparkcolumnarplugin {
namespace codegen {

class BenchmarkArrowComputeWSCG : public ::testing::Test {
 public:
  void SetUp() override {
    // read input from parquet file
#ifdef BENCHMARK_FILE_PATH
    std::string dir_path = BENCHMARK_FILE_PATH;
#else
    std::string dir_path = "";
#endif
    std::string build_path_1 = dir_path + "tpch_500/supplier";
    std::string build_path_2 = dir_path + "tpch_500/nation";
    std::string build_path_3 = dir_path + "tpch_500/region";
    std::string stream_path = dir_path + "tpch_500/partsupp";

    std::cout << "Read files are " << build_path_1 << ", " << build_path_2 << ", "
              << build_path_3 << ", " << stream_path << std::endl;
    std::shared_ptr<arrow::fs::FileSystem> build_1_fs;
    std::shared_ptr<arrow::fs::FileSystem> build_2_fs;
    std::shared_ptr<arrow::fs::FileSystem> build_3_fs;
    std::shared_ptr<arrow::fs::FileSystem> stream_fs;
    std::string build_1_file_name;
    std::string build_2_file_name;
    std::string build_3_file_name;
    std::string stream_file_name;
    ASSERT_OK_AND_ASSIGN(build_1_fs,
                         arrow::fs::FileSystemFromUri(build_path_1, &build_1_file_name));
    ASSERT_OK_AND_ASSIGN(build_2_fs,
                         arrow::fs::FileSystemFromUri(build_path_2, &build_2_file_name));
    ASSERT_OK_AND_ASSIGN(build_3_fs,
                         arrow::fs::FileSystemFromUri(build_path_3, &build_3_file_name));
    ASSERT_OK_AND_ASSIGN(stream_fs,
                         arrow::fs::FileSystemFromUri(stream_path, &stream_file_name));

    arrow::fs::FileSelector build_1_dataset_dir_selector;
    build_1_dataset_dir_selector.base_dir = build_1_file_name;
    auto build_1_file_infos =
        build_1_fs->GetFileInfo(build_1_dataset_dir_selector).ValueOrDie();
    for (const auto& file_info : build_1_file_infos) {
      auto file = build_1_fs->OpenInputFile(file_info.path()).ValueOrDie();
      build_1_files.push_back(file);
    }

    arrow::fs::FileSelector build_2_dataset_dir_selector;
    build_2_dataset_dir_selector.base_dir = build_2_file_name;
    auto build_2_file_infos =
        build_2_fs->GetFileInfo(build_2_dataset_dir_selector).ValueOrDie();
    for (const auto& file_info : build_2_file_infos) {
      auto file = build_2_fs->OpenInputFile(file_info.path()).ValueOrDie();
      build_2_files.push_back(file);
    }

    arrow::fs::FileSelector build_3_dataset_dir_selector;
    build_3_dataset_dir_selector.base_dir = build_3_file_name;
    auto build_3_file_infos =
        build_3_fs->GetFileInfo(build_3_dataset_dir_selector).ValueOrDie();
    for (const auto& file_info : build_3_file_infos) {
      auto file = build_3_fs->OpenInputFile(file_info.path()).ValueOrDie();
      build_3_files.push_back(file);
    }

    arrow::fs::FileSelector stream_dataset_dir_selector;
    stream_dataset_dir_selector.base_dir = stream_file_name;
    auto stream_file_infos =
        stream_fs->GetFileInfo(stream_dataset_dir_selector).ValueOrDie();
    for (const auto& file_info : stream_file_infos) {
      auto file = stream_fs->OpenInputFile(file_info.path()).ValueOrDie();
      stream_files.push_back(file);
    }

    properties.set_batch_size(10240);
    pool = arrow::default_memory_pool();

    ASSERT_NOT_OK(::parquet::arrow::FileReader::Make(
        pool, ::parquet::ParquetFileReader::Open(build_1_files[0]), properties,
        &build_1_parquet_reader));
    ASSERT_NOT_OK(build_1_parquet_reader->GetRecordBatchReader(
        {0}, {0, 3}, &build_1_record_batch_reader));

    ASSERT_NOT_OK(::parquet::arrow::FileReader::Make(
        pool, ::parquet::ParquetFileReader::Open(build_2_files[0]), properties,
        &build_2_parquet_reader));
    ASSERT_NOT_OK(build_2_parquet_reader->GetRecordBatchReader(
        {0}, {0, 2}, &build_2_record_batch_reader));

    ASSERT_NOT_OK(::parquet::arrow::FileReader::Make(
        pool, ::parquet::ParquetFileReader::Open(build_3_files[0]), properties,
        &build_3_parquet_reader));
    ASSERT_NOT_OK(build_3_parquet_reader->GetRecordBatchReader(
        {0}, {0, 1}, &build_3_record_batch_reader));

    ASSERT_NOT_OK(::parquet::arrow::FileReader::Make(
        pool, ::parquet::ParquetFileReader::Open(stream_files[0]), properties,
        &stream_parquet_reader));
    ASSERT_NOT_OK(stream_parquet_reader->GetRecordBatchReader(
        {0}, {0, 1, 3}, &stream_record_batch_reader));
  }

 protected:
  std::vector<std::shared_ptr<arrow::io::RandomAccessFile>> build_1_files;
  std::vector<std::shared_ptr<arrow::io::RandomAccessFile>> build_2_files;
  std::vector<std::shared_ptr<arrow::io::RandomAccessFile>> build_3_files;
  std::vector<std::shared_ptr<arrow::io::RandomAccessFile>> stream_files;
  std::unique_ptr<::parquet::arrow::FileReader> build_1_parquet_reader;
  std::unique_ptr<::parquet::arrow::FileReader> build_2_parquet_reader;
  std::unique_ptr<::parquet::arrow::FileReader> build_3_parquet_reader;
  std::unique_ptr<::parquet::arrow::FileReader> stream_parquet_reader;
  std::shared_ptr<RecordBatchReader> build_1_record_batch_reader;
  std::shared_ptr<RecordBatchReader> build_2_record_batch_reader;
  std::shared_ptr<RecordBatchReader> build_3_record_batch_reader;
  std::shared_ptr<RecordBatchReader> stream_record_batch_reader;

  std::vector<std::shared_ptr<::gandiva::Expression>> expr_vector;
  std::vector<std::shared_ptr<::arrow::Field>> ret_field_list;

  parquet::ArrowReaderProperties properties;
  arrow::MemoryPool* pool;
};

TEST_F(BenchmarkArrowComputeWSCG, JoinBenchmark) {
  int left_primary_key_index = 0;
  int right_primary_key_index = 0;

  auto build_1_schema = build_1_record_batch_reader->schema();
  auto build_2_schema = build_2_record_batch_reader->schema();
  auto build_3_schema = build_3_record_batch_reader->schema();
  auto stream_schema = stream_record_batch_reader->schema();

  auto build_1_field_list = build_1_record_batch_reader->schema()->fields();
  auto build_2_field_list = build_2_record_batch_reader->schema()->fields();
  auto build_3_field_list = build_3_record_batch_reader->schema()->fields();
  auto stream_field_list = stream_record_batch_reader->schema()->fields();

  // prepare expression
  auto n_hash_config = TreeExprBuilder::MakeFunction(
      "build_keys_config_node", {TreeExprBuilder::MakeLiteral((int)1)}, uint32());
  auto f_res = field("res", uint32());

  std::vector<std::shared_ptr<::gandiva::Node>> build_1_field_node_list;
  for (auto field : build_1_field_list) {
    build_1_field_node_list.push_back(TreeExprBuilder::MakeField(field));
  }
  auto n_build_1 = TreeExprBuilder::MakeFunction(
      "codegen_left_schema", {build_1_field_node_list[left_primary_key_index]}, uint32());

  auto n_hash_relation_1_kernel = TreeExprBuilder::MakeFunction(
      "standalone",
      {TreeExprBuilder::MakeFunction("HashRelation", {n_build_1, n_hash_config},
                                     uint32())},
      uint32());
  auto hashRelation_1_expr =
      TreeExprBuilder::MakeExpression(n_hash_relation_1_kernel, f_res);
  std::cout << hashRelation_1_expr->ToString() << std::endl;
  std::shared_ptr<CodeGenerator> expr_build_1;
  ASSERT_NOT_OK(CreateCodeGenerator(build_1_schema, {hashRelation_1_expr}, {},
                                    &expr_build_1, true));
  //////////////////////////////////////////////////////////////

  std::vector<std::shared_ptr<::gandiva::Node>> build_2_field_node_list;
  for (auto field : build_2_field_list) {
    build_2_field_node_list.push_back(TreeExprBuilder::MakeField(field));
  }
  auto n_build_2 = TreeExprBuilder::MakeFunction(
      "codegen_left_schema", {build_2_field_node_list[left_primary_key_index]}, uint32());

  auto n_hash_relation_2_kernel = TreeExprBuilder::MakeFunction(
      "standalone",
      {TreeExprBuilder::MakeFunction("HashRelation", {n_build_2, n_hash_config},
                                     uint32())},
      uint32());
  auto hashRelation_2_expr =
      TreeExprBuilder::MakeExpression(n_hash_relation_2_kernel, f_res);
  std::cout << hashRelation_2_expr->ToString() << std::endl;
  std::shared_ptr<CodeGenerator> expr_build_2;
  ASSERT_NOT_OK(CreateCodeGenerator(build_2_schema, {hashRelation_2_expr}, {},
                                    &expr_build_2, true));
  //////////////////////////////////////////////////////////////

  std::vector<std::shared_ptr<::gandiva::Node>> build_3_field_node_list;
  for (auto field : build_3_field_list) {
    build_3_field_node_list.push_back(TreeExprBuilder::MakeField(field));
  }
  auto n_build_3 = TreeExprBuilder::MakeFunction(
      "codegen_left_schema", {build_3_field_node_list[left_primary_key_index]}, uint32());

  auto n_hash_relation_3_kernel = TreeExprBuilder::MakeFunction(
      "standalone",
      {TreeExprBuilder::MakeFunction("HashRelation", {n_build_3, n_hash_config},
                                     uint32())},
      uint32());
  auto hashRelation_3_expr =
      TreeExprBuilder::MakeExpression(n_hash_relation_3_kernel, f_res);
  std::cout << hashRelation_3_expr->ToString() << std::endl;
  std::shared_ptr<CodeGenerator> expr_build_3;
  ASSERT_NOT_OK(CreateCodeGenerator(build_3_schema, {hashRelation_3_expr}, {},
                                    &expr_build_3, true));
  //////////////////////////////////////////////////////////////

  // prepare expression
  std::vector<std::shared_ptr<::gandiva::Node>> stream_field_node_list;
  for (auto field : stream_field_list) {
    stream_field_node_list.push_back(TreeExprBuilder::MakeField(field));
  }

  //////////////////////////////////////////////////////////////
  auto n_join_1_build = TreeExprBuilder::MakeFunction("codegen_right_schema",
                                                      build_1_field_node_list, uint32());
  auto n_join_1_stream = TreeExprBuilder::MakeFunction("codegen_right_schema",
                                                       stream_field_node_list, uint32());
  auto n_join_1_build_key = TreeExprBuilder::MakeFunction(
      "codegen_left_schema", {build_1_field_node_list[left_primary_key_index]}, uint32());
  auto n_join_1_stream_key = TreeExprBuilder::MakeFunction(
      "codegen_right_schema", {stream_field_node_list[right_primary_key_index]},
      uint32());

  auto join_1_res_field_list = {stream_field_list[0], stream_field_list[2],
                                build_1_field_list[1]};
  auto join_1_res_schema = arrow::schema(join_1_res_field_list);

  ::gandiva::NodeVector join_1_result_node_list;
  for (auto field : join_1_res_field_list) {
    join_1_result_node_list.push_back(TreeExprBuilder::MakeField(field));
  }
  auto n_join_1_result =
      TreeExprBuilder::MakeFunction("result", join_1_result_node_list, uint32());
  auto n_join_1 = TreeExprBuilder::MakeFunction(
      "child",
      {TreeExprBuilder::MakeFunction(
          "conditionedProbeArraysInner",
          {n_join_1_build, n_join_1_stream, n_join_1_build_key, n_join_1_stream_key,
           n_join_1_result, n_hash_config},
          uint32())},
      uint32());

  //////////////////////////////////////////////////////////////

  auto n_join_2_build = TreeExprBuilder::MakeFunction("codegen_right_schema",
                                                      build_2_field_node_list, uint32());
  auto n_join_2_stream = TreeExprBuilder::MakeFunction("codegen_right_schema",
                                                       join_1_result_node_list, uint32());
  auto n_join_2_build_key = TreeExprBuilder::MakeFunction(
      "codegen_left_schema", {build_2_field_node_list[left_primary_key_index]}, uint32());
  auto n_join_2_stream_key = TreeExprBuilder::MakeFunction(
      "codegen_right_schema", {join_1_result_node_list[2]}, uint32());

  auto join_2_res_field_list = {stream_field_list[0], stream_field_list[2],
                                build_2_field_list[1]};
  auto join_2_res_schema = arrow::schema(join_2_res_field_list);

  ::gandiva::NodeVector join_2_result_node_list;
  for (auto field : join_2_res_field_list) {
    join_2_result_node_list.push_back(TreeExprBuilder::MakeField(field));
  }
  auto n_join_2_result =
      TreeExprBuilder::MakeFunction("result", join_2_result_node_list, uint32());
  auto n_join_2 = TreeExprBuilder::MakeFunction(
      "child",
      {TreeExprBuilder::MakeFunction(
           "conditionedProbeArraysInner",
           {n_join_2_build, n_join_2_stream, n_join_2_build_key, n_join_2_stream_key,
            n_join_2_result, n_hash_config},
           uint32()),
       n_join_1},
      uint32());

  //////////////////////////////////////////////////////////////
  auto n_join_3_build = TreeExprBuilder::MakeFunction("codegen_right_schema",
                                                      build_3_field_node_list, uint32());
  auto n_join_3_stream = TreeExprBuilder::MakeFunction("codegen_right_schema",
                                                       join_2_result_node_list, uint32());
  auto n_join_3_build_key = TreeExprBuilder::MakeFunction(
      "codegen_left_schema", {build_3_field_node_list[left_primary_key_index]}, uint32());
  auto n_join_3_stream_key = TreeExprBuilder::MakeFunction(
      "codegen_right_schema", {join_2_result_node_list[2]}, uint32());

  auto join_3_res_field_list = {stream_field_list[0], stream_field_list[2]};
  auto join_3_res_schema = arrow::schema(join_3_res_field_list);

  ::gandiva::NodeVector join_3_result_node_list;
  for (auto field : join_3_res_field_list) {
    join_3_result_node_list.push_back(TreeExprBuilder::MakeField(field));
  }
  auto n_join_3_result =
      TreeExprBuilder::MakeFunction("result", join_3_result_node_list, uint32());
  auto n_join_3 = TreeExprBuilder::MakeFunction(
      "child",
      {TreeExprBuilder::MakeFunction(
           "conditionedProbeArraysInner",
           {n_join_3_build, n_join_3_stream, n_join_3_build_key, n_join_3_stream_key,
            n_join_3_result, n_hash_config},
           uint32()),
       n_join_2},
      uint32());

  //////////////////////////////////////////////////////////////

  auto n_wscg = TreeExprBuilder::MakeFunction("wholestagecodegen", {n_join_3}, uint32());

  auto probeArrays_expr = TreeExprBuilder::MakeExpression(n_wscg, f_res);
  std::cout << probeArrays_expr->ToString() << std::endl;

  std::shared_ptr<CodeGenerator> expr_probe;
  ASSERT_NOT_OK(CreateCodeGenerator(stream_schema, {probeArrays_expr},
                                    join_3_res_field_list, &expr_probe, true));

  ///////////////////// Calculation //////////////////
  std::vector<std::shared_ptr<arrow::RecordBatch>> dummy_result_batches;
  std::shared_ptr<ResultIteratorBase> build_result_iterator_1;
  std::shared_ptr<ResultIteratorBase> build_result_iterator_2;
  std::shared_ptr<ResultIteratorBase> build_result_iterator_3;
  std::shared_ptr<ResultIteratorBase> probe_result_iterator_base;

  ////////////////////// evaluate //////////////////////
  std::shared_ptr<arrow::RecordBatch> build_1_record_batch;
  std::shared_ptr<arrow::RecordBatch> build_2_record_batch;
  std::shared_ptr<arrow::RecordBatch> build_3_record_batch;
  std::shared_ptr<arrow::RecordBatch> stream_record_batch;
  uint64_t elapse_gen = 0;
  uint64_t elapse_left_read = 0;
  uint64_t elapse_right_read = 0;
  uint64_t elapse_eval = 0;
  uint64_t elapse_finish = 0;
  uint64_t elapse_probe_process = 0;
  uint64_t elapse_shuffle_process = 0;
  uint64_t num_batches = 0;
  uint64_t num_rows = 0;
  int build_1_reader_index = 1;
  int build_2_reader_index = 1;
  int build_3_reader_index = 1;
  int stream_reader_index = 1;

  TIME_MICRO_OR_THROW(elapse_finish, expr_probe->finish(&probe_result_iterator_base));
  auto probe_result_iterator =
      std::dynamic_pointer_cast<ResultIterator<arrow::RecordBatch>>(
          probe_result_iterator_base);

  do {
    do {
      TIME_MICRO_OR_THROW(elapse_left_read,
                          build_1_record_batch_reader->ReadNext(&build_1_record_batch));
      if (build_1_record_batch) {
        TIME_MICRO_OR_THROW(elapse_eval, expr_build_1->evaluate(build_1_record_batch,
                                                                &dummy_result_batches));
        num_batches += 1;
        num_rows += build_1_record_batch->num_rows();
      }
    } while (build_1_record_batch);
    // if (++build_1_reader_index < build_1_files.size()) {
    if (++build_1_reader_index < 150) {
      ASSERT_NOT_OK(::parquet::arrow::FileReader::Make(
          pool, ::parquet::ParquetFileReader::Open(build_1_files[build_1_reader_index]),
          properties, &build_1_parquet_reader));
      ASSERT_NOT_OK(build_1_parquet_reader->GetRecordBatchReader(
          {0}, {0, 3}, &build_1_record_batch_reader));
    }
  } while (build_1_reader_index < 150);
  TIME_MICRO_OR_THROW(elapse_finish, expr_build_1->finish(&build_result_iterator_1));

  do {
    do {
      TIME_MICRO_OR_THROW(elapse_left_read,
                          build_2_record_batch_reader->ReadNext(&build_2_record_batch));
      if (build_2_record_batch) {
        TIME_MICRO_OR_THROW(elapse_eval, expr_build_2->evaluate(build_2_record_batch,
                                                                &dummy_result_batches));
        num_batches += 1;
        num_rows += build_2_record_batch->num_rows();
      }
    } while (build_2_record_batch);
    if (++build_2_reader_index < build_2_files.size()) {
      ASSERT_NOT_OK(::parquet::arrow::FileReader::Make(
          pool, ::parquet::ParquetFileReader::Open(build_2_files[build_2_reader_index]),
          properties, &build_2_parquet_reader));
      ASSERT_NOT_OK(build_2_parquet_reader->GetRecordBatchReader(
          {0}, {0, 2}, &build_2_record_batch_reader));
    }
  } while (build_2_reader_index < build_2_files.size());
  TIME_MICRO_OR_THROW(elapse_finish, expr_build_2->finish(&build_result_iterator_2));

  do {
    do {
      TIME_MICRO_OR_THROW(elapse_left_read,
                          build_3_record_batch_reader->ReadNext(&build_3_record_batch));
      if (build_3_record_batch) {
        TIME_MICRO_OR_THROW(elapse_eval, expr_build_3->evaluate(build_3_record_batch,
                                                                &dummy_result_batches));
        num_batches += 1;
        num_rows += build_3_record_batch->num_rows();
      }
    } while (build_3_record_batch);
    if (++build_3_reader_index < build_3_files.size()) {
      ASSERT_NOT_OK(::parquet::arrow::FileReader::Make(
          pool, ::parquet::ParquetFileReader::Open(build_3_files[build_3_reader_index]),
          properties, &build_3_parquet_reader));
      ASSERT_NOT_OK(build_3_parquet_reader->GetRecordBatchReader(
          {0}, {0, 1}, &build_3_record_batch_reader));
    }
  } while (build_3_reader_index < build_3_files.size());
  TIME_MICRO_OR_THROW(elapse_finish, expr_build_3->finish(&build_result_iterator_3));

  probe_result_iterator->SetDependencies(
      {build_result_iterator_1, build_result_iterator_2, build_result_iterator_3});

  std::cout << "Loaded all build side tables, total numRows is " << num_rows
            << ", start to Join" << std::endl;
  num_batches = 0;
  num_rows = 0;
  uint64_t out_num_rows = 0;
  uint64_t num_output_batches = 0;
  std::shared_ptr<arrow::RecordBatch> out;
  do {
    do {
      TIME_MICRO_OR_THROW(elapse_right_read,
                          stream_record_batch_reader->ReadNext(&stream_record_batch));
      if (stream_record_batch) {
        std::vector<std::shared_ptr<arrow::Array>> stream_column_vector;
        for (int i = 0; i < stream_record_batch->num_columns(); i++) {
          stream_column_vector.push_back(stream_record_batch->column(i));
        }
        TIME_MICRO_OR_THROW(elapse_probe_process,
                            probe_result_iterator->Process(stream_column_vector, &out));
        num_batches += 1;
        num_rows += stream_record_batch->num_rows();
        num_output_batches++;
        out_num_rows += out->num_rows();
      }
    } while (stream_record_batch);
    // if (++stream_reader_index < stream_files.size()) {
    if (++stream_reader_index < 150) {
      ASSERT_NOT_OK(::parquet::arrow::FileReader::Make(
          pool, ::parquet::ParquetFileReader::Open(stream_files[stream_reader_index]),
          properties, &stream_parquet_reader));
      ASSERT_NOT_OK(stream_parquet_reader->GetRecordBatchReader(
          {0}, {0, 1, 3}, &stream_record_batch_reader));
    }
  } while (stream_reader_index < 150);
  std::cout << "Readed right table with " << num_batches << " batches from " << 150
            << " files." << std::endl;

  std::cout << "=========================================="
            << "\nBenchmarkArrowComputeWSCG processed " << num_rows << " rows"
            << "\noutput " << num_output_batches << " batches with " << out_num_rows
            << " rows"
            << "\nCodeGen took " << TIME_TO_STRING(elapse_gen)
            << "\nLeft Batch Read took " << TIME_TO_STRING(elapse_left_read)
            << "\nRight Batch Read took " << TIME_TO_STRING(elapse_right_read)
            << "\nLeft Table Hash Insert took " << TIME_TO_STRING(elapse_eval)
            << "\nMake Result Iterator took " << TIME_TO_STRING(elapse_finish)
            << "\nProbe and Shuffle took " << TIME_TO_STRING(elapse_probe_process) << "\n"
            << "===========================================" << std::endl;
}

}  // namespace codegen
}  // namespace sparkcolumnarplugin
