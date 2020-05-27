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

#include "codegen/arrow_compute/ext/codegen_common.h"

#include <dlfcn.h>
#include <fcntl.h>
#include <sys/file.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>

#include <fstream>
#include <iostream>
#include <sstream>

namespace sparkcolumnarplugin {
namespace codegen {
namespace arrowcompute {
namespace extra {

std::string BaseCodes() {
  return R"(
#include <arrow/array.h>
#include <arrow/array/builder_binary.h>
#include <arrow/array/builder_primitive.h>
#include <arrow/buffer.h>
#include <arrow/builder.h>
#include <arrow/compute/context.h>
#include <arrow/record_batch.h>
#include <arrow/type.h>
#include <arrow/type_traits.h>

#include <algorithm>
#include <iostream>

#include "codegen/arrow_compute/ext/array_item_index.h"
#include "codegen/arrow_compute/ext/code_generator_base.h"
#include "codegen/arrow_compute/ext/kernels_ext.h"
#include "codegen/common/result_iterator.h"
#include "third_party/arrow/utils/hashing.h"
#include "third_party/sparsehash/sparse_hash_map.h"

using namespace sparkcolumnarplugin::codegen::arrowcompute::extra;

)";
}

int FileSpinLock(std::string path) {
  std::string lockfile = path + "/nativesql_compile.lock";

  auto fd = open(lockfile.c_str(), O_CREAT, S_IRWXU | S_IRWXG);
  flock(fd, LOCK_EX);

  return fd;
}

void FileSpinUnLock(int fd) {
  flock(fd, LOCK_UN);
  close(fd);
}

std::string GetArrowTypeDefString(std::shared_ptr<arrow::DataType> type) {
  switch (type->id()) {
    case arrow::UInt8Type::type_id:
      return "uint8()";
    case arrow::Int8Type::type_id:
      return "int8()";
    case arrow::UInt16Type::type_id:
      return "uint16()";
    case arrow::Int16Type::type_id:
      return "int16()";
    case arrow::UInt32Type::type_id:
      return "uint32()";
    case arrow::Int32Type::type_id:
      return "int32()";
    case arrow::UInt64Type::type_id:
      return "uint64()";
    case arrow::Int64Type::type_id:
      return "int64()";
    case arrow::FloatType::type_id:
      return "float632()";
    case arrow::DoubleType::type_id:
      return "float64()";
    case arrow::Date32Type::type_id:
      return "date32()";
    case arrow::StringType::type_id:
      return "utf8()";
    default:
      std::cout << "GetTypeString can't convert " << type->ToString() << std::endl;
      throw;
  }
}
std::string GetCTypeString(std::shared_ptr<arrow::DataType> type) {
  switch (type->id()) {
    case arrow::UInt8Type::type_id:
      return "uint8_t";
    case arrow::Int8Type::type_id:
      return "int8_t";
    case arrow::UInt16Type::type_id:
      return "uint16_t";
    case arrow::Int16Type::type_id:
      return "int16_t";
    case arrow::UInt32Type::type_id:
      return "uint32_t";
    case arrow::Int32Type::type_id:
      return "int32_t";
    case arrow::UInt64Type::type_id:
      return "uint64_t";
    case arrow::Int64Type::type_id:
      return "int64_t";
    case arrow::FloatType::type_id:
      return "float";
    case arrow::DoubleType::type_id:
      return "double";
    case arrow::Date32Type::type_id:
      std::cout << "Can't handle Data32Type yet" << std::endl;
      throw;
    case arrow::StringType::type_id:
      return "std::string";
    default:
      std::cout << "GetTypeString can't convert " << type->ToString() << std::endl;
      throw;
  }
}
std::string GetTypeString(std::shared_ptr<arrow::DataType> type, std::string tail) {
  switch (type->id()) {
    case arrow::UInt8Type::type_id:
      return "UInt8" + tail;
    case arrow::Int8Type::type_id:
      return "Int8" + tail;
    case arrow::UInt16Type::type_id:
      return "UInt16" + tail;
    case arrow::Int16Type::type_id:
      return "Int16" + tail;
    case arrow::UInt32Type::type_id:
      return "UInt32" + tail;
    case arrow::Int32Type::type_id:
      return "Int32" + tail;
    case arrow::UInt64Type::type_id:
      return "UInt64" + tail;
    case arrow::Int64Type::type_id:
      return "Int64" + tail;
    case arrow::FloatType::type_id:
      return "Float" + tail;
    case arrow::DoubleType::type_id:
      return "Double" + tail;
    case arrow::Date32Type::type_id:
      return "Date32" + tail;
    case arrow::StringType::type_id:
      return "String" + tail;
    default:
      std::cout << "GetTypeString can't convert " << type->ToString() << std::endl;
      throw;
  }
}

arrow::Status CompileCodes(std::string codes, std::string signature) {
  // temporary cpp/library output files
  srand(time(NULL));
  std::string outpath = "/tmp";
  std::string prefix = "/spark-columnar-plugin-codegen-";
  std::string cppfile = outpath + prefix + signature + ".cc";
  std::string libfile = outpath + prefix + signature + ".so";
  std::string logfile = outpath + prefix + signature + ".log";
  std::ofstream out(cppfile.c_str(), std::ofstream::out);

  // output code to file
  if (out.bad()) {
    std::cout << "cannot open " << cppfile << std::endl;
    exit(EXIT_FAILURE);
  }
  out << codes;
  out.flush();
  out.close();

  // compile the code
  const char* env_gcc_ = std::getenv("CC");
  if (env_gcc_ == nullptr) {
    env_gcc_ = "gcc";
  }
  std::string env_gcc = std::string(env_gcc_);

  const char* env_arrow_dir = std::getenv("LIBARROW_DIR");
  const char* env_nativesql_dir = std::getenv("LIBSPARKSQLPLUGIN_DIR");
  std::string arrow_header;
  std::string nativesql_header;
  std::string arrow_lib;
  if (env_arrow_dir != nullptr) {
    arrow_header = " -I" + std::string(env_arrow_dir) + "/include ";
    arrow_lib = " -L" + std::string(env_arrow_dir) + "/lib64 ";
  }
  if (env_nativesql_dir != nullptr) {
    nativesql_header = " -I" + std::string(env_nativesql_dir) + " ";
  } else {
    // std::cout << "compilation failed, please export LIBSPARKSQLPLUGIN_DIR" <<
    // std::endl; exit(EXIT_FAILURE);
    nativesql_header = " -I/mnt/nvme2/chendi/intel-bigdata/OAP/oap-native-sql/cpp/src/ ";
  }
  auto sparsemap_header =
      nativesql_header.substr(0, (nativesql_header.size() - 1)) + "/third_party/ ";
  // compile the code
  std::string cmd = env_gcc + " -std=c++11 -Wall -Wextra " + arrow_header + arrow_lib +
                    nativesql_header + sparsemap_header + cppfile + " -o " + libfile +
                    " -O3 -shared -fPIC -larrow -lspark_columnar_jni 2> " + logfile;
  int ret = system(cmd.c_str());
  if (WEXITSTATUS(ret) != EXIT_SUCCESS) {
    std::cout << "compilation failed, see " << logfile << std::endl;
    exit(EXIT_FAILURE);
  }

  struct stat tstat;
  ret = stat(libfile.c_str(), &tstat);
  if (ret == -1) {
    std::cout << "stat failed: " << strerror(errno) << std::endl;
    exit(EXIT_FAILURE);
  }

  return arrow::Status::OK();
}

arrow::Status LoadLibrary(std::string signature, arrow::compute::FunctionContext* ctx,
                          std::shared_ptr<CodeGenBase>* out) {
  std::string outpath = "/tmp";
  std::string prefix = "/spark-columnar-plugin-codegen-";
  std::string libfile = outpath + prefix + signature + ".so";
  std::cout << "LoadLibrary " << libfile << std::endl;
  // load dynamic library
  void* dynlib = dlopen(libfile.c_str(), RTLD_LAZY);
  if (!dynlib) {
    return arrow::Status::Invalid(libfile, " is not generated");
  }

  // loading symbol from library and assign to pointer
  // (to be cast to function pointer later)

  void (*MakeCodeGen)(arrow::compute::FunctionContext * ctx,
                      std::shared_ptr<CodeGenBase> * out);
  *(void**)(&MakeCodeGen) = dlsym(dynlib, "MakeCodeGen");
  const char* dlsym_error = dlerror();
  if (dlsym_error != NULL) {
    std::stringstream ss;
    ss << "error loading symbol:\n" << dlsym_error << std::endl;
    return arrow::Status::Invalid(ss.str());
  }

  MakeCodeGen(ctx, out);
  return arrow::Status::OK();
}
}  // namespace extra
}  // namespace arrowcompute
}  // namespace codegen
}  // namespace sparkcolumnarplugin
