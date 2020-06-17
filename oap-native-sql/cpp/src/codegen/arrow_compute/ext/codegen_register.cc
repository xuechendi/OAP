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

#include "codegen/arrow_compute/ext/codegen_register.h"

#include <gandiva/node.h>

#include <iostream>

namespace sparkcolumnarplugin {
namespace codegen {
namespace arrowcompute {
namespace extra {
std::string CodeGenRegister::GetFingerprint() { return fp_; }
arrow::Status CodeGenRegister::Visit(const gandiva::FunctionNode& node) {
  std::stringstream ss;
  ss << node.descriptor()->return_type()->ToString() << " " << node.descriptor()->name()
     << " (";
  bool skip_comma = true;
  for (auto& child : node.children()) {
    std::shared_ptr<CodeGenRegister> node;
    RETURN_NOT_OK(MakeCodeGenRegister(child, &node));
    if (skip_comma) {
      ss << node->GetFingerprint();
      skip_comma = false;
    } else {
      ss << ", " << node->GetFingerprint();
    }
  }
  ss << ")";
  fp_ = ss.str();

  return arrow::Status::OK();
}

arrow::Status CodeGenRegister::Visit(const gandiva::FieldNode& node) {
  fp_ = "(" + node.field()->type()->ToString() + ") ";
  return arrow::Status::OK();
}

arrow::Status CodeGenRegister::Visit(const gandiva::IfNode& node) {
  return arrow::Status::OK();
}

arrow::Status CodeGenRegister::Visit(const gandiva::LiteralNode& node) {
  fp_ = node.ToString();
  return arrow::Status::OK();
}

arrow::Status CodeGenRegister::Visit(const gandiva::BooleanNode& node) {
  return arrow::Status::OK();
}

arrow::Status CodeGenRegister::Visit(const gandiva::InExpressionNode<int>& node) {
  return arrow::Status::OK();
}

arrow::Status CodeGenRegister::Visit(const gandiva::InExpressionNode<long int>& node) {
  return arrow::Status::OK();
}

arrow::Status CodeGenRegister::Visit(const gandiva::InExpressionNode<std::string>& node) {
  return arrow::Status::OK();
}

}  // namespace extra
}  // namespace arrowcompute
}  // namespace codegen
}  // namespace sparkcolumnarplugin
