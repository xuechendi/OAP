#pragma once

#include <arrow/compute/context.h>
#include <arrow/status.h>
#include <gandiva/node.h>
#include <gandiva/node_visitor.h>
#include <iostream>

#include <memory>
#include <unordered_map>
#include "codegen/common/result_iterator.h"
#include "codegen/common/visitor_base.h"
#include "utils/macros.h"

namespace sparkcolumnarplugin {
namespace codegen {
namespace arrowcompute {

class ExprVisitor;
class BuilderVisitor;
class ExprVisitorImpl;

using ExprVisitorMap = std::unordered_map<std::string, std::shared_ptr<ExprVisitor>>;
using ArrayList = std::vector<std::shared_ptr<arrow::Array>>;
enum class ArrowComputeResultType {
  Array,
  ArrayList,
  Batch,
  BatchList,
  BatchIterator,
  None
};
enum class BuilderVisitorNodeType { FunctionNode, FieldNode };

class BuilderVisitor : public VisitorBase {
 public:
  BuilderVisitor(std::shared_ptr<arrow::Schema> schema_ptr,
                 std::shared_ptr<gandiva::Node> func, ExprVisitorMap* expr_visitor_cache)
      : schema_(schema_ptr), func_(func), expr_visitor_cache_(expr_visitor_cache) {}
  BuilderVisitor(std::shared_ptr<arrow::Schema> schema_ptr,
                 std::shared_ptr<gandiva::Node> func,
                 std::shared_ptr<gandiva::Node> finish_func,
                 ExprVisitorMap* expr_visitor_cache)
      : schema_(schema_ptr),
        func_(func),
        finish_func_(finish_func),
        expr_visitor_cache_(expr_visitor_cache) {}
  ~BuilderVisitor() {}
  arrow::Status Eval() {
    RETURN_NOT_OK(func_->Accept(*this));
    return arrow::Status::OK();
  }
  arrow::Status GetResult(std::shared_ptr<ExprVisitor>* out);
  std::string GetResult();
  BuilderVisitorNodeType GetNodeType() { return node_type_; }

 private:
  arrow::Status Visit(const gandiva::FunctionNode& node) override;
  arrow::Status Visit(const gandiva::FieldNode& node) override;
  // input
  std::shared_ptr<arrow::Schema> schema_;
  std::shared_ptr<gandiva::Node> func_;
  std::shared_ptr<gandiva::Node> finish_func_;
  // output
  std::shared_ptr<ExprVisitor> expr_visitor_;
  BuilderVisitorNodeType node_type_;
  // ExprVisitor Cache, used when multiple node depends on same node.
  ExprVisitorMap* expr_visitor_cache_;
  std::string node_id_;
};

class ExprVisitor : public std::enable_shared_from_this<ExprVisitor> {
 public:
  static arrow::Status Make(std::shared_ptr<arrow::Schema> schema_ptr,
                            std::string func_name,
                            std::vector<std::string> param_field_names,
                            std::shared_ptr<ExprVisitor> dependency,
                            std::shared_ptr<gandiva::Node> finish_func,
                            std::shared_ptr<ExprVisitor>* out);

  ExprVisitor(std::shared_ptr<arrow::Schema> schema_ptr, std::string func_name,
              std::vector<std::string> param_field_names,
              std::shared_ptr<ExprVisitor> dependency,
              std::shared_ptr<gandiva::Node> finish_func);

  ~ExprVisitor() {
#ifdef DEBUG
    std::cout << "Destruct " << func_name_ << " ExprVisitor, ptr is " << this
              << std::endl;
#endif
  }
  arrow::Status MakeExprVisitorImpl(const std::string& func_name, ExprVisitor* p);
  arrow::Status AppendAction(const std::string& func_name, const std::string& param_name);
  arrow::Status Init();
  arrow::Status Eval(const std::shared_ptr<arrow::RecordBatch>& in);
  arrow::Status Eval();
  arrow::Status SetMember(const std::shared_ptr<arrow::RecordBatch>& ms);
  arrow::Status SetDependency(
      const std::shared_ptr<ResultIterator<arrow::RecordBatch>>& dependency_iter,
      int index);
  arrow::Status GetResultFromDependency();
  arrow::Status Reset();
  arrow::Status ResetDependency();
  arrow::Status Finish(std::shared_ptr<ExprVisitor>* finish_visitor);
  arrow::Status MakeResultIterator(
      std::shared_ptr<arrow::Schema> schema,
      std::shared_ptr<ResultIterator<arrow::RecordBatch>>* out);
  std::string GetName() { return func_name_; }

  ArrowComputeResultType GetResultType();
  arrow::Status GetResult(std::shared_ptr<arrow::Array>* out,
                          std::vector<std::shared_ptr<arrow::Field>>* out_fields);
  arrow::Status GetResult(ArrayList* out,
                          std::vector<std::shared_ptr<arrow::Field>>* out_fields);
  arrow::Status GetResult(std::vector<ArrayList>* out, std::vector<int>* out_sizes,
                          std::vector<std::shared_ptr<arrow::Field>>* out_fields);

  void PrintMetrics() {
    if (dependency_) {
      dependency_->PrintMetrics();
    }
    std::cout << func_name_ << " took " << TIME_TO_STRING(elapse_time_) << ", ";
  }

  // Input data holder.
  std::shared_ptr<arrow::Schema> schema_;
  std::string func_name_;
  std::shared_ptr<ExprVisitor> dependency_;
  std::shared_ptr<arrow::RecordBatch> in_record_batch_;

  // For dual input kernels like probe
  std::shared_ptr<arrow::RecordBatch> member_record_batch_;

  std::vector<std::string> param_field_names_;
  std::shared_ptr<gandiva::Node> finish_func_;
  std::vector<std::string> action_name_list_;
  std::vector<std::string> action_param_list_;

  // Input data from dependency.
  ArrowComputeResultType dependency_result_type_ = ArrowComputeResultType::None;
  std::vector<std::shared_ptr<arrow::Field>> in_fields_;
  std::vector<ArrayList> in_batch_array_;
  std::vector<int> in_batch_size_array_;
  ArrayList in_batch_;
  ArrayList in_array_list_;
  std::shared_ptr<arrow::Array> in_array_;
  // group_indices is used to tell item in array_list_ and batch_list_ belong to which
  // group
  std::vector<int> group_indices_;

  // Output data types.
  ArrowComputeResultType return_type_ = ArrowComputeResultType::None;
  // This is used when we want to output an Array after evaluate.
  std::shared_ptr<arrow::Array> result_array_;
  // This is used when we want to output an ArrayList after evaluation.
  ArrayList result_array_list_;
  ArrayList result_batch_;
  // This is used when we want to output an BatchList after evaluation.
  std::vector<ArrayList> result_batch_list_;
  std::vector<int> result_batch_size_list_;
  // Return fields
  std::vector<std::shared_ptr<arrow::Field>> result_fields_;
  // This is used when we want to output an ResultIterator<RecordBatch>
  std::shared_ptr<ResultIterator<arrow::RecordBatch>> result_batch_iterator_;

  // Long live variables
  arrow::compute::FunctionContext ctx_;
  std::shared_ptr<ExprVisitorImpl> impl_;
  std::shared_ptr<ExprVisitor> finish_visitor_;
  bool initialized_ = false;

  // metrics, in microseconds
  uint64_t elapse_time_ = 0;

  arrow::Status GetResult(std::shared_ptr<arrow::Array>* out,
                          std::vector<std::shared_ptr<arrow::Field>>* out_fields,
                          std::vector<int>* group_indices);
  arrow::Status GetResult(ArrayList* out,
                          std::vector<std::shared_ptr<arrow::Field>>* out_fields,
                          std::vector<int>* group_indices);
  arrow::Status GetResult(std::vector<ArrayList>* out, std::vector<int>* out_sizes,
                          std::vector<std::shared_ptr<arrow::Field>>* out_fields,
                          std::vector<int>* group_indices);
};

arrow::Status MakeExprVisitor(std::shared_ptr<arrow::Schema> schema_ptr,
                              std::shared_ptr<gandiva::Expression> expr,
                              ExprVisitorMap* expr_visitor_cache,
                              std::shared_ptr<ExprVisitor>* out);

arrow::Status MakeExprVisitor(std::shared_ptr<arrow::Schema> schema_ptr,
                              std::shared_ptr<gandiva::Expression> expr,
                              std::shared_ptr<gandiva::Expression> finish_expr,
                              ExprVisitorMap* expr_visitor_cache,
                              std::shared_ptr<ExprVisitor>* out);

}  // namespace arrowcompute
}  // namespace codegen
}  // namespace sparkcolumnarplugin
