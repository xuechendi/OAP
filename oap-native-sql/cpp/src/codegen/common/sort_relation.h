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

#pragma once

#include <arrow/compute/context.h>
#include <arrow/status.h>
#include <arrow/type_fwd.h>

#include "codegen/arrow_compute/ext/array_item_index.h"
#include "codegen/common/relation_column.h"
#include "precompile/type_traits.h"

using sparkcolumnarplugin::codegen::arrowcompute::extra::ArrayItemIndexS;
using sparkcolumnarplugin::precompile::enable_if_number;
using sparkcolumnarplugin::precompile::enable_if_string_like;
using sparkcolumnarplugin::precompile::FixedSizeBinaryArray;
using sparkcolumnarplugin::precompile::StringArray;
using sparkcolumnarplugin::precompile::TypeTraits;

class SortRelation {
 public:
  SortRelation(
      uint64_t items_total, const std::vector<int>& size_array,
      const std::vector<std::shared_ptr<RelationColumn>>& sort_relation_key_list,
      const std::vector<std::shared_ptr<RelationColumn>>& sort_relation_payload_list)
      : items_total_(items_total),
        size_array_(size_array),
        num_arrays_(size_array.size()) {
    sort_relation_key_list_ = sort_relation_key_list;
    sort_relation_payload_list_ = sort_relation_payload_list;
  }

  ~SortRelation() {}

  ArrayItemIndexS GetItemIndexWithShift(int shift) {
    auto tmp_array_id = array_id_;
    auto tmp_id = id_;

    auto after = tmp_id + shift;
    while (after >= size_array_[tmp_array_id]) {
      after -= size_array_[tmp_array_id++];
    }
    return ArrayItemIndexS(tmp_array_id, after);
  }

  bool Next() {
    if (!CheckRangeBound(1)) return false;
    auto index = GetItemIndexWithShift(1);

    array_id_ = index.array_id;
    id_ = index.id;
    offset_++;
    range_cache_ = -1;

    return true;
  }

  bool NextNewKey() {
    auto range = GetSameKeyRange();
    if (!CheckRangeBound(range)) return false;
    auto index = GetItemIndexWithShift(range);

    array_id_ = index.array_id;
    id_ = index.id;
    offset_ += range;
    range_cache_ = -1;

    return true;
  }

  int GetSameKeyRange() {
    if (range_cache_ != -1) return range_cache_;
    int range = 0;
    bool is_same = true;
    while (is_same) {
      if (CheckRangeBound(range + 1)) {
        auto cur_idx = GetItemIndexWithShift(range);
        auto cur_idx_plus_one = GetItemIndexWithShift(range + 1);
        for (auto col : sort_relation_key_list_) {
          if (!(is_same = col->IsEqualTo(cur_idx.array_id, cur_idx.id,
                                         cur_idx_plus_one.array_id, cur_idx_plus_one.id)))
            break;
        }
      } else {
        is_same = false;
      }
      if (!is_same) break;
      range++;
    }
    range += 1;
    range_cache_ = range;
    return range;
  }

  bool CheckRangeBound(int shift) { return offset_ + shift < items_total_; }

  template <typename T>
  arrow::Status GetColumn(int idx, std::shared_ptr<T>* out) {
    *out = std::dynamic_pointer_cast<T>(sort_relation_payload_list_[idx]);
    return arrow::Status::OK();
  }

 protected:
  const uint64_t items_total_;
  uint64_t offset_ = 0;
  int array_id_ = 0;
  int id_ = 0;
  const int num_arrays_;
  const std::vector<int> size_array_;
  int range_cache_ = -1;
  std::vector<std::shared_ptr<RelationColumn>> sort_relation_key_list_;
  std::vector<std::shared_ptr<RelationColumn>> sort_relation_payload_list_;
};