#include "precompile/hash_map.h"

#include <arrow/compute/context.h>
#include <arrow/status.h>

#include <iostream>

#include "third_party/arrow/utils/hashing.h"
#include "third_party/sparsehash/sparse_hash_map.h"

namespace sparkcolumnarplugin {
namespace precompile {

#define TYPED_SPARSE_HASH_MAP_IMPL(TYPENAME, TYPE)                                      \
  class TYPENAME::Impl : public SparseHashMap<TYPE> {                                   \
   public:                                                                              \
    Impl(arrow::MemoryPool* pool) : SparseHashMap<TYPE>(pool) {}                        \
  };                                                                                    \
                                                                                        \
  TYPENAME::TYPENAME(arrow::MemoryPool* pool) { impl_ = std::make_shared<Impl>(pool); } \
  arrow::Status TYPENAME::GetOrInsert(const TYPE& value, void (*on_found)(int32_t),     \
                                      void (*on_not_found)(int32_t),                    \
                                      int32_t* out_memo_index) {                        \
    return impl_->GetOrInsert(value, on_found, on_not_found, out_memo_index);           \
  }                                                                                     \
  int32_t TYPENAME::GetOrInsertNull(void (*on_found)(int32_t),                          \
                                    void (*on_not_found)(int32_t)) {                    \
    return impl_->GetOrInsertNull(on_found, on_not_found);                              \
  }                                                                                     \
  int32_t TYPENAME::Get(const TYPE& value) { return impl_->Get(value); }                \
  int32_t TYPENAME::GetNull() { return impl_->GetNull(); }

TYPED_SPARSE_HASH_MAP_IMPL(Int32HashMap, int32_t)
TYPED_SPARSE_HASH_MAP_IMPL(Int64HashMap, int64_t)
TYPED_SPARSE_HASH_MAP_IMPL(UInt32HashMap, uint32_t)
TYPED_SPARSE_HASH_MAP_IMPL(UInt64HashMap, uint64_t)
TYPED_SPARSE_HASH_MAP_IMPL(FloatHashMap, float)
TYPED_SPARSE_HASH_MAP_IMPL(DoubleHashMap, double)
TYPED_SPARSE_HASH_MAP_IMPL(Date32HashMap, int32_t)
#undef TYPED_SPARSE_HASH_MAP_IMPL

#define TYPED_ARROW_HASH_MAP_IMPL(HASHMAPNAME, TYPENAME, TYPE)                         \
  using MemoTableType =                                                                \
      typename arrow::internal::HashTraits<arrow::TYPENAME>::MemoTableType;            \
  class HASHMAPNAME::Impl : public MemoTableType {                                     \
   public:                                                                             \
    Impl(arrow::MemoryPool* pool) : MemoTableType(pool) {}                             \
  };                                                                                   \
                                                                                       \
  HASHMAPNAME::HASHMAPNAME(arrow::MemoryPool* pool) {                                  \
    impl_ = std::make_shared<Impl>(pool);                                              \
  }                                                                                    \
  arrow::Status HASHMAPNAME::GetOrInsert(const TYPE& value, void (*on_found)(int32_t), \
                                         void (*on_not_found)(int32_t),                \
                                         int32_t* out_memo_index) {                    \
    return impl_->GetOrInsert(value, on_found, on_not_found, out_memo_index);          \
  }                                                                                    \
  int32_t HASHMAPNAME::GetOrInsertNull(void (*on_found)(int32_t),                      \
                                       void (*on_not_found)(int32_t)) {                \
    return impl_->GetOrInsertNull(on_found, on_not_found);                             \
  }                                                                                    \
  int32_t HASHMAPNAME::Get(const TYPE& value) { return impl_->Get(value); }            \
  int32_t HASHMAPNAME::GetNull() { return impl_->GetNull(); }

TYPED_ARROW_HASH_MAP_IMPL(StringHashMap, StringType, std::string)
#undef TYPED_ARROW_HASH_MAP_IMPL

}  // namespace precompile
}  // namespace sparkcolumnarplugin
