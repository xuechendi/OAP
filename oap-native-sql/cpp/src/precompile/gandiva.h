#pragma once

#include <math.h>

#include <cstdint>
#include <type_traits>

#include "third_party/gandiva/types.h"

int64_t castDATE(int32_t in) { return castDATE_date32(in); }
int64_t extractYear(int64_t millis) { return extractYear_timestamp(millis); }
template <typename T>
T round_2(T in, int precision = 2) {
  switch (precision) {
    case 0:
      return static_cast<T>(round(in));
    case 1:
      return static_cast<T>(round(in * 10.0) / 10.0);
    case 2:
      return static_cast<T>(round(in * 100.0) / 100.0);
    case 3:
      return static_cast<T>(round(in * 1000.0) / 1000.0);
    case 4:
      return static_cast<T>(round(in * 10000.0) / 10000.0);
    case 5:
      return static_cast<T>(round(in * 100000.0) / 1000000.0);
    default:
      return static_cast<T>(round(in * 1000000.0) / 1000000.0);
  }
}
