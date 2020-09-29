#pragma once

#include <arrow/util/decimal.h>
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
arrow::Decimal128 castDECIMAL(double in, int32_t precision, int32_t scale) {
  char str[40];
  char format[10];
  int32_t high;
  int32_t low;
  format[0] = '%';
  if (in > 0) {
    sprintf(&format[1], "+%d.%df", precision, scale);
  } else {
    sprintf(&format[1], "-%d.%df", precision, scale);
  }
  auto format_len = strlen(format);
  char* decimal_format = new char[format_len];
  memcpy(decimal_format, format, format_len);
  sprintf(str, decimal_format, in);
  int i = 0;
  while (str[i] == ' ') i++;
  int j = i;
  while (str[j] != ' ') j++;
  auto decimal_len = j - i;
  char* decimal_str = new char[decimal_len];
  memcpy(decimal_str, str + i, decimal_len);
  auto decimal_string = std::string(decimal_str);
  delete[] decimal_format;
  delete[] decimal_str;
  return arrow::Decimal128(decimal_string);
}

arrow::Decimal128 castDECIMAL(arrow::Decimal128 in, int32_t original_scale,
                              int32_t new_scale) {
  return in.Rescale(original_scale, new_scale).ValueOrDie();
}