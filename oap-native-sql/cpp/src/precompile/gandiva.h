#pragma once

#include "third_party/gandiva/types.h"

int64_t castDATE(int32_t in) { return castDATE_date32(in); }
int64_t extractYear(int64_t millis) { return extractYear_timestamp(millis); }
