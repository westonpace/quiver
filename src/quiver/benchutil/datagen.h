// SPDX-License-Identifier: Apache-2.0

#pragma once

#include <arrow/status.h>

#include <cstdint>

#include "quiver/core/arrow.h"
#include "quiver/util/arrow_util.h"
namespace quiver::bench {

void AssertOk(const arrow::Status& status);
const std::shared_ptr<ArrowSchema>& GetFlatDataSchema();
util::OwnedArrowArray GenFlatData(int32_t target_num_bytes);

}  // namespace quiver::bench
