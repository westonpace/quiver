// SPDX-License-Identifier: Apache-2.0

#pragma once

#include <cstdint>

#include "quiver/core/arrow.h"

namespace quiver::bench {

ArrowArray GenFlatData(int64_t target_num_bytes);

}
