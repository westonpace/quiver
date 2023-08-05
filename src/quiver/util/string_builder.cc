// SPDX-License-Identifier: Apache-2.0

// Adapted from Apache Arrow

#include "quiver/util/string_builder.h"

#include <memory>
#include <sstream>

namespace quiver::util::detail {

StringStreamWrapper::StringStreamWrapper()
    : sstream_(std::make_unique<std::ostringstream>()), ostream_(*sstream_) {}

StringStreamWrapper::~StringStreamWrapper() = default;

std::string StringStreamWrapper::str() { return sstream_->str(); }

}  // namespace quiver::util::detail
