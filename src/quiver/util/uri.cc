// SPDX-License-Identifier: Apache-2.0

#include "quiver/util/uri.h"

#include <sstream>

namespace quiver::util {

std::string Uri::ToString() const {
  std::stringstream stream;
  stream << scheme << "://" << path;
  if (query.empty()) {
    return stream.str();
  }
  bool first = true;
  for (const auto& [key, value] : query) {
    if (first) {
      stream << '?';
      first = false;
    } else {
      stream << '&';
    }
    stream << key << "=" << value;
  }
  return stream.str();
}

}  // namespace quiver::util
