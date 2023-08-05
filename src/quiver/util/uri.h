// SPDX-License-Identifier: Apache-2.0

#pragma once

#include <string>
#include <unordered_map>

namespace quiver::util {

// Quiver doesn't actually have any URI parsing so that we can avoid dependencies
// So instead of taking in a URI as a string we only accept parsed URIs and assume
// that callers can handle the parsing
struct Uri {
  std::string scheme;
  std::string path;
  std::unordered_map<std::string, std::string> query;

  std::string ToString() const;
};

}  // namespace quiver::util
