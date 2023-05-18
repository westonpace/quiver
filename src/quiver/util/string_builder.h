// SPDX-License-Identifier: Apache-2.0

// Adapted from Apache Arrow

#pragma once

#include <memory>
#include <ostream>
#include <string>
#include <utility>

namespace quiver::util {

namespace detail {

class StringStreamWrapper {
 public:
  StringStreamWrapper();
  ~StringStreamWrapper();

  std::ostream& stream() { return ostream_; }
  std::string str();

 private:
  std::unique_ptr<std::ostringstream> sstream_;
  std::ostream& ostream_;
};

}  // namespace detail

template <typename Head>
void StringBuilderRecursive(std::ostream& stream, Head&& head) {
  stream << head;
}

template <typename Head, typename... Tail>
void StringBuilderRecursive(std::ostream& stream, Head&& head, Tail&&... tail) {
  StringBuilderRecursive(stream, std::forward<Head>(head));
  StringBuilderRecursive(stream, std::forward<Tail>(tail)...);
}

template <typename... Args>
std::string StringBuilder(Args&&... args) {
  detail::StringStreamWrapper sstream;
  StringBuilderRecursive(sstream.stream(), std::forward<Args>(args)...);
  return sstream.str();
}

}  // namespace quiver::util
