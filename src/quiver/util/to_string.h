// SPDX-License-Identifier: Apache-2.0

// Adapted from Apache Arrow

#pragma once

#include <memory>
#include <ostream>
#include <string>
#include <utility>

namespace quiver::util {

/// CRTP helper for declaring string representation. Defines operator<<
template <typename T>
class ToStringOstreamable {
 public:
  ~ToStringOstreamable() {
    static_assert(
        std::is_same<decltype(std::declval<const T>().ToString()), std::string>::value,
        "ToStringOstreamable depends on the method T::ToString() const");
  }

 private:
  [[nodiscard]] const T& cast() const { return static_cast<const T&>(*this); }

  friend inline std::ostream& operator<<(std::ostream& ostr,
                                         const ToStringOstreamable& value) {
    return ostr << value.cast().ToString();
  }
};

}  // namespace quiver::util
