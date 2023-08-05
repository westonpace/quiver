// SPDX-License-Identifier: Apache-2.0

// Adapted from Apache Arrow

#pragma once

#include <memory>
#include <type_traits>
#include <utility>

namespace quiver::util {

/// CRTP helper for declaring equality comparison. Defines operator== and operator!=
template <typename T>
class EqualityComparable {
 public:
  ~EqualityComparable() {
    static_assert(
        std::is_same<decltype(std::declval<const T>().Equals(std::declval<const T>())),
                     bool>::value,
        "EqualityComparable depends on the method T::Equals(const T&) const");
  }

  template <typename... Extra>
  bool Equals(const std::shared_ptr<T>& other, Extra&&... extra) const {
    if (other == nullptr) {
      return false;
    }
    return cast().Equals(*other, std::forward<Extra>(extra)...);
  }

  struct PtrsEqual {
    bool operator()(const std::shared_ptr<T>& left,
                    const std::shared_ptr<T>& right) const {
      return left->Equals(right);
    }
  };

  bool operator==(const T& other) const { return cast().Equals(other); }
  bool operator!=(const T& other) const { return !(cast() == other); }

 private:
  [[nodiscard]] const T& cast() const { return static_cast<const T&>(*this); }
};

}  // namespace quiver::util
