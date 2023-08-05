// SPDX-License-Identifier: Apache-2.0

#pragma once

#include <functional>

namespace quiver::util {

class Finally {
 public:
  template <typename Callable>
  Finally(Callable callable) : finalize_(std::move(callable)) {}
  Finally(const Finally& other) = delete;
  Finally& operator=(const Finally&) = delete;

  ~Finally() { finalize_(); }

 private:
  std::function<void()> finalize_;
};

}