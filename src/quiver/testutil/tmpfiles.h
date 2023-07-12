
#pragma once

#include <string>

namespace quiver::testing {

class TemporaryFilesManager {
 public:
  virtual std::string NewTemporaryFile() = 0;
};

TemporaryFilesManager& TemporaryFiles();

}  // namespace quiver::testing
