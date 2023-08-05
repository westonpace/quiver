
#pragma once

#include <string>

namespace quiver::testutil {

class TemporaryFilesManager {
 public:
  virtual std::string NewTemporaryFile() = 0;
};

TemporaryFilesManager& TemporaryFiles();

}  // namespace quiver::testutil
