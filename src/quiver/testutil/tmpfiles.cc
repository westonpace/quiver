#include "quiver/testutil/tmpfiles.h"

#include <filesystem>
#include <iostream>
#include <memory>
#include <system_error>

namespace quiver::testutil {

constexpr int32_t kMaxTmpDirCreateAttempts = 1000;

class TemporaryFilesManagerImpl : public TemporaryFilesManager {
 public:
  void Init() {
    std::error_code err;
    for (int32_t i = 0; i < kMaxTmpDirCreateAttempts; i++) {
      std::filesystem::path tmp_path =
          std::filesystem::temp_directory_path() / ("quiver_tmp_" + std::to_string(i));
      if (std::filesystem::create_directory(tmp_path, err)) {
        this->tmp_path = tmp_path;
        return;
      }
    }
    std::cerr << "Failed to create temporary files directory after "
              << kMaxTmpDirCreateAttempts << " attempts: " << err.message() << std::endl;
  }

  ~TemporaryFilesManagerImpl() {
    std::error_code err;
    for (int32_t file_idx = 0; file_idx < tmp_file_count; file_idx++) {
      std::filesystem::path tmp_file_path = tmp_path / std::to_string(file_idx);
      if (!std::filesystem::remove(tmp_file_path, err)) {
        if (err != std::errc::no_such_file_or_directory) {
          std::cerr << "Failed to remove temporary file (" << tmp_file_path
                    << "): " << err.message() << std::endl;
        }
      }
    }
    if (!std::filesystem::remove(tmp_path)) {
      if (err != std::errc::no_such_file_or_directory) {
        std::cerr << "Failed to remove temporary files directory (" << tmp_path
                  << "): " << err.message() << std::endl;
      }
    }
  }

  std::string NewTemporaryFile() override {
    std::filesystem::path tmp_file_path = tmp_path / std::to_string(tmp_file_count++);
    return tmp_file_path.string();
  }

  static std::unique_ptr<TemporaryFilesManagerImpl> Create() {
    auto instance = std::make_unique<TemporaryFilesManagerImpl>();
    instance->Init();
    return instance;
  }

 private:
  std::filesystem::path tmp_path;
  int32_t tmp_file_count = 0;
};

TemporaryFilesManager& TemporaryFiles() {
  static std::unique_ptr<TemporaryFilesManagerImpl> temporary_files =
      TemporaryFilesManagerImpl::Create();
  return *temporary_files;
}

}  // namespace quiver::testutil
