#include <gtest/gtest.h>

#include "quiver/util/config.h"
#include "quiver/util/tracing.h"

class QuiverEnvironment : public ::testing::Environment {
 public:
  ~QuiverEnvironment() override = default;

  void SetUp() override {
    quiver::util::config::SetLogLevel(quiver::util::config::LogLevel::kTrace);
    quiver::util::Tracer::RegisterCategory(quiver::util::tracecat::kUnitTest, "UnitTest");
    quiver::util::Tracer::SetCurrent(quiver::util::Tracer::Singleton());
    op_scope = quiver::util::Tracer::GetCurrent()->StartOperation(
        quiver::util::tracecat::kUnitTest);
  }

  void TearDown() override {
    op_scope.reset();
    quiver::util::Tracer::GetCurrent()->Print();
    quiver::util::Tracer::GetCurrent()->PrintHistogram();
  }

 private:
  std::optional<quiver::util::TracerScope> op_scope;
};

int main(int argc, char **argv) {
  ::testing::AddGlobalTestEnvironment(new QuiverEnvironment);
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}