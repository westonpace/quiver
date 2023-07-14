// SPDX-License-Identifier: Apache-2.0

#pragma once

#include <string>
#include <vector>

namespace quiver::util {

class Tracer;

class TracerScope {
 public:
  TracerScope(Tracer* src);
  TracerScope(const TracerScope&) = delete;
  TracerScope(TracerScope&&) noexcept;
  TracerScope& operator=(const TracerScope&) = delete;
  TracerScope& operator=(TracerScope&&) noexcept;
  ~TracerScope();

 private:
  Tracer* src_;
};

class SpanScope {
 public:
  SpanScope(int64_t previous_activity);
  SpanScope(const SpanScope&) = delete;
  SpanScope(SpanScope&&) = delete;
  SpanScope& operator=(const SpanScope&) = delete;
  SpanScope& operator=(SpanScope&&) = delete;
  ~SpanScope();

 private:
  int64_t previous_activity_;
};

class Tracer {
 public:
  Tracer();
  Tracer(const Tracer&) = delete;
  Tracer(Tracer&&) = delete;
  Tracer& operator=(const Tracer&) = delete;
  Tracer& operator=(Tracer&&) = delete;

  [[nodiscard]] TracerScope StartOperation(int64_t initial_category);
  void SwitchActivity(int64_t new_category);
  [[nodiscard]] SpanScope ScopeActivity(int64_t new_category);

  void Clear();
  void Print();
  void PrintHistogram(int32_t width = 40);

  static Tracer* Singleton();
  static Tracer* GetCurrent();
  static void SetCurrent(Tracer* tracer);
  static void RegisterCategory(int64_t index, std::string_view label);

 private:
  void EndOperation();
  std::vector<std::string> category_labels_;
  std::vector<uint64_t> counts_;

  friend TracerScope;
};

namespace tracecat {
constexpr int64_t kAccumulatorInsert = 0;
constexpr int64_t kAccumulatorFinish = 1;
constexpr int64_t kHasherHash = 2;
constexpr int64_t kHashTableEncode = 3;
constexpr int64_t kHashTableDecode = 4;
constexpr int64_t kEqualityCompare = 5;
constexpr int64_t kHashMapInsert = 6;
constexpr int64_t kHashMapLookup = 7;
constexpr int64_t kHashMapInnerJoin = 8;
constexpr int64_t kRowEncoderEncode = 9;
constexpr int64_t kRowDecoderDecode = 10;
constexpr int64_t kPythonBindings = 11;
constexpr int64_t kUnitTest = 12;
constexpr int64_t kBenchmark = 13;
}  // namespace tracecat

}  // namespace quiver::util
