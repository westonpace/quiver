#include "quiver/util/tracing.h"

#include <chrono>
#include <cmath>
#include <iostream>

#include "quiver/util/logging_p.h"

namespace quiver::util {

namespace {

// At the moment we don't deal with user-defined categories so this should
// always be large enough
constexpr std::size_t kNumInitialCategorySlots = 1024;

std::vector<std::string>& CategoryLabels() {
  static std::vector<std::string> category_labels(kNumInitialCategorySlots);
  return category_labels;
}

int64_t& CurrentCategory() {
  thread_local int64_t category = -1;
  return category;
}

std::chrono::steady_clock::time_point& LastEventTime() {
  thread_local std::chrono::steady_clock::time_point last_event_time =
      std::chrono::steady_clock::time_point::min();
  return last_event_time;
}

Tracer*& CurrentTracer() {
  thread_local Tracer* tracer = nullptr;
  return tracer;
}

// It's a bit weird that we have both a tracer singleton and a thread local tracer.
// However, accessing a singleton is costlier than accessing a thread local and I want to
// leave it open for future non-singleton tracers anyways.
Tracer* TracerSingleton() {
  static Tracer tracer;
  return &tracer;
}

void InitializeCurrentCategory(int64_t initial_category) {
  DCHECK_EQ(CurrentCategory(), -1)
      << "Start tracing called but operation already in progress on thread";
  CurrentCategory() = initial_category;
  DCHECK_EQ(LastEventTime(), std::chrono::steady_clock::time_point::min());
  LastEventTime() = std::chrono::steady_clock::now();
}

void ClearCurrentCategory() {
  DCHECK_NE(CurrentCategory(), -1)
      << "Clear tracing called but no operation in progress on thread";
  CurrentCategory() = -1;
  DCHECK_NE(LastEventTime(), std::chrono::steady_clock::time_point::min());
  LastEventTime() = std::chrono::steady_clock::time_point::min();
}

void UpdateCurrentCategory(int64_t new_value) {
  DCHECK_NE(CurrentCategory(), -1)
      << "Update tracing called but no operation in progress on thread";
  CurrentCategory() = new_value;
  DCHECK_NE(LastEventTime(), std::chrono::steady_clock::time_point::min());
  LastEventTime() = std::chrono::steady_clock::now();
}

}  // namespace

TracerScope::TracerScope(Tracer* src) : src_(src) {}
TracerScope::TracerScope(TracerScope&& other) noexcept : src_(other.src_) {
  other.src_ = nullptr;
}
TracerScope& TracerScope::operator=(TracerScope&& other) noexcept {
  src_ = other.src_;
  other.src_ = nullptr;
  return *this;
}
TracerScope::~TracerScope() {
  if (src_ != nullptr) {
    src_->EndOperation();
  }
}

SpanScope::SpanScope(int64_t previous_activity) : previous_activity_(previous_activity) {}
SpanScope::~SpanScope() { Tracer::GetCurrent()->SwitchActivity(previous_activity_); }

Tracer::Tracer() : category_labels_(CategoryLabels()), counts_(category_labels_.size()) {}

TracerScope Tracer::StartOperation(int64_t initial_category) {
  InitializeCurrentCategory(initial_category);
  LastEventTime() = std::chrono::steady_clock::now();
  return {this};
}

void Tracer::SwitchActivity(int64_t new_category) {
  int64_t current = CurrentCategory();
  auto now = std::chrono::steady_clock::now();
  std::chrono::steady_clock::duration time_since_last_event = now - LastEventTime();
  LastEventTime() = now;
  UpdateCurrentCategory(new_category);
  counts_[current] += time_since_last_event.count();
}

SpanScope Tracer::ScopeActivity(int64_t new_category) {
  int64_t current = CurrentCategory();
  auto now = std::chrono::steady_clock::now();
  std::chrono::steady_clock::duration time_since_last_event = now - LastEventTime();
  LastEventTime() = now;
  UpdateCurrentCategory(new_category);
  counts_[current] += time_since_last_event.count();
  return {current};
}

void Tracer::EndOperation() {
  int64_t current = CurrentCategory();
  auto now = std::chrono::steady_clock::now();
  std::chrono::steady_clock::duration time_since_last_event = now - LastEventTime();
  LastEventTime() = now;
  ClearCurrentCategory();
  counts_[current] += time_since_last_event.count();
}

Tracer* Tracer::GetCurrent() {
  Tracer* current = CurrentTracer();
  DCHECK_NE(current, nullptr);
  return current;
}

void Tracer::SetCurrent(Tracer* tracer) { CurrentTracer() = tracer; }

void Tracer::RegisterCategory(int64_t index, std::string_view label) {
  DCHECK_LT(index, static_cast<int64_t>(CategoryLabels().size()));
  if (CategoryLabels()[index].empty()) {
    CategoryLabels()[index] = label;
  } else {
    DCHECK_EQ(CategoryLabels()[index], label)
        << "RegisterCategory called multiple times with different labels";
  }
}

Tracer* Tracer::Singleton() { return TracerSingleton(); }

void Tracer::Clear() {
  for (auto& count : counts_) {
    count = 0;
  }
}

namespace {

int32_t MaxCategoryLabelLength(const std::vector<std::string>& labels) {
  int32_t max_length = 0;
  for (const auto& item : labels) {
    if (static_cast<int32_t>(item.size()) > max_length) {
      max_length = static_cast<int32_t>(item.size());
    }
  }
  return max_length;
}

int32_t NumDigits(uint64_t value) {
  return static_cast<int32_t>(log10(static_cast<double>(value))) + 1;
}

int32_t MaxCountLength(const std::vector<uint64_t>& counts) {
  int32_t max_length = 0;
  for (const auto count : counts) {
    if (NumDigits(count) > max_length) {
      max_length = NumDigits(count);
    }
  }
  return max_length;
}

std::string LeftPad(const std::string& input, int32_t length, char pad_char = ' ') {
  if (input.size() >= static_cast<std::size_t>(length)) {
    return input;
  }
  return std::string(length - input.size(), pad_char) + input;
}

std::string RightPad(const std::string& input, int32_t length, char pad_char = ' ') {
  if (input.size() >= static_cast<std::size_t>(length)) {
    return input;
  }
  return input + std::string(length - input.size(), pad_char);
}

uint64_t SumCounts(const std::vector<uint64_t>& counts) {
  uint64_t sum = 0;
  for (const auto count : counts) {
    sum += count;
  }
  return sum;
}

}  // namespace

void Tracer::Print() {
  int32_t max_label_length = MaxCategoryLabelLength(CategoryLabels());
  int32_t max_count_length = MaxCountLength(counts_);
  std::cout << RightPad("Category", max_label_length) << "| "
            << "Duration(ns)" << std::endl;
  for (std::size_t i = 0; i < counts_.size(); ++i) {
    if (counts_[i] > 0) {
      std::cout << RightPad(CategoryLabels()[i], max_label_length) << "| "
                << LeftPad(std::to_string(counts_[i]), max_count_length) << std::endl;
    }
  }
}

void Tracer::PrintHistogram(int32_t width) {
  int32_t max_label_length = MaxCategoryLabelLength(CategoryLabels());
  uint64_t count_sum = SumCounts(counts_);
  auto hash_width = static_cast<int32_t>(
      std::round(static_cast<double>(count_sum) / static_cast<double>(width)));
  std::cout << RightPad("Category", max_label_length) << "| "
            << "Duration(# = " << hash_width << "ns)" << std::endl;
  for (std::size_t i = 0; i < counts_.size(); ++i) {
    if (counts_[i] > 0) {
      double bar_width_dbl =
          (static_cast<double>(counts_[i]) / static_cast<double>(count_sum)) * width;
      auto bar_width = static_cast<int32_t>(std::round(bar_width_dbl));
      std::cout << RightPad(CategoryLabels()[i], max_label_length) << "| "
                << std::string(bar_width, '#') << std::endl;
    }
  }
}

}  // namespace quiver::util
