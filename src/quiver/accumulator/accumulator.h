#pragma once

#include <functional>

#include "quiver/pch.h"

namespace quiver::accum {

class Accumulator {
 public:
  virtual ~Accumulator() = default;
  /// Insert an entire batch into the accumulator
  virtual Status InsertBatch(ReadOnlyBatch* batch) = 0;
  /// Insert a continuous range of rows into the accumulator
  virtual Status InsertRange(ReadOnlyBatch* batch, int64_t row_start = 0,
                             int64_t length = -1) = 0;

  /// Insert indexed rows into the accumulator
  virtual Status InsertIndexed(ReadOnlyBatch* batch,
                               std::span<const int32_t> indices) = 0;
  virtual Status InsertIndexed(ReadOnlyBatch* batch,
                               std::span<const int64_t> indices) = 0;

  /// Finishes the accumulation
  ///
  /// No methods should be called after this.
  virtual Status Finish() = 0;

  /// Creates an accumulator that accumulates batches in memory, up to a given
  /// batch size, and then emits them.
  ///
  /// The final emitted batch may occupy more memory than strictly needed.
  ///
  /// This accumulator will allocate all fixed-sized buffers up-front.
  static std::unique_ptr<Accumulator> FixedMemory(
      const SimpleSchema* schema, int64_t rows_per_batch,
      std::function<Status(std::unique_ptr<ReadOnlyBatch>)> emit);
};

}  // namespace quiver::accum
