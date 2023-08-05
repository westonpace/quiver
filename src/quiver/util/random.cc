#include "quiver/util/random.h"

#include <algorithm>
#include <cstring>
#include <random>

#include "quiver/pch.h"
#include "quiver/util/bit_util.h"

namespace quiver::util {
std::mt19937& Rng() {
  static std::mt19937 rng;
  return rng;
}

void Seed(int seed) { Rng().seed(seed); }

uint8_t RandByte(uint8_t min, uint8_t max) {
  std::uniform_int_distribution<uint16_t> dist(min, max);
  return static_cast<uint8_t>(dist(Rng()));
}

int32_t RandInt(int32_t min, int32_t max) {
  std::uniform_int_distribution<int32_t> dist(min, max);
  return dist(Rng());
}

int64_t RandLong(int64_t min, int64_t max) {
  std::uniform_int_distribution<int64_t> dist(min, max);
  return dist(Rng());
}

void RandomBytes(std::span<uint8_t> span) {
  uint64_t num_words = bit_util::CeilDiv(static_cast<int64_t>(span.size()),
                                         sizeof(std::mt19937::result_type));
  std::span<std::mt19937::result_type> words{
      reinterpret_cast<std::mt19937::result_type*>(span.data()), num_words};
  for (std::mt19937::result_type& word : words) {
    word = Rng()();
  }
  uint64_t num_remaining_bytes = span.size() % sizeof(std::mt19937::result_type);
  std::mt19937::result_type last_word = Rng()();
  std::memcpy(span.data() + num_words * sizeof(std::mt19937::result_type), &last_word,
              num_remaining_bytes);
}

void RandomInts(std::span<int32_t> span) {
  RandomBytes(std::span<uint8_t>(reinterpret_cast<uint8_t*>(span.data()),
                                 span.size() * sizeof(int32_t)));
}

void RandomLongs(std::span<int64_t> span) {
  RandomBytes(std::span<uint8_t>(reinterpret_cast<uint8_t*>(span.data()),
                                 span.size() * sizeof(int64_t)));
}

template <typename T>
void DoShuffle(std::span<T> values) {
  std::shuffle(values.begin(), values.end(), Rng());
}

void Shuffle(std::span<uint8_t> values) { return DoShuffle(values); }
void Shuffle(std::span<int32_t> values) { return DoShuffle(values); }
void Shuffle(std::span<int64_t> values) { return DoShuffle(values); }

}  // namespace quiver::util
