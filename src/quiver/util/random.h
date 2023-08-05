#pragma once

#include "quiver/pch.h"

namespace quiver::util {

uint8_t RandByte(uint8_t min, uint8_t max);
int32_t RandInt(int32_t min, int32_t max);
int64_t RandLong(int64_t min, int64_t max);
void RandomBytes(std::span<uint8_t> span);
void RandomInts(std::span<int32_t> span);
void RandomLongs(std::span<int64_t> span);
void Seed(int seed);
void Shuffle(std::span<uint8_t> values);
void Shuffle(std::span<int32_t> values);
void Shuffle(std::span<int64_t> values);

}  // namespace quiver::util
