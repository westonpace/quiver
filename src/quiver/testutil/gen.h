#pragma once

#include "quiver/core/array.h"

namespace quiver::testutil {

/// <summary>
/// Randomizes the contents of an array, optionally resizing the array
/// </summary>
/// <param name="batch">The batch holding the array</param>
/// <param name="array_index">The index of the array to randomize</param>
/// <param name="array_size">The number of elements in the array, -1 to keep the original
/// size</param>
void RandomizeArray(Batch* batch, int array_index, int array_size = -1);

}  // namespace quiver::testutil
