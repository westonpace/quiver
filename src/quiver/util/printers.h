#include <fmt/core.h>
#include <fmt/format.h>

#include "quiver/core/array.h"

template <>
struct fmt::formatter<quiver::SimpleSchema> : formatter<std::string_view> {
  // Formats the point p using the parsed format specification (presentation)
  // stored in this formatter.
  template <typename FormatContext>
  auto format(const quiver::SimpleSchema& schema, FormatContext& ctx)
      -> format_context::iterator {
    return formatter<std::string_view>::format(schema.ToString(), ctx);
  }
};
