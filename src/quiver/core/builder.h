// SPDX-License-Identifier: Apache-2.0

#pragma once

#include "quiver/core/array.h"

namespace quiver {

class FieldBuilder {
 public:
  virtual ~FieldBuilder() = default;
  virtual FieldBuilder& Name(std::string name) = 0;
  virtual Status Finish(SimpleSchema* schema) = 0;
  static std::unique_ptr<FieldBuilder> Flat(int width_bytes);
};

class SchemaBuilder {
 public:
  virtual ~SchemaBuilder() = default;
  virtual SchemaBuilder& Field(std::unique_ptr<FieldBuilder> field) = 0;
  virtual SchemaBuilder& Fields(std::vector<std::unique_ptr<FieldBuilder>> fields) = 0;
  virtual Status Finish(SimpleSchema* out) = 0;
  static std::unique_ptr<SchemaBuilder> Start();
};

}  // namespace quiver
