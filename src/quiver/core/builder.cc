#include "quiver/core/builder.h"

#include <limits>
#include <memory>

#include "quiver/core/array.h"
#include "quiver/util/status.h"

namespace quiver {

class FieldBuilderImpl : public FieldBuilder {
 public:
  FieldBuilder& Name(std::string name) override {
    name_ = name;
    return *this;
  }

  std::string& name() { return name_; }

 private:
  std::string name_;
};

class FlatFieldBuilderImpl : public FieldBuilderImpl {
 public:
  explicit FlatFieldBuilderImpl(int32_t data_width_bytes)
      : data_width_bytes_(data_width_bytes) {}
  Status Finish(SimpleSchema* schema) override {
    if (schema->types.size() > std::numeric_limits<int32_t>::max()) {
      return Status::Invalid("Attempt to create schema with too many types");
    }
    FieldDescriptor field;
    field.format = "w:" + std::to_string(data_width_bytes_);
    field.name = name();
    field.metadata = "";
    field.nullable = true;
    field.dict_indices_ordered = false;
    field.map_keys_sorted = false;
    field.num_children = 0;
    field.index = static_cast<int32_t>(schema->types.size());
    field.layout = LayoutKind::kFlat;
    field.data_width_bytes = data_width_bytes_;
    field.schema = schema;

    schema->types.push_back(field);
    schema->top_level_types.push_back(field);
    schema->top_level_indices.push_back(field.index);
    return Status::OK();
  }

 private:
  int data_width_bytes_;
};

std::unique_ptr<FieldBuilder> FieldBuilder::Flat(int32_t width_bytes) {
  return std::make_unique<FlatFieldBuilderImpl>(width_bytes);
}

class SchemaBuilderImpl : public SchemaBuilder {
 public:
  SchemaBuilder& Field(std::unique_ptr<FieldBuilder> field) override {
    if (current_err_.has_value()) {
      return *this;
    }
    Status field_st = field->Finish(&schema_);
    if (!field_st.ok()) {
      current_err_ = field_st;
    }
    return *this;
  }

  SchemaBuilder& Fields(std::vector<std::unique_ptr<FieldBuilder>> fields) override {
    if (current_err_.has_value()) {
      return *this;
    }
    for (auto& field : fields) {
      Status field_st = field->Finish(&schema_);
      if (!field_st.ok()) {
        current_err_ = field_st;
        return *this;
      }
    }
    return *this;
  }

  Status Finish(SimpleSchema* out) override {
    if (current_err_) {
      return *current_err_;
    }
    *out = std::move(schema_);
    return Status::OK();
  }

 private:
  std::optional<Status> current_err_;
  SimpleSchema schema_;
};

std::unique_ptr<SchemaBuilder> SchemaBuilder::Start() {
  return std::make_unique<SchemaBuilderImpl>();
}

}  // namespace quiver
