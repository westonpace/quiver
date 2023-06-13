#include <pybind11/pybind11.h>

#include <memory>

#include "quiver/core/array.h"
#include "quiver/core/io.h"
#include "quiver/hash/hasher.h"
#include "quiver/hashtable/hashtable.h"
#include "quiver/map/hashmap.h"
#include "quiver/util/arrow_util.h"

// std::shared_ptr<arrow::Array> from_pyarrow(const pybind11::handle& array) {
//   if (!pybind11::hasattr(array, "_export_to_c")) {
//     throw pybind11::type_error("Expected pyarrow.Array");
//   }

//   ArrowArray c_data_array;
//   ArrowSchema c_data_schema;

//   intptr_t c_data_array_ptr = reinterpret_cast<intptr_t>(&c_data_array);
//   intptr_t c_data_schema_ptr = reinterpret_cast<intptr_t>(&c_data_schema);

//   auto export_fn = array.attr("_export_to_c");
//   export_fn(c_data_array_ptr, c_data_schema_ptr);

//   arrow::Result<std::shared_ptr<arrow::Array>> maybe_arr =
//       arrow::ImportArray(&c_data_array, &c_data_schema);

//   throw_not_ok(maybe_arr.status());

//   return maybe_arr.MoveValueUnsafe();
// }

void ThrowNotOk(const quiver::Status& status) {
  if (!status.ok()) {
    // TOOD: Emit appropriate exception based on status code
    throw std::runtime_error(status.ToString());
  }
}

quiver::Status SchemaFromPyarrow(const pybind11::handle& pyarrow_schema,
                                 quiver::SimpleSchema* out) {
  if (!pybind11::hasattr(pyarrow_schema, "_export_to_c")) {
    throw pybind11::type_error("Expected pyarrow.Schema");
  }

  quiver::util::OwnedArrowSchema c_data_schema = quiver::util::AllocateArrowSchema();
  auto c_data_schema_ptr = reinterpret_cast<intptr_t>(c_data_schema.get());

  auto export_fn = pyarrow_schema.attr("_export_to_c");
  export_fn(c_data_schema_ptr);

  return quiver::SimpleSchema::ImportFromArrow(c_data_schema.get(), out);
}

quiver::Status BatchFromPyarrow(const pybind11::handle& pyarrow_batch,
                                const quiver::SimpleSchema* schema,
                                std::unique_ptr<quiver::ReadOnlyBatch>* out) {
  if (!pybind11::hasattr(pyarrow_batch, "_export_to_c")) {
    throw pybind11::type_error("Expected pyarrow.RecordBatch");
  }

  quiver::util::OwnedArrowArray c_data_array = quiver::util::AllocateArrowArray();
  auto c_data_arr_ptr = reinterpret_cast<intptr_t>(c_data_array.get());

  auto export_fn = pyarrow_batch.attr("_export_to_c");
  export_fn(c_data_arr_ptr);

  return quiver::ImportBatch(c_data_array.get(), schema, out);
}

pybind11::object BatchToPyarrow(quiver::ReadOnlyBatch&& batch) {
  quiver::util::OwnedArrowArray c_data_array = quiver::util::AllocateArrowArray();
  quiver::util::OwnedArrowSchema c_data_schema = quiver::util::AllocateArrowSchema();

  ThrowNotOk(batch.schema()->ExportToArrow(c_data_schema.get()));
  ThrowNotOk(std::move(batch).ExportToArrow(c_data_array.get()));

  intptr_t c_data_array_ptr = reinterpret_cast<intptr_t>(c_data_array.get());
  intptr_t c_data_schema_ptr = reinterpret_cast<intptr_t>(c_data_schema.get());

  pybind11::module pyarrow = pybind11::module::import("pyarrow");
  pybind11::object pa_record_batch_cls = pyarrow.attr("RecordBatch");
  auto import_fn = pa_record_batch_cls.attr("_import_from_c");

  return import_fn(c_data_array_ptr, c_data_schema_ptr);
}

// pybind11::object to_pyarrow(const arrow::Array& array) {
//   ArrowArray c_data_array;
//   ArrowSchema c_data_schema;

//   throw_not_ok(arrow::ExportArray(array, &c_data_array, &c_data_schema));

//   pybind11::module pyarrow = pybind11::module::import("pyarrow");
//   pybind11::object pa_array_cls = pyarrow.attr("Array");
//   auto import_fn = pa_array_cls.attr("_import_from_c");

//   intptr_t c_data_array_ptr = reinterpret_cast<intptr_t>(&c_data_array);
//   intptr_t c_data_schema_ptr = reinterpret_cast<intptr_t>(&c_data_schema);

//   return import_fn(c_data_array_ptr, c_data_schema_ptr);
// }

class HashMap {
 public:
  HashMap(const pybind11::handle& key_schema, const pybind11::handle& payload_schema) {
    ThrowNotOk(SchemaFromPyarrow(key_schema, &key_schema_));
    ThrowNotOk(SchemaFromPyarrow(payload_schema, &payload_schema_));
    combined_schema_ = quiver::SimpleSchema::AllColumnsFrom(key_schema_, payload_schema_);
    scratch_.resize(1024 * 1024);
    std::span<uint8_t> scratch_span{scratch_.data(), scratch_.size()};
    sink_ = std::make_unique<quiver::StreamSink>(
        quiver::StreamSink::FromFixedSizeSpan(scratch_span));
    source_ = std::make_unique<quiver::RandomAccessSource>(
        quiver::RandomAccessSource::WrapSpan(scratch_span));
    std::unique_ptr<quiver::hash::Hasher> hasher =
        quiver::hash::Hasher::CreateIdentityHasher();
    std::unique_ptr<quiver::hashtable::HashTable> hashtable =
        quiver::hashtable::HashTable::MakeStl();
    ThrowNotOk(quiver::map::HashMap::Create(&key_schema_, &payload_schema_,
                                            std::move(hasher), sink_.get(), source_.get(),
                                            std::move(hashtable), &hashmap_));
  }

  void Insert(const pybind11::handle& key_batch, const pybind11::handle& payload_batch) {
    std::unique_ptr<quiver::ReadOnlyBatch> q_key_batch;
    ThrowNotOk(BatchFromPyarrow(key_batch, &key_schema_, &q_key_batch));
    std::unique_ptr<quiver::ReadOnlyBatch> q_payload_batch;
    ThrowNotOk(BatchFromPyarrow(payload_batch, &payload_schema_, &q_payload_batch));
    ThrowNotOk(hashmap_->Insert(q_key_batch.get(), q_payload_batch.get()));
  }

  pybind11::object Lookup(const pybind11::handle& probe_keys_batch) {
    std::unique_ptr<quiver::ReadOnlyBatch> q_probe_keys_batch;
    ThrowNotOk(BatchFromPyarrow(probe_keys_batch, &key_schema_, &q_probe_keys_batch));
    std::unique_ptr<quiver::Batch> q_result_batch =
        quiver::Batch::CreateBasic(&combined_schema_);
    ThrowNotOk(hashmap_->Lookup(q_probe_keys_batch.get(), q_result_batch.get()));
    return BatchToPyarrow(std::move(*q_result_batch));
  }

 private:
  quiver::SimpleSchema key_schema_;
  quiver::SimpleSchema payload_schema_;
  quiver::SimpleSchema combined_schema_;
  std::vector<uint8_t> scratch_;
  std::unique_ptr<quiver::StreamSink> sink_;
  std::unique_ptr<quiver::RandomAccessSource> source_;
  std::unique_ptr<quiver::map::HashMap> hashmap_;
};

int run_udf(int lhs, int rhs) { return lhs + rhs; }

PYBIND11_MODULE(pyquiver, mod) {
  mod.doc() = "An example module";
  mod.def("run_udf", &run_udf,
          "A function that does some transformation of a pyarrow array");
  pybind11::class_<HashMap>(mod, "HashMap")
      .def(pybind11::init<const pybind11::handle&, const pybind11::handle&>())
      .def("insert", &HashMap::Insert)
      .def("lookup", &HashMap::Lookup);
}