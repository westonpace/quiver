#include <pybind11/pybind11.h>

#include <memory>
#include <numeric>

#include "quiver/accumulator/accumulator.h"
#include "quiver/core/array.h"
#include "quiver/core/io.h"
#include "quiver/hash/hasher.h"
#include "quiver/hashtable/hashtable.h"
#include "quiver/map/hashmap.h"
#include "quiver/util/arrow_util.h"
#include "quiver/util/literals.h"
#include "quiver/util/status.h"

using namespace quiver::util::literals;

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

quiver::Status BatchesFromPyarrow(
    const pybind11::handle& pyarrow_thing, const quiver::SimpleSchema* schema,
    std::vector<std::unique_ptr<quiver::ReadOnlyBatch>>* out) {
  if (pybind11::hasattr(pyarrow_thing, "to_batches")) {
    auto to_batches_fn = pyarrow_thing.attr("to_batches");
    auto batches = to_batches_fn();

    for (auto batch : batches) {
      std::unique_ptr<quiver::ReadOnlyBatch> q_batch;
      ThrowNotOk(BatchFromPyarrow(batch, schema, &q_batch));
      out->push_back(std::move(q_batch));
    }
    return quiver::Status::OK();
  } else if (pybind11::hasattr(pyarrow_thing, "_export_to_c")) {
    std::unique_ptr<quiver::ReadOnlyBatch> q_batch;
    ThrowNotOk(BatchFromPyarrow(pyarrow_thing, schema, &q_batch));
    out->push_back(std::move(q_batch));
    return quiver::Status::OK();
  } else {
    throw pybind11::type_error("Expected pyarrow.Table or pyarrow.RecordBatch");
  }
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

class HashMap {
 public:
  HashMap(const pybind11::handle& key_schema, const pybind11::handle& payload_schema) {
    ThrowNotOk(SchemaFromPyarrow(key_schema, &key_schema_));
    ThrowNotOk(SchemaFromPyarrow(payload_schema, &payload_schema_));
    combined_schema_ = quiver::SimpleSchema::AllColumnsFrom(key_schema_, payload_schema_);
    key_indices_.resize(key_schema_.num_fields());
    std::iota(key_indices_.begin(), key_indices_.end(), 0);
    payload_indices_.resize(payload_schema_.num_fields());
    std::iota(payload_indices_.begin(), payload_indices_.end(), key_schema_.num_fields());
    scratch_.resize(1024 * 1024);
    std::span<uint8_t> scratch_span{scratch_.data(), scratch_.size()};
    sink_ = std::make_unique<quiver::StreamSink>(
        quiver::StreamSink::FromFixedSizeSpan(scratch_span));
    source_ = quiver::RandomAccessSource::FromSpan(scratch_span);
    std::unique_ptr<quiver::hash::Hasher> hasher =
        quiver::hash::Hasher::CreateIdentityHasher();
    std::unique_ptr<quiver::hashtable::HashTable> hashtable =
        quiver::hashtable::HashTable::MakeStl();
    ThrowNotOk(quiver::map::HashMap::Create(&key_schema_, &payload_schema_,
                                            std::move(hasher), sink_.get(), source_.get(),
                                            std::move(hashtable), &hashmap_));
  }

  void Insert(const pybind11::handle& batch) {
    std::vector<std::unique_ptr<quiver::ReadOnlyBatch>> q_batches;
    ThrowNotOk(BatchesFromPyarrow(batch, &combined_schema_, &q_batches));
    for (const auto& q_batch : q_batches) {
      ThrowNotOk(hashmap_->InsertCombinedBatch(q_batch.get()));
    }
  }

  pybind11::object Lookup(const pybind11::handle& probe_keys_batch) {
    std::unique_ptr<quiver::ReadOnlyBatch> q_probe_keys_batch;
    ThrowNotOk(BatchFromPyarrow(probe_keys_batch, &key_schema_, &q_probe_keys_batch));
    std::unique_ptr<quiver::Batch> q_result_batch =
        quiver::Batch::CreateBasic(&combined_schema_);
    ThrowNotOk(hashmap_->Lookup(q_probe_keys_batch.get(), q_result_batch.get()));
    return BatchToPyarrow(std::move(*q_result_batch));
  }

  void InnerJoin(const pybind11::handle& batch, const pybind11::function& consume,
                 int32_t rows_per_batch) {
    std::vector<std::unique_ptr<quiver::ReadOnlyBatch>> q_batches;
    ThrowNotOk(BatchesFromPyarrow(batch, &combined_schema_, &q_batches));

    if (q_batches.size() != 1) {
      ThrowNotOk(quiver::Status::NotImplemented("Expected exactly one batch"));
    }

    const std::unique_ptr<quiver::ReadOnlyBatch>& q_batch = q_batches[0];
    std::unique_ptr<quiver::ReadOnlyBatch> q_key_batch =
        q_batch->SelectView(key_indices_, &key_schema_);
    std::unique_ptr<quiver::ReadOnlyBatch> q_payload_batch =
        q_batch->SelectView(payload_indices_, &payload_schema_);
    std::unique_ptr<quiver::Batch> q_result_batch =
        quiver::Batch::CreateBasic(&combined_schema_);
    auto callback = [&](std::unique_ptr<quiver::ReadOnlyBatch> q_result_batch) {
      pybind11::object result_batch = BatchToPyarrow(std::move(*q_result_batch));
      consume(result_batch);
      return quiver::Status::OK();
    };
    ThrowNotOk(
        hashmap_->InnerJoin(q_key_batch.get(), q_payload_batch.get(), 1_MiLL, callback));
  }

 private:
  quiver::SimpleSchema key_schema_;
  quiver::SimpleSchema payload_schema_;
  quiver::SimpleSchema combined_schema_;
  std::vector<uint8_t> scratch_;
  std::vector<int32_t> key_indices_;
  std::vector<int32_t> payload_indices_;
  std::unique_ptr<quiver::StreamSink> sink_;
  std::unique_ptr<quiver::RandomAccessSource> source_;
  std::unique_ptr<quiver::map::HashMap> hashmap_;
};

class Accumulator {
 public:
  Accumulator(const pybind11::handle& schema, int32_t rows_per_batch,
              pybind11::function callback)
      : callback_(callback) {
    ThrowNotOk(SchemaFromPyarrow(schema, &schema_));
    auto q_callback = [this](std::unique_ptr<quiver::ReadOnlyBatch> q_batch) {
      pybind11::object batch = BatchToPyarrow(std::move(*q_batch));
      callback_(batch);
      return quiver::Status::OK();
    };
    accumulator_ =
        quiver::accum::Accumulator::FixedMemory(&schema_, rows_per_batch, q_callback);
  }

  void Insert(const pybind11::handle& batch) {
    std::vector<std::unique_ptr<quiver::ReadOnlyBatch>> q_batches;
    ThrowNotOk(BatchesFromPyarrow(batch, &schema_, &q_batches));
    for (const auto& q_batch : q_batches) {
      ThrowNotOk(accumulator_->InsertBatch(q_batch.get()));
    }
  }

  void Finish() { ThrowNotOk(accumulator_->Finish()); }

 private:
  quiver::SimpleSchema schema_;
  std::unique_ptr<quiver::accum::Accumulator> accumulator_;
  pybind11::function callback_;
};

PYBIND11_MODULE(pyquiver, mod) {
  // Clang format will mess up the formatting of the docstrings and so we disable it
  // clang-format off
  mod.doc() = "A set of unique collections for Arrow data";
  pybind11::class_<HashMap>(mod, "HashMap")
      .def(pybind11::init<const pybind11::handle&, const pybind11::handle&>(), R"(
        Creates a HashMap with the given key and payload schema.  TODO: document further
      )", pybind11::arg("key_schema"), pybind11::arg("payload_schema"))
      .def("insert", &HashMap::Insert, R"(
        Inserts a batch of data into the hashmap.

        Parameters
        ----------

        batch : pyarrow.RecordBatch
            The batch to insert.  The schema should be the combined key + payload
            schema.  Each row will be inserted into the map.
      )", pybind11::arg("key_value_batch"))
      .def("lookup", &HashMap::Lookup, R"(
        Given a batch of keys, returns the payloads that correspond to those keys.
      )", pybind11::arg("keys_batch"))
      .def("inner_join", &HashMap::InnerJoin, R"(
        Performs an inner_join on a batch of data.

        Each row of data will be searched in the map.  For each matching row stored
        in the map an output row will be generated that contains:

        Keys | Input Payload | Matching Row Payload

        There may be multiple matches and so there may be multiple output rows for
        each input row.  There may also be no matches and so there may be no output
        rows for a given input row.

        Since a single input row could generate potentially billions of output rows
        we cannot return a simple batch.  Instead we call a callback function for
        each batch of size `rows_per_batch` that is generated.
      )", pybind11::arg("batch"), pybind11::arg("callback"),
          pybind11::arg("rows_per_batch") = 1_MiLL);
  pybind11::class_<Accumulator>(mod, "Accumulator")
      .def(pybind11::init<const pybind11::handle&, int32_t, pybind11::function>(), R"(
            Creates an accumulator which will accumulate data into(typically larger)
            batches of a given size.This is faster than collecting all the batches
            in memory and concatenating them and can be used to accumulate small
            groups of rows into a larger batch.
      )", pybind11::arg("schema"), pybind11::arg("rows_per_batch"),
          pybind11::arg("callback"))
      .def("insert", &Accumulator::Insert, R"(
            Inserts a batch of data into the accumulator.  If enough rows have accumulated
            then this will trigger a call to the callback.
           )", pybind11::arg("batch"))
      .def("finish", &Accumulator::Finish, R"(
            Finishes the accumulation and triggers a call to the callback with the
            final (short) batch.
           )");
  // clang-format on
}