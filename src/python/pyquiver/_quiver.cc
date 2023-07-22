#include <pybind11/pybind11.h>

#include <iostream>
#include <memory>
#include <numeric>

#include "pybind11/pytypes.h"
#include "quiver/accumulator/accumulator.h"
#include "quiver/core/array.h"
#include "quiver/core/io.h"
#include "quiver/hash/hasher.h"
#include "quiver/hashtable/hashtable.h"
#include "quiver/map/hashmap.h"
#include "quiver/util/arrow_util.h"
#include "quiver/util/config.h"
#include "quiver/util/literals.h"
#include "quiver/util/status.h"
#include "quiver/util/tracing.h"
#include "quiver/util/uri.h"

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
  }
  if (pybind11::hasattr(pyarrow_thing, "_export_to_c")) {
    std::unique_ptr<quiver::ReadOnlyBatch> q_batch;
    ThrowNotOk(BatchFromPyarrow(pyarrow_thing, schema, &q_batch));
    out->push_back(std::move(q_batch));
    return quiver::Status::OK();
  }
  throw pybind11::type_error("Expected pyarrow.Table or pyarrow.RecordBatch");
}

pybind11::object BatchToPyarrow(quiver::ReadOnlyBatch&& batch) {
  quiver::util::OwnedArrowArray c_data_array = quiver::util::AllocateArrowArray();
  quiver::util::OwnedArrowSchema c_data_schema = quiver::util::AllocateArrowSchema();

  ThrowNotOk(batch.schema()->ExportToArrow(c_data_schema.get()));
  ThrowNotOk(std::move(batch).ExportToArrow(c_data_array.get()));

  auto c_data_array_ptr = reinterpret_cast<intptr_t>(c_data_array.get());
  auto c_data_schema_ptr = reinterpret_cast<intptr_t>(c_data_schema.get());

  pybind11::module pyarrow = pybind11::module::import("pyarrow");
  pybind11::object pa_record_batch_cls = pyarrow.attr("RecordBatch");
  auto import_fn = pa_record_batch_cls.attr("_import_from_c");

  return import_fn(c_data_array_ptr, c_data_schema_ptr);
}

quiver::util::Uri UriFromPython(const std::string& scheme, const std::string& path,
                                const pybind11::dict& query) {
  std::unordered_map<std::string, std::string> query_params;
  for (const auto& item : query) {
    if (!pybind11::isinstance<pybind11::str>(item.first) ||
        !pybind11::isinstance<pybind11::str>(item.second)) {
      throw pybind11::type_error("Expected string key/value in query parameters");
    }
    auto key_str = item.first.cast<pybind11::str>();
    auto val_str = item.second.cast<pybind11::str>();
    query_params[key_str] = val_str;
  }
  return quiver::util::Uri{scheme, path, std::move(query_params)};
}

quiver::util::TracerScope TraceOperation() {
  quiver::util::Tracer::SetCurrent(quiver::util::Tracer::Singleton());
  return quiver::util::Tracer::GetCurrent()->StartOperation(
      quiver::util::tracecat::kPythonBindings);
}

class CHashMap {
 public:
  CHashMap(const pybind11::handle& key_schema,
           const pybind11::handle& build_payload_schema,
           const pybind11::handle& probe_payload_schema, const std::string& sd_scheme,
           const std::string& sd_path, const pybind11::dict& sd_query) {
    quiver::util::Tracer::RegisterCategory(quiver::util::tracecat::kPythonBindings,
                                           "PythonBindings");
    ThrowNotOk(SchemaFromPyarrow(key_schema, &key_schema_));
    ThrowNotOk(SchemaFromPyarrow(build_payload_schema, &build_payload_schema_));
    ThrowNotOk(SchemaFromPyarrow(probe_payload_schema, &probe_payload_schema_));
    quiver::util::Uri storage_descriptor_uri =
        UriFromPython(sd_scheme, sd_path, sd_query);
    ThrowNotOk(quiver::Storage::FromSpecifier(storage_descriptor_uri, &storage_));
    build_schema_ =
        quiver::SimpleSchema::AllColumnsFrom(key_schema_, build_payload_schema_);
    probe_schema_ =
        quiver::SimpleSchema::AllColumnsFrom(key_schema_, probe_payload_schema_);
    join_schema_ =
        quiver::SimpleSchema::AllColumnsFrom(build_schema_, probe_payload_schema_);
    key_indices_.resize(key_schema_.num_fields());
    std::iota(key_indices_.begin(), key_indices_.end(), 0);
    build_payload_indices_.resize(build_payload_schema_.num_fields());
    std::iota(build_payload_indices_.begin(), build_payload_indices_.end(),
              key_schema_.num_fields());
    probe_payload_indices_.resize(probe_payload_schema_.num_fields());
    std::iota(probe_payload_indices_.begin(), probe_payload_indices_.end(),
              key_schema_.num_fields());
    std::unique_ptr<quiver::hash::Hasher> hasher =
        quiver::hash::Hasher::CreateIdentityHasher();
    std::unique_ptr<quiver::hashtable::HashTable> hashtable =
        quiver::hashtable::HashTable::MakeStl();
    ThrowNotOk(quiver::map::HashMap::Create(
        &key_schema_, &build_payload_schema_, &probe_payload_schema_, std::move(hasher),
        storage_.get(), std::move(hashtable), &hashmap_));
  }

  ~CHashMap() { std::cout << "Deleting hashmap" << std::endl; }

  void Insert(const pybind11::handle& batch) {
    auto op_scope = TraceOperation();
    std::vector<std::unique_ptr<quiver::ReadOnlyBatch>> q_batches;
    ThrowNotOk(BatchesFromPyarrow(batch, &build_schema_, &q_batches));
    for (const auto& q_batch : q_batches) {
      ThrowNotOk(hashmap_->InsertCombinedBatch(q_batch.get()));
    }
  }

  pybind11::object Lookup(const pybind11::handle& probe_keys_batch) {
    auto op_scope = TraceOperation();
    std::unique_ptr<quiver::ReadOnlyBatch> q_probe_keys_batch;
    ThrowNotOk(BatchFromPyarrow(probe_keys_batch, &key_schema_, &q_probe_keys_batch));
    std::unique_ptr<quiver::Batch> q_result_batch =
        quiver::Batch::CreateBasic(&build_schema_);
    ThrowNotOk(hashmap_->Lookup(q_probe_keys_batch.get(), q_result_batch.get()));
    return BatchToPyarrow(std::move(*q_result_batch));
  }

  void InnerJoin(const pybind11::handle& batch, const pybind11::function& consume,
                 int32_t rows_per_batch) {
    auto op_scope = TraceOperation();
    std::vector<std::unique_ptr<quiver::ReadOnlyBatch>> q_batches;
    ThrowNotOk(BatchesFromPyarrow(batch, &probe_schema_, &q_batches));

    if (q_batches.size() != 1) {
      ThrowNotOk(quiver::Status::NotImplemented("Expected exactly one batch"));
    }

    const std::unique_ptr<quiver::ReadOnlyBatch>& q_batch = q_batches[0];
    std::unique_ptr<quiver::ReadOnlyBatch> q_key_batch =
        q_batch->SelectView(key_indices_, &key_schema_);
    std::unique_ptr<quiver::ReadOnlyBatch> q_payload_batch =
        q_batch->SelectView(probe_payload_indices_, &probe_payload_schema_);
    std::unique_ptr<quiver::Batch> q_result_batch =
        quiver::Batch::CreateBasic(&join_schema_);
    auto callback = [&](std::unique_ptr<quiver::ReadOnlyBatch> q_result_batch) {
      pybind11::object result_batch = BatchToPyarrow(std::move(*q_result_batch));
      consume(result_batch);
      return quiver::Status::OK();
    };
    ThrowNotOk(hashmap_->InnerJoin(q_key_batch.get(), q_payload_batch.get(),
                                   rows_per_batch, callback));
  }

 private:
  quiver::SimpleSchema key_schema_;
  quiver::SimpleSchema build_payload_schema_;
  quiver::SimpleSchema probe_payload_schema_;
  quiver::SimpleSchema build_schema_;
  quiver::SimpleSchema probe_schema_;
  quiver::SimpleSchema join_schema_;
  std::vector<int32_t> key_indices_;
  std::vector<int32_t> build_payload_indices_;
  std::vector<int32_t> probe_payload_indices_;
  std::unique_ptr<quiver::Storage> storage_;
  std::unique_ptr<quiver::map::HashMap> hashmap_;
};

class CAccumulator {
 public:
  CAccumulator(const pybind11::handle& schema, int32_t rows_per_batch,
               pybind11::function callback)
      : callback_(std::move(callback)) {
    quiver::util::Tracer::RegisterCategory(quiver::util::tracecat::kPythonBindings,
                                           "PythonBindings");
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
    auto op_scope = TraceOperation();
    std::vector<std::unique_ptr<quiver::ReadOnlyBatch>> q_batches;
    ThrowNotOk(BatchesFromPyarrow(batch, &schema_, &q_batches));
    for (const auto& q_batch : q_batches) {
      ThrowNotOk(accumulator_->InsertBatch(q_batch.get()));
    }
  }

  void Finish() {
    auto op_scope = TraceOperation();
    ThrowNotOk(accumulator_->Finish());
  }

 private:
  quiver::SimpleSchema schema_;
  std::unique_ptr<quiver::accum::Accumulator> accumulator_;
  pybind11::function callback_;
};

void ClearTracing() {
  quiver::util::Tracer::SetCurrent(quiver::util::Tracer::Singleton());
  quiver::util::Tracer::GetCurrent()->Clear();
}

void PrintTracing() {
  quiver::util::Tracer::SetCurrent(quiver::util::Tracer::Singleton());
  quiver::util::Tracer::GetCurrent()->Print();
}

void PrintTracingHistogram(int32_t width) {
  quiver::util::Tracer::SetCurrent(quiver::util::Tracer::Singleton());
  quiver::util::Tracer::GetCurrent()->PrintHistogram(width);
}

std::string Info() {
  return std::string("quiver ") +
         (quiver::util::config::IsDebug() ? " debug" : " release");
}

PYBIND11_MODULE(_quiver, mod) {
  // Clang format will mess up the formatting of the docstrings and so we disable it
  // clang-format off
  mod.doc() = "A set of unique collections for Arrow data";
  mod.def("_info", Info, "Returns information about the Quiver library");
  mod.def("_clear_tracing", ClearTracing, "Clears the tracing data");
  mod.def("_print_tracing", PrintTracing, "Prints the tracing data to stdout");
  mod.def("_print_tracing_histogram", PrintTracingHistogram, "Prints the tracing data, as a histogram, to stdout", pybind11::arg("width") = 40);
  pybind11::class_<CHashMap>(mod, "CHashMap")
      .def(pybind11::init<const pybind11::handle&, const pybind11::handle&, const pybind11::handle&, const std::string&, const std::string&, const pybind11::dict&>(), R"(
        Creates a HashMap with the given schemas

        A HashMap will encode batches of data into a hashtable.  The HashMap can
        then be queried in a number of different ways.
        
        The HashMap can be configured to act either as a map or a multi-map.
        When configured as a map then only the latest payload is kept for each
        combination of keys.
      )", pybind11::arg("key_schema"), pybind11::arg("build_payload_schema"), pybind11::arg("probe_payload_schema") = pybind11::none(), pybind11::arg("storage_descriptor_scheme") = "mem", pybind11::arg("storage_descriptor_path") = "", pybind11::arg("storage_descriptor_query") = pybind11::none())
      .def("insert", &CHashMap::Insert, R"(
        Inserts a batch of data into the hashmap.

        Parameters
        ----------

        batch : pyarrow.RecordBatch
            The batch to insert.  The schema should be the combined key + payload
            schema.  Each row will be inserted into the map.
      )", pybind11::arg("key_value_batch"))
      .def("lookup", &CHashMap::Lookup, R"(
        Given a batch of keys, returns the payloads that correspond to those keys.
      )", pybind11::arg("keys_batch"))
      .def("inner_join", &CHashMap::InnerJoin, R"(
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

        Parameters
        ----------
        batch : pyarrow.RecordBatch, pyarrow.Table
            A batch of data to join.  The batch must match the probe schema.  It
            should first have the key columns and then have the probe payload columns.
            The key columns will be used to lookup stored entries and these stored
            entries will be joined to the payload columns.
        callback : Callable[[pyarrow.RecordBatch], None]
            A function that will be called each time a new batch of data is generated
        rows_per_batch : int
            How many rows should the join accumulate before calling the callback.  The
            default, if not specified, will pick a value that should generate about
            16MB of data per batch.
      )", pybind11::arg("batch"), pybind11::arg("callback"),
          pybind11::arg("rows_per_batch") = -1);
  pybind11::class_<CAccumulator>(mod, "CAccumulator")
      .def(pybind11::init<const pybind11::handle&, int32_t, pybind11::function>(), R"(
            Creates an accumulator which will accumulate data into(typically larger)
            batches of a given size.This is faster than collecting all the batches
            in memory and concatenating them and can be used to accumulate small
            groups of rows into a larger batch.
      )", pybind11::arg("schema"), pybind11::arg("rows_per_batch"),
          pybind11::arg("callback"))
      .def("insert", &CAccumulator::Insert, R"(
            Inserts a batch of data into the accumulator.  If enough rows have accumulated
            then this will trigger a call to the callback.
           )", pybind11::arg("batch"))
      .def("finish", &CAccumulator::Finish, R"(
            Finishes the accumulation and triggers a call to the callback with the
            final (short) batch.
           )");
  // clang-format on
}