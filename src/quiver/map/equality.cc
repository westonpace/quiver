#include "quiver/map/equality.h"

#include <cstring>

#include "quiver/util/logging_p.h"
#include "quiver/util/tracing.h"

namespace quiver::map {

class SimpleBinaryEqualityComparer : public EqualityComparer {
 public:
  SimpleBinaryEqualityComparer() {
    util::Tracer::RegisterCategory(util::tracecat::kEqualityCompare,
                                   "SimpleBinaryEqualityComparer::CompareEquality");
  }

  Status CompareEquality(ReadOnlyArray lhs, ReadOnlyArray rhs,
                         std::span<uint8_t> out) const override {
    auto trace_scope =
        util::Tracer::GetCurrent()->ScopeActivity(util::tracecat::kEqualityCompare);
    if (array::ArrayLayout(lhs) != array::ArrayLayout(rhs)) {
      AllFalse(out);
      return Status::OK();
    }
    switch (array::ArrayLayout(lhs)) {
      case LayoutKind::kFlat:
        CompareFlatEquality(std::get<ReadOnlyFlatArray>(lhs),
                            std::get<ReadOnlyFlatArray>(rhs), out);
        return Status::OK();
      default:
        return Status::NotImplemented("Equality comparison for layout ",
                                      layout::to_string(array::ArrayLayout(lhs)));
    }
  }

 private:
  static void AllFalse(std::span<uint8_t> out) { std::memset(out.data(), 0, out.size()); }
  static void CompareFlatEquality(ReadOnlyFlatArray lhs, ReadOnlyFlatArray rhs,
                                  std::span<uint8_t> out) {
    AllFalse(out);
    if (lhs.width_bytes != rhs.width_bytes) {
      return;
    }
    const uint8_t* lhs_val_itr = lhs.values.data();
    const uint8_t* rhs_val_itr = rhs.values.data();
    uint8_t* out_itr = out.data();
    int width_bytes = lhs.width_bytes;
    uint8_t bitmask = 1;
    for (int i = 0; i < lhs.length; i++) {
      *out_itr |= bitmask & (0xFF * static_cast<uint8_t>(memcmp(lhs_val_itr, rhs_val_itr,
                                                                width_bytes) == 0));
      lhs_val_itr += width_bytes;
      rhs_val_itr += width_bytes;
      bitmask <<= 1;
      if (bitmask == 0) {
        bitmask = 1;
        out_itr++;
      }
    }
    out_itr = out.data();
    for (uint64_t i = 0; i < out.size(); i++) {
      const uint8_t* lhs_valid_itr = lhs.validity.data();
      const uint8_t* rhs_valid_itr = rhs.validity.data();
      *out_itr &= ~(*lhs_valid_itr ^ *rhs_valid_itr);
    }
  }
};

std::unique_ptr<EqualityComparer> EqualityComparer::MakeSimpleBinaryEqualityComparer() {
  return std::make_unique<SimpleBinaryEqualityComparer>();
}

}  // namespace quiver::map