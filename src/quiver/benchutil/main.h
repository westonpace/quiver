#include <benchmark/benchmark.h>

#include <array>
#include <cstring>

#include "quiver/util/config.h"
#include "quiver/util/tracing.h"

#ifdef NDEBUG
#define QUIVER_BENCHMARK_MAIN()                                                \
  int main(int argc, char** argv) {                                            \
    quiver::util::Tracer::RegisterCategory(quiver::util::tracecat::kBenchmark, \
                                           "Benchmark");                       \
    quiver::util::Tracer::SetCurrent(quiver::util::Tracer::Singleton());       \
    auto trace_scope = quiver::util::Tracer::GetCurrent()->StartOperation(     \
        quiver::util::tracecat::kBenchmark);                                   \
                                                                               \
    ::benchmark::Initialize(&argc, argv);                                      \
    if (::benchmark::ReportUnrecognizedArguments(argc, argv)) {                \
      return 1;                                                                \
    }                                                                          \
    ::benchmark::RunSpecifiedBenchmarks();                                     \
    ::benchmark::Shutdown();                                                   \
    quiver::util::Tracer::GetCurrent()->PrintHistogram();                      \
    return 0;                                                                  \
  }                                                                            \
  int main(int, char**)
#else
#define QUIVER_BENCHMARK_MAIN()                                                \
  int main(int argc, char** argv) {                                            \
    quiver::util::config::SetLogLevel(quiver::util::config::LogLevel::kDebug); \
    quiver::util::Tracer::RegisterCategory(quiver::util::tracecat::kBenchmark, \
                                           "Benchmark");                       \
    quiver::util::Tracer::SetCurrent(quiver::util::Tracer::Singleton());       \
    auto trace_scope = quiver::util::Tracer::GetCurrent()->StartOperation(     \
        quiver::util::tracecat::kBenchmark);                                   \
                                                                               \
    ::benchmark::Initialize(&argc, argv);                                      \
    if (::benchmark::ReportUnrecognizedArguments(argc, argv)) {                \
      return 1;                                                                \
    }                                                                          \
    ::benchmark::RunSpecifiedBenchmarks();                                     \
    ::benchmark::Shutdown();                                                   \
    quiver::util::Tracer::GetCurrent()->PrintHistogram();                      \
    return 0;                                                                  \
  }                                                                            \
  int main(int, char**)
#endif