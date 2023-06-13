// SPDX-License-Identifier: Apache-2.0

// Adapted from Apache Arrow

// NOTE: failing must be inline in the macros below, to get correct file / line number
// reporting on test failures.

// NOTE: using a for loop for this macro allows extra failure messages to be
// appended with operator<<

// NOLINTBEGIN
#define ASSERT_RAISES(ENUM, expr)                                                   \
  for (const ::quiver::Status _st = ::quiver::internal::GenericToStatus((expr));    \
       !_st.Is##ENUM();)                                                            \
  FAIL() << "Expected '" QUIVER_STRINGIFY(expr) "' to fail with " QUIVER_STRINGIFY( \
                ENUM) ", but got "                                                  \
         << _st.ToString()

#define ASSERT_RAISES_WITH_MESSAGE(ENUM, message, expr)                                 \
  do {                                                                                  \
    auto _res = (expr);                                                                 \
    ::quiver::Status _st = ::quiver::internal::GenericToStatus(_res);                   \
    if (!_st.Is##ENUM()) {                                                              \
      FAIL() << "Expected '" QUIVER_STRINGIFY(expr) "' to fail with " QUIVER_STRINGIFY( \
                    ENUM) ", but got "                                                  \
             << _st.ToString();                                                         \
    }                                                                                   \
    ASSERT_EQ((message), _st.ToString());                                               \
  } while (false)

#define EXPECT_RAISES_WITH_MESSAGE_THAT(ENUM, matcher, expr)          \
  do {                                                                \
    auto _res = (expr);                                               \
    ::quiver::Status _st = ::quiver::internal::GenericToStatus(_res); \
    EXPECT_TRUE(_st.Is##ENUM())                                       \
        << "Expected '" QUIVER_STRINGIFY(expr) "' to fail with "      \
        << QUIVER_STRINGIFY(ENUM) ", but got " << _st.ToString();     \
    EXPECT_THAT(_st.ToString(), (matcher));                           \
  } while (false)

#define EXPECT_RAISES_WITH_CODE_AND_MESSAGE_THAT(code, matcher, expr) \
  do {                                                                \
    auto _res = (expr);                                               \
    ::quiver::Status _st = ::quiver::internal::GenericToStatus(_res); \
    EXPECT_EQ(_st.CodeAsString(), Status::CodeAsString(code));        \
    EXPECT_THAT(_st.ToString(), (matcher));                           \
  } while (false)

#define AssertOk(expr)                                                                 \
  for (::quiver::Status _st = ::quiver::internal::GenericToStatus((expr)); !_st.ok();) \
  FAIL() << "'" QUIVER_STRINGIFY(expr) "' failed with " << _st.ToString()

#define AssertOk_NO_THROW(expr) ASSERT_NO_THROW(AssertOk(expr))

#define QUIVER_EXPECT_OK(expr)                                           \
  do {                                                                   \
    auto _res = (expr);                                                  \
    ::quiver::Status _st = ::quiver::internal::GenericToStatus(_res);    \
    EXPECT_TRUE(_st.ok()) << "'" QUIVER_STRINGIFY(expr) "' failed with " \
                          << _st.ToString();                             \
  } while (false)

#define ASSERT_NOT_OK(expr)                                                           \
  for (::quiver::Status _st = ::quiver::internal::GenericToStatus((expr)); _st.ok();) \
  FAIL() << "'" QUIVER_STRINGIFY(expr) "' did not failed" << _st.ToString()

#define ABORT_NOT_OK(expr)                                            \
  do {                                                                \
    auto _res = (expr);                                               \
    ::quiver::Status _st = ::quiver::internal::GenericToStatus(_res); \
    if (QUIVER_PREDICT_FALSE(!_st.ok())) {                            \
      _st.Abort();                                                    \
    }                                                                 \
  } while (false);

#define ASSIGN_OR_HANDLE_ERROR_IMPL(handle_error, status_name, lhs, rexpr) \
  auto&& status_name = (rexpr);                                            \
  handle_error(status_name.status());                                      \
  lhs = std::move(status_name).ValueOrDie();

#define AssertOk_AND_ASSIGN(lhs, rexpr) \
  ASSIGN_OR_HANDLE_ERROR_IMPL(          \
      AssertOk, QUIVER_ASSIGN_OR_RAISE_NAME(_error_or_value, __COUNTER__), lhs, rexpr);

#define ASSIGN_OR_ABORT(lhs, rexpr)                                                      \
  ASSIGN_OR_HANDLE_ERROR_IMPL(ABORT_NOT_OK,                                              \
                              QUIVER_ASSIGN_OR_RAISE_NAME(_error_or_value, __COUNTER__), \
                              lhs, rexpr);

#define EXPECT_OK_AND_ASSIGN(lhs, rexpr)                                                 \
  ASSIGN_OR_HANDLE_ERROR_IMPL(QUIVER_EXPECT_OK,                                          \
                              QUIVER_ASSIGN_OR_RAISE_NAME(_error_or_value, __COUNTER__), \
                              lhs, rexpr);

#define AssertOk_AND_EQ(expected, expr)        \
  do {                                         \
    AssertOk_AND_ASSIGN(auto _actual, (expr)); \
    ASSERT_EQ(expected, _actual);              \
  } while (0)

// A generalized version of GTest's SCOPED_TRACE that takes arbitrary arguments.
//   QUIVER_SCOPED_TRACE("some variable = ", some_variable, ...)

#define QUIVER_SCOPED_TRACE(...) SCOPED_TRACE(::quiver::util::StringBuilder(__VA_ARGS__))
// NOLINTEND