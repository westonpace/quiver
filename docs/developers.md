# Development Guidelines

## Style

We follow the Google C++ style guide with the following caveats:

 * We use `#pragma once` for include guards because we aim to keep the build simple
   enough that this always works.  It's technically non-standard but portable enough
   for our purposes

## Running Tools

The cmake build will detect if these tools are present and will issue a message if
they are not found.  If the tools are found then a target will be created to run the
tool.  We do not run these tools during normal compilation for performance reasons.

However, all of these tools should be run as part of CI and their passing will be
enforced.

 * `clang-format`
 * `clang-tidy`
 * `iwyu` - this tool is currently run as part of the normal build process but that
            should ideally be changed

## Running coverage

To generate a coverage report build the project with `QUIVER_CODE_COVERAGE=ON`
and then run the target `coverage`.  Details on how to run a target will
depend on which cmake generator you use but it is typically one of:

```
make coverage
ninja coverage
```

The coverage report will be an HTML report in the `coverage` directory in
your build directory.
