#!/bin/bash
set -e -u -x

function repair_wheel {
    wheel="$1"
    if ! auditwheel show "$wheel"; then
        echo "Skipping non-platform wheel $wheel"
    else
        auditwheel repair "$wheel" --no-update-tags --plat manylinux_2_28_x86_64 -w ./wheelhouse/
    fi
}


# Install a system package required by our library
# yum install -y atlas-devel

# Compile wheels
for PYBIN in /opt/python/cp310-cp310/bin /opt/python/cp311-cp311/bin; do
    "${PYBIN}/pip" wheel . --no-deps -w ./wheelhouse/
done

# Bundle external shared libraries into the wheels
for whl in ./wheelhouse/*.whl; do
    repair_wheel "$whl"
done

# Install packages and test
for PYBIN in /opt/python/cp310-cp310/bin /opt/python/cp311-cp311/bin; do
    "${PYBIN}/pip" install quiver --no-index -f ./wheelhouse
    "${PYBIN}/pip" install pytest pyarrow
    "${PYBIN}/python" -mpytest ./src/python/tests
done
