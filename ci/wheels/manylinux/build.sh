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

# Install dependencies
dnf install epel-release -y
dnf update -y
yum install -y spdlog-devel

PYTHONS="cp38-cp38 cp39-cp39 cp310-cp310 cp311-cp311"

# Compile wheels
for PYTHON in ${PYTHONS}; do
    PYBIN="/opt/python/${PYTHON}/bin"
    "${PYBIN}/pip" wheel . --no-deps -w ./wheelhouse/
done

# Bundle external shared libraries into the wheels
for whl in ./wheelhouse/*.whl; do
    repair_wheel "$whl"
done

# Install packages and test
for PYTHON in ${PYTHONS}; do
    PYBIN="/opt/python/${PYTHON}/bin"
    "${PYBIN}/pip" install pytest pyarrow
    "${PYBIN}/pip" install pyquiver --no-index -f ./wheelhouse
    "${PYBIN}/python" -mpytest ./src/python/tests
done
