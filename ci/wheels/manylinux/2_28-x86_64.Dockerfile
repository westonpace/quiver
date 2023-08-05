FROM quay.io/pypa/manylinux_2_28_x86_64

COPY . /quiver

ENTRYPOINT /quiver/ci/wheels/manylinux/build.sh