#!/usr/bin/env bash

set -euo pipefail

fetch_orca_src () {
    local orca_tag="${1}"

    echo "Downloading greenplum-db/gporca@${orca_tag}"
    mkdir orca_src
    wget --quiet --output-document=- "https://github.com/greenplum-db/gporca/archive/${orca_tag}.tar.gz" \
        | tar xzf - --strip-components=1 --directory=orca_src
}

build_xerces () {
    local output_dir="${1}"

    echo "Building Xerces-C"
    # TODO this works when OUTPUT_DIR is a relative path but fails if an absolute path
    # TODO this does not work when the output dir is outside the current dir
    mkdir -p xerces_patch/concourse
    cp -r orca_src/concourse/xerces-c xerces_patch/concourse
    cp -r orca_src/patches/ xerces_patch
    /usr/bin/python xerces_patch/concourse/xerces-c/build_xerces.py --output_dir=${output_dir}
    rm -rf build
}

build_orca () {
    local output_dir="${1}"
    echo "Building orca"
    orca_src/concourse/build_and_test.py --build_type=RelWithDebInfo --output_dir=${output_dir} --skiptests
}

install_python () {
    echo "Installing python"
    tar xzf python-tarball/python-*.tar.gz -C /opt --strip-components=2
}

build_gpdb () {
    local build_dir
    build_dir="$(readlink -f "${1}")"

    local greenplum_install_dir="${2}"

    local include_dir="${build_dir}/include"
    local lib_dir="${build_dir}/lib"

    pushd gpdb_src
        # TODO this is gross
        # this is where the src/Makefile.global expects python
        (
        export LD_LIBRARY_PATH="/opt/python-2.7.12/lib"
        export PATH="/opt/python-2.7.12/bin:${PATH}"
        export PYTHONHOME=/opt/python-2.7.12
        CC="gcc" CFLAGS="-O3 -fargument-noalias-global -fno-omit-frame-pointer -g" \
            ./configure \
                --with-includes="${include_dir}" \
                --with-libraries="${lib_dir}" \
                --enable-orca \
                --with-zstd \
                --with-gssapi \
                --with-libxml \
                --with-perl \
                --with-python \
                --with-openssl \
                --with-pam \
                --with-ldap \
                --prefix="${greenplum_install_dir}" \
                --mandir="${greenplum_install_dir}/man"
        make -j
        make install)
    popd


    mkdir -p "${greenplum_install_dir}/etc"
    mkdir -p "${greenplum_install_dir}/include"
}

include_xerces () {
    local build_dir="${1}"
    local greenplum_install_dir="${2}"

    echo "Including libxerces-c in greenplum package"
    cp --archive ${build_dir}/lib/libxerces-c*.so ${greenplum_install_dir}/lib
}

include_python () {
    local greenplum_install_dir="${1}"

    # Create the python directory to flag to build scripts that python has been handled
    mkdir -p ${greenplum_install_dir}/ext/python
    echo "Copying python from /opt/python-2.7.12 into ${greenplum_install_dir}/ext/python..."
    cp --archive /opt/python-2.7.12/* ${greenplum_install_dir}/ext/python
}

include_libstdcxx () {
    local greenplum_install_dir="${1}"

    # if this is a platform that we use a non-system toolchain for, we need to
    # vendor libstdc++
    if [ -d /opt/gcc-6.4.0 ]; then
        cp --archive /opt/gcc-6.4.0/lib64/libstdc++.so.6{,.0.22} "${greemplum_install_dir}/lib"
    fi
}

include_zstd () {
    local greenplum_install_dir="${1}"
    local platform
    platform="$(. /etc/os-release; echo "${ID}")"

    local libdir
    case "${platform}" in
    centos) libdir=/usr/lib64 ;;
    ubuntu) libdir=/usr/lib ;;
    *) return ;;
    esac

    cp --archive ${libdir}/libzstd.so.1* "${greenplum_install_dir}/lib"
}

export_gpdb () {
    local greenplum_install_dir="${1}"
    local tarball="${2}"

#  TARBALL="${GPDB_ARTIFACTS_DIR}/${GPDB_BIN_FILENAME}"
  pushd ${greenplum_install_dir}
    (source greenplum_path.sh; python -m compileall -q -x test .)
    tar -czf "${tarball}" ./*
  popd
}

_main () {
    fetch_orca_src "${ORCA_TAG}"

    local build_dir="$(mktemp -d --tmpdir=.)"
    build_xerces "${build_dir}"
    build_orca "${build_dir}"

    install_python

    local greenplum_install_dir="/usr/local/greenplum-db-oss"
    build_gpdb "${build_dir}" "${greenplum_install_dir}"

    include_xerces "${build_dir}" "${greenplum_install_dir}"
    include_python "${greenplum_install_dir}"
    include_libstdcxx "${greenplum_install_dir}"
    include_zstd "${greenplum_install_dir}"
    export_gpdb "${greenplum_install_dir}" "${PWD}/output.tgz"

}

_main "${@}"
