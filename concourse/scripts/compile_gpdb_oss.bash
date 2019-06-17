#!/usr/bin/env bash

set -e
set -x

## TODO set these in task yaml
#ORCA_TAG="v3.48.0"
#
#echo "Downloading greenplum-db/gporca@${ORCA_TAG}"
#mkdir orca_src
#wget --quiet --output-document=- "https://github.com/greenplum-db/gporca/archive/${ORCA_TAG}.tar.gz" \
#    | tar xzf - --strip-components=1 --directory=orca_src
#
#echo "Building Xerces-C"
## TODO this works when OUTPUT_DIR is a relative path but fails if an absolute path
## TODO this does not work when the output dir is outside the current dir
#OUTPUT_DIR="$(mktemp -d --tmpdir=.)"
#mkdir -p xerces_patch/concourse
#cp -r orca_src/concourse/xerces-c xerces_patch/concourse
#cp -r orca_src/patches/ xerces_patch
#/usr/bin/python xerces_patch/concourse/xerces-c/build_xerces.py --output_dir=${OUTPUT_DIR}
#rm -rf build
#
#echo "Building orca"
#orca_src/concourse/build_and_test.py --build_type=RelWithDebInfo --output_dir=${OUTPUT_DIR} --skiptests
#
#echo "Installing python"
#tar xzf python-tarball/python-*.tar.gz -C ${OUTPUT_DIR} --strip-components=2
export OUTPUT_DIR="./tmp.7hmc717yXU"
# TODO this is gross
export LD_LIBRARY_PATH="$(readlink -f ${OUTPUT_DIR})/python-2.7.12/lib"
export PATH="$(readlink -f ${OUTPUT_DIR})/python-2.7.12/bin:${PATH}"

# TODO set with includes
INCLUDE_DIR="$(readlink -f ${OUTPUT_DIR})/include"
LIB_DIR="$(readlink -f ${OUTPUT_DIR})/lib"
INSTALL_DIR="/usr/local/greenplum-db-oss"
pushd gpdb_src
    CC="gcc" CFLAGS="-O3 -fargument-noalias-global -fno-omit-frame-pointer -g" \
        ./configure \
            --with-includes="${INCLUDE_DIR}" \
            --with-libraries="${LIB_DIR}" \
            --enable-orca \
            --with-zstd \
            --with-gssapi \
            --with-libxml \
            --with-perl \
            --with-python \
            --with-openssl \
            --with-pam \
            --with-ldap \
            --prefix="${INSTALL_DIR}" \
            --mandir="${INSTALL_DIR}/man"
    make -j
    make install
popd
