<!---
  Licensed to the Apache Software Foundation (ASF) under one
  or more contributor license agreements.  See the NOTICE file
  distributed with this work for additional information
  regarding copyright ownership.  The ASF licenses this file
  to you under the Apache License, Version 2.0 (the
  "License"); you may not use this file except in compliance
  with the License.  You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

  Unless required by applicable law or agreed to in writing,
  software distributed under the License is distributed on an
  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
  KIND, either express or implied.  See the License for the
  specific language governing permissions and limitations
  under the License.
-->

# Apache Arrow

[![Fuzzing Status](https://oss-fuzz-build-logs.storage.googleapis.com/badges/arrow.svg)](https://bugs.chromium.org/p/oss-fuzz/issues/list?sort=-opened&can=1&q=proj:arrow)
[![License](http://img.shields.io/:license-Apache%202-blue.svg)](https://github.com/apache/arrow/blob/main/LICENSE.txt)
[![Twitter Follow](https://img.shields.io/twitter/follow/apachearrow.svg?style=social&label=Follow)](https://twitter.com/apachearrow)

## Intel IAA Modifications

In order to use arrow with the Intel IAA Accelerator, we need to build both arrow and QPL separately.

Arrow build instructions:
```
git clone https://github.com/apache/arrow.git
pushd arrow
git submodule update --init
export PARQUET_TEST_DATA="${PWD}/cpp/submodules/parquet-testing/data"
export ARROW_TEST_DATA="${PWD}/testing/data"
popd

mkdir dist

export ARROW_HOME=$(pwd)/dist
export LD_LIBRARY_PATH=$(pwd)/dist/lib:$LD_LIBRARY_PATH
export CMAKE_PREFIX_PATH=$ARROW_HOME:$CMAKE_PREFIX_PATH

export QPL_HOME=/home/raunaks3/qpl_library
export CMAKE_PREFIX_PATH=$QPL_HOME:$CMAKE_PREFIX_PATH

mkdir arrow/cpp/build
pushd arrow/cpp/build

cmake -DCMAKE_PREFIX_PATH=$CMAKE_PREFIX_PATH \
        -DCMAKE_INSTALL_PREFIX=$ARROW_HOME \
        -DCMAKE_INSTALL_LIBDIR=lib \
        -DCMAKE_BUILD_TYPE=Debug \
        -DARROW_BUILD_TESTS=ON \
        -DARROW_COMPUTE=ON \
        -DARROW_CSV=ON \
        -DARROW_DATASET=ON \
        -DARROW_FILESYSTEM=ON \
        -DARROW_HDFS=ON \
        -DARROW_JSON=ON \
        -DARROW_PARQUET=ON \
        -DARROW_WITH_BROTLI=ON \
        -DARROW_WITH_BZ2=ON \
        -DARROW_WITH_LZ4=ON \
        -DARROW_WITH_SNAPPY=ON \
        -DARROW_WITH_ZLIB=ON \
        -DARROW_WITH_ZSTD=ON \
        -DPARQUET_REQUIRE_ENCRYPTION=ON \
        -DARROW_EXTRA_ERROR_CONTEXT="ON" \
        ..

make -j8
sudo make install
popd

# If you want to use python later.
pushd arrow/python
export PYARROW_WITH_PARQUET=1
export PYARROW_WITH_DATASET=1
export PYARROW_PARALLEL=8
export PYARROW_WITH_PARQUET_ENCRYPTION=1
python setup.py build_ext --inplace
popd

```

For building QPL,
```
git clone --recursive https://github.com/intel/qpl.git ./qpl_library
cd qpl_library
mkdir build
cd build

mkdir ../qpl_installation
cmake -DCMAKE_BUILD_TYPE=Debug -DCMAKE_INSTALL_PREFIX=../qpl_installation ..
cmake --build . --target install

# To configure the IAA device (in case we are using hardware path):
sudo python3 /home/<USER>/qpl_library/qpl_installation/share/QPL/scripts/accel_conf.py --load=/home/<USER>/qpl_library/qpl_installation/share/QPL/configs/1n1d1e1w-s-n2.conf
```

The testing file is `arrow/cpp/examples/parquet/parquet_arrow/reader-writer.cc`.
It creates a table, writes it to disk as a parquet file using compression with QPL, and then reads and decompresses the file (also using QPL). Currently this is working with both the software path (no accelerator) and hardware path (IAA accelerator).

To test and run (note that if we change any source code in the main arrow repository we need to rebuild arrow before running the following):
```
cd arrow/cpp/examples/parquet/parquet_arrow
mkdir qpl_build
cd qpl_build
cmake ..
make
./parquet-compression-example
```

TODOs - 
1. Support QPL Codec - Currently the QPL codec has been implemented in place of the snappy codec. Ideally we want to create a new QPLCodec in a file `compression_qpl.cc` and support both snappy and QPL compression.
2. Test in different settings and benchmark performance

-----------------------------------------------

## Powering In-Memory Analytics

Apache Arrow is a development platform for in-memory analytics. It contains a
set of technologies that enable big data systems to process and move data fast.

Major components of the project include:

 - [The Arrow Columnar In-Memory Format](https://arrow.apache.org/docs/dev/format/Columnar.html):
   a standard and efficient in-memory representation of various datatypes, plain or nested
 - [The Arrow IPC Format](https://arrow.apache.org/docs/dev/format/Columnar.html#serialization-and-interprocess-communication-ipc):
   an efficient serialization of the Arrow format and associated metadata,
   for communication between processes and heterogeneous environments
 - [The Arrow Flight RPC protocol](https://github.com/apache/arrow/tree/main/format/Flight.proto):
   based on the Arrow IPC format, a building block for remote services exchanging
   Arrow data with application-defined semantics (for example a storage server or a database)
 - [C++ libraries](https://github.com/apache/arrow/tree/main/cpp)
 - [C bindings using GLib](https://github.com/apache/arrow/tree/main/c_glib)
 - [C# .NET libraries](https://github.com/apache/arrow/tree/main/csharp)
 - [Gandiva](https://github.com/apache/arrow/tree/main/cpp/src/gandiva):
   an [LLVM](https://llvm.org)-based Arrow expression compiler, part of the C++ codebase
 - [Go libraries](https://github.com/apache/arrow/tree/main/go)
 - [Java libraries](https://github.com/apache/arrow/tree/main/java)
 - [JavaScript libraries](https://github.com/apache/arrow/tree/main/js)
 - [Python libraries](https://github.com/apache/arrow/tree/main/python)
 - [R libraries](https://github.com/apache/arrow/tree/main/r)
 - [Ruby libraries](https://github.com/apache/arrow/tree/main/ruby)
 - [Rust libraries](https://github.com/apache/arrow-rs)

Arrow is an [Apache Software Foundation](https://www.apache.org) project. Learn more at
[arrow.apache.org](https://arrow.apache.org).

## What's in the Arrow libraries?

The reference Arrow libraries contain many distinct software components:

- Columnar vector and table-like containers (similar to data frames) supporting
  flat or nested types
- Fast, language agnostic metadata messaging layer (using Google's Flatbuffers
  library)
- Reference-counted off-heap buffer memory management, for zero-copy memory
  sharing and handling memory-mapped files
- IO interfaces to local and remote filesystems
- Self-describing binary wire formats (streaming and batch/file-like) for
  remote procedure calls (RPC) and interprocess communication (IPC)
- Integration tests for verifying binary compatibility between the
  implementations (e.g. sending data from Java to C++)
- Conversions to and from other in-memory data structures
- Readers and writers for various widely-used file formats (such as Parquet, CSV)

## Implementation status

The official Arrow libraries in this repository are in different stages of
implementing the Arrow format and related features.  See our current
[feature matrix](https://arrow.apache.org/docs/dev/status.html)
on git main.

## How to Contribute

Please read our latest [project contribution guide][5].

## Getting involved

Even if you do not plan to contribute to Apache Arrow itself or Arrow
integrations in other projects, we'd be happy to have you involved:

- Join the mailing list: send an email to
  [dev-subscribe@arrow.apache.org][1]. Share your ideas and use cases for the
  project.
- Follow our activity on [GitHub issues][3]
- [Learn the format][2]
- Contribute code to one of the reference implementations

[1]: mailto:dev-subscribe@arrow.apache.org
[2]: https://github.com/apache/arrow/tree/main/format
[3]: https://github.com/apache/arrow/issues
[4]: https://github.com/apache/arrow
[5]: https://arrow.apache.org/docs/dev/developers/contributing.html
