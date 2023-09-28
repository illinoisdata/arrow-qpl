// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

#include "arrow/util/compression_internal.h"

#include <iostream>
#include <fstream>
#include <cstddef>
#include <cstdint>
#include <memory>

#include <snappy.h>
#include <qpl/qpl.h>
#include "examples_utils.hpp"

#include "arrow/result.h"
#include "arrow/status.h"
#include "arrow/util/logging.h"
#include "arrow/util/macros.h"

using std::size_t;

namespace arrow {
namespace util {
namespace internal {

namespace {

// ----------------------------------------------------------------------
// Snappy implementation

class SnappyCodec : public Codec {
 public:
  Result<int64_t> Decompress(int64_t input_len, const uint8_t* input,
                             int64_t output_buffer_len, uint8_t* output_buffer) override {

    qpl_path_t execution_path = qpl_path_hardware;
    // constexpr const uint32_t source_size = 1000; // input_len
    // std::vector<uint8_t> source(source_size, 5);
    // std::vector<uint8_t> destination(source_size / 2, 4);
    // std::vector<uint8_t> reference(source_size, 7);

    std::unique_ptr<uint8_t[]> job_buffer;
    qpl_status                 status;
    uint32_t                   size = 0;
    // qpl_histogram              deflate_histogram{};

    // Job initialization
    status = qpl_get_job_size(execution_path, &size);
    if (status != QPL_STS_OK) {
        throw std::runtime_error("An error acquired during job size getting.");
    }
    else {
      std::cout << "It's working" << std::endl;
    }

    job_buffer = std::make_unique<uint8_t[]>(size);
    qpl_job *job = reinterpret_cast<qpl_job *>(job_buffer.get());
    status = qpl_init_job(execution_path, job);
    if (status != QPL_STS_OK) {
        throw std::runtime_error("An error acquired during compression job initializing.");
    }

    uint8_t* casted_input_ptr = const_cast<uint8_t*>(input);

    // Huffman table initialization
    // qpl_huffman_table_t huffman_table;

    // status = qpl_deflate_huffman_table_create(combined_table_type,
    //                                           execution_path,
    //                                           DEFAULT_ALLOCATOR_C,
    //                                           &huffman_table);
    // if (status != QPL_STS_OK) {
    //     throw std::runtime_error("An error acquired during Huffman table creation.");
    // }

    // status = qpl_gather_deflate_statistics(casted_input_ptr,
    //                                        input_len,
    //                                        &deflate_histogram,
    //                                        qpl_default_level,
    //                                        execution_path);
    // if (status != QPL_STS_OK) {
    //     throw std::runtime_error("An error acquired during gathering statistics for Huffman table.");
    // }

    // // Building the Huffman table
    // status = qpl_huffman_table_init_with_histogram(huffman_table, &deflate_histogram);
    // if (status != QPL_STS_OK) {
    //     throw std::runtime_error("An error acquired during Huffman table initialization.");
    // }

    // Performing a decompression operation
    job->op            = qpl_op_decompress;
    job->next_in_ptr   = casted_input_ptr;
    job->next_out_ptr  = output_buffer;
    job->available_in  = static_cast<uint32_t>(input_len);
    job->available_out = static_cast<uint32_t>(output_buffer_len);
    // job->flags         = QPL_FLAG_FIRST | QPL_FLAG_LAST | QPL_FLAG_CANNED_MODE;
    job->flags         = QPL_FLAG_FIRST | QPL_FLAG_LAST;
    // job->huffman_table = huffman_table;

    // Decompression
    status = qpl_execute_job(job);
    if (status != QPL_STS_OK) {
        throw std::runtime_error("An error while decompression occurred.");
    }

    status = qpl_fini_job(job);
    if (status != QPL_STS_OK) {
        throw std::runtime_error("An error acquired during job finalization.");
    }

    // const uint32_t decompressed_size = job->total_out;

    // std::ofstream compressionStats("/home/raunaks3/arrow/cpp/examples/parquet/parquet_arrow/qpl_build/compression_stats.txt");
    
    // if (compressionStats.is_open())
    //     compressionStats << "\t" << input_len << ", " << output_buffer_len << ", ";
    //     // compressionStats << decompressed_size << "\n";
    // else
    //     std::cout << "Something went wrong with opening the file!";
    
    // compressionStats.close();

    std::cout << input_len << std::endl;
    std::cout << output_buffer_len << std::endl;
    // std::cout << decompressed_size << std::endl;

    // return static_cast<int64_t>(decompressed_size);
    return output_buffer_len;

    // size_t decompressed_size;
    // std::cout << "Decompressing..." << std::endl;
    
    // if (!snappy::GetUncompressedLength(reinterpret_cast<const char*>(input),
    //                                    static_cast<size_t>(input_len),
    //                                    &decompressed_size)) {
    //   return Status::IOError("Corrupt snappy compressed data.");
    // }
    // if (output_buffer_len < static_cast<int64_t>(decompressed_size)) {
    //   return Status::Invalid("Output buffer size (", output_buffer_len, ") must be ",
    //                          decompressed_size, " or larger.");
    // }
    // if (!snappy::RawUncompress(reinterpret_cast<const char*>(input),
    //                            static_cast<size_t>(input_len),
    //                            reinterpret_cast<char*>(output_buffer))) {
    //   return Status::IOError("Corrupt snappy compressed data.");
    // }
    // return static_cast<int64_t>(decompressed_size);
  }

  int64_t MaxCompressedLen(int64_t input_len,
                           const uint8_t* ARROW_ARG_UNUSED(input)) override {
    DCHECK_GE(input_len, 0);
    return snappy::MaxCompressedLength(static_cast<size_t>(input_len));
  }

  // Result<int64_t> Compress(int64_t input_len, const uint8_t* input,
  //                          int64_t ARROW_ARG_UNUSED(output_buffer_len),
  //                          uint8_t* output_buffer) override {
  Result<int64_t> Compress(int64_t input_len, const uint8_t* input,
                           int64_t output_buffer_len,
                           uint8_t* output_buffer) override {
    
    std::cout << "Compressing..." << input_len << std::endl;

    // ------------------- CANNED COMPRESSION ------------------------
    qpl_path_t execution_path = qpl_path_hardware;
    
    // Source and output containers
    // std::vector<uint8_t> source(source_size, 5);
    // std::vector<uint8_t> destination(source_size / 2, 4);
    // std::vector<uint8_t> reference(source_size, 7);

    std::unique_ptr<uint8_t[]> job_buffer;
    qpl_status                 status;
    uint32_t                   size = 0;
    // qpl_histogram              deflate_histogram{};

    // Job initialization
    status = qpl_get_job_size(execution_path, &size);
    if (status != QPL_STS_OK) {
        throw std::runtime_error("An error acquired during job size getting.");
    }

    job_buffer = std::make_unique<uint8_t[]>(size);
    qpl_job *job = reinterpret_cast<qpl_job *>(job_buffer.get());

    status = qpl_init_job(execution_path, job);
    if (status != QPL_STS_OK) {
        throw std::runtime_error("An error acquired during compression job initializing.");
    }

    uint8_t* casted_input_ptr = const_cast<uint8_t*>(input);


    // Huffman table initialization
    // qpl_huffman_table_t huffman_table;

    // status = qpl_deflate_huffman_table_create(combined_table_type,
    //                                           execution_path,
    //                                           DEFAULT_ALLOCATOR_C,
    //                                           &huffman_table);
    // if (status != QPL_STS_OK) {
    //     throw std::runtime_error("An error acquired during Huffman table creation.");
    // }

    // // Filling deflate histogram first
    // status = qpl_gather_deflate_statistics(casted_input_ptr,
    //                                        input_len,
    //                                        &deflate_histogram,
    //                                        qpl_default_level,
    //                                        execution_path);
    // if (status != QPL_STS_OK) {
    //     throw std::runtime_error("An error acquired during gathering statistics for Huffman table.");
    // }

    // Building the Huffman table
    // status = qpl_huffman_table_init_with_histogram(huffman_table, &deflate_histogram);
    // if (status != QPL_STS_OK) {
    //     throw std::runtime_error("An error acquired during Huffman table initialization.");
    // }

    // Now perform canned mode compression
    job->op            = qpl_op_compress;
    job->level         = qpl_default_level;
    job->next_in_ptr   = casted_input_ptr;
    job->next_out_ptr  = output_buffer;
    job->available_in  = input_len;
    job->available_out = static_cast<uint32_t>(output_buffer_len);
    // job->flags         = QPL_FLAG_FIRST | QPL_FLAG_LAST | QPL_FLAG_CANNED_MODE | QPL_FLAG_OMIT_VERIFY;
    job->flags         = QPL_FLAG_FIRST | QPL_FLAG_LAST | QPL_FLAG_DYNAMIC_HUFFMAN | QPL_FLAG_OMIT_VERIFY;

    // job->huffman_table = huffman_table;

    // // Compression
    status = qpl_execute_job(job);
    if (status != QPL_STS_OK) {
        throw std::runtime_error("An error while compression occurred.");
    }

    const uint32_t compressed_size = job->total_out;
    std::cout << "Compressed size: " << compressed_size << std::endl;

    return static_cast<int64_t>(compressed_size);
    // ------------------- CANNED COMPRESSION...END ------------------------

    // RAW SNAPPY COMPRESSION
    // size_t output_size;
    // snappy::RawCompress(reinterpret_cast<const char*>(input),
    //                     static_cast<size_t>(input_len),
    //                     reinterpret_cast<char*>(output_buffer), &output_size);
    // return static_cast<int64_t>(output_size);
  }

  Result<std::shared_ptr<Compressor>> MakeCompressor() override {
    return Status::NotImplemented("Streaming compression unsupported with Snappy");
  }

  Result<std::shared_ptr<Decompressor>> MakeDecompressor() override {
    return Status::NotImplemented("Streaming decompression unsupported with Snappy");
  }

  Compression::type compression_type() const override { return Compression::SNAPPY; }
  int minimum_compression_level() const override { return kUseDefaultCompressionLevel; }
  int maximum_compression_level() const override { return kUseDefaultCompressionLevel; }
  int default_compression_level() const override { return kUseDefaultCompressionLevel; }
};

}  // namespace

std::unique_ptr<Codec> MakeSnappyCodec() { return std::make_unique<SnappyCodec>(); }

}  // namespace internal
}  // namespace util
}  // namespace arrow
