<!--
  ~ Licensed to the Apache Software Foundation (ASF) under one
  ~ or more contributor license agreements.  See the NOTICE file
  ~ distributed with this work for additional information
  ~ regarding copyright ownership.  The ASF licenses this file
  ~ to you under the Apache License, Version 2.0 (the
  ~ "License"); you may not use this file except in compliance
  ~ with the License.  You may obtain a copy of the License at
  ~
  ~   http://www.apache.org/licenses/LICENSE-2.0
  ~
  ~ Unless required by applicable law or agreed to in writing,
  ~ software distributed under the License is distributed on an
  ~ "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
  ~ KIND, either express or implied.  See the License for the
  ~ specific language governing permissions and limitations
  ~ under the License.
  -->

# Test data files for Parquet compatibility and regression testing

| File                                         | Description                                                                                                                                                      |
|----------------------------------------------|------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| delta_byte_array.parquet                     | string columns with DELTA_BYTE_ARRAY encoding. See [delta_byte_array.md](delta_byte_array.md) for details.                                                       |
| delta_length_byte_array.parquet              | string columns with DELTA_LENGTH_BYTE_ARRAY encoding.                                                                                                            |
| delta_binary_packed.parquet                  | INT32 and INT64 columns with DELTA_BINARY_PACKED encoding. See [delta_binary_packed.md](delta_binary_packed.md) for details.                                     |
| delta_encoding_required_column.parquet       | required INT32 and STRING columns with delta encoding. See [delta_encoding_required_column.md](delta_encoding_required_column.md) for details.                   |
| delta_encoding_optional_column.parquet       | optional INT64 and STRING columns with delta encoding. See [delta_encoding_optional_column.md](delta_encoding_optional_column.md) for details.                   |
| nested_structs.rust.parquet                  | Used to test that the Rust Arrow reader can lookup the correct field from a nested struct. See [ARROW-11452](https://issues.apache.org/jira/browse/ARROW-11452)  |
| data_index_bloom_encoding_stats.parquet | optional STRING column. Contains optional metadata: bloom filters, column index, offset index and encoding stats.                                                |
| data_index_bloom_encoding_with_length.parquet | Same as `data_index_bloom_encoding_stats.parquet` but has `bloom_filter_length` populated in the ColumnMetaData |
| null_list.parquet                       | an empty list. Generated from this json `{"emptylist":[]}` and for the purposes of testing correct read/write behaviour of this base case.                       |
| alltypes_tiny_pages.parquet             | small page sizes with dictionary encoding with page index from [impala](https://github.com/apache/impala/tree/master/testdata/data/alltypes_tiny_pages.parquet). |
| alltypes_tiny_pages_plain.parquet       | small page sizes with plain encoding with page index [impala](https://github.com/apache/impala/tree/master/testdata/data/alltypes_tiny_pages.parquet).           |
| rle_boolean_encoding.parquet            | option boolean columns with RLE encoding                                                                                                                         |
| fixed_length_byte_array.parquet                | optional FIXED_LENGTH_BYTE_ARRAY column with page index. See [fixed_length_byte_array.md](fixed_length_byte_array.md) for details.                        |
| int32_with_null_pages.parquet                  | optional INT32 column with random null pages. See [int32_with_null_pages.md](int32_with_null_pages.md) for details.                        |
| datapage_v1-uncompressed-checksum.parquet      | uncompressed INT32 columns in v1 data pages with a matching CRC        |
| datapage_v1-snappy-compressed-checksum.parquet | compressed INT32 columns in v1 data pages with a matching CRC          |
| datapage_v1-corrupt-checksum.parquet           | uncompressed INT32 columns in v1 data pages with a mismatching CRC     |
| overflow_i16_page_cnt.parquet                  | row group with more than INT16_MAX pages                   |
| bloom_filter.bin                               | deprecated bloom filter binary with binary header and murmur3 hashing |
| bloom_filter.xxhash.bin                        | bloom filter binary with thrift header and xxhash hashing    |
| nan_in_stats.parquet                           | statistics contains NaN in max, from PyArrow 0.8.0. See note below on "NaN in stats".  |
| rle-dict-snappy-checksum.parquet                 | compressed and dictionary-encoded INT32 and STRING columns in format v2 with a matching CRC |
| plain-dict-uncompressed-checksum.parquet         | uncompressed and dictionary-encoded INT32 and STRING columns in format v1 with a matching CRC |
| rle-dict-uncompressed-corrupt-checksum.parquet   | uncompressed and dictionary-encoded INT32 and STRING columns in format v2 with a mismatching CRC |
| large_string_map.brotli.parquet       | MAP(STRING, INT32) with a string column chunk of more than 2GB. See [note](#large-string-map) below |
| float16_nonzeros_and_nans.parquet | Float16 (logical type) column with NaNs and nonzero finite min/max values |
| float16_zeros_and_nans.parquet    | Float16 (logical type) column with NaNs and zeros as min/max values. . See [note](#float16-files) below |
| concatenated_gzip_members.parquet     | 513 UINT64 numbers compressed using 2 concatenated gzip members in a single data page |
| byte_stream_split.zstd.parquet | Standard normals with `BYTE_STREAM_SPLIT` encoding. See [note](#byte-stream-split) below |
| incorrect_map_schema.parquet | Contains a Map schema without explicitly required keys, produced by Presto. See [note](#incorrect-map-schema) |
| column_chunk_key_value_metadata.parquet | two INT32 columns, one with column chunk key-value metadata {"foo": "bar", "thisiskeywithoutvalue": null} note that the second key "thisiskeywithoutvalue", does not have a value, but the value can be mapped to an empty string "" when read depending on the client |
| sort_columns.parquet | INT64 and BYTE_ARRAY columns with first column with nulls first and descending, second column with nulls last and ascending. This file contains two row groups with same data and sorting columns. |
| old_list_structure.parquet | Single LIST<LIST<INT32>> column with legacy two-level list structure. See [old_list_structure.md](old_list_structure.md) |
| repeated_primitive_no_list.parquet | REPEATED INT32 and BYTE_ARRAY fields without LIST annotation. See [note](#REPEATED-primitive-fields-with-no-LIST-annotation) |
| map_no_value.parquet | MAP with null values, MAP with INT32 keys and no values, and LIST<INT32> column with same values as the MAP keys. See [map_no_value.md](map_no_value.md) |
| page_v2_empty_compressed.parquet | An INT32 column with DataPageV2, all values are null, the zero-sized data is compressed using ZSTD. This is a valid non-zero bytes ZSTD stream that uncompresses into 0 bytes. |
| datapage_v2_empty_datapage.snappy.parquet | A compressed FLOAT column with DataPageV2, a single row, value is null, the file uses Snappy compression, but there is no data for uncompression (see [related issue](https://github.com/apache/arrow-rs/issues/7388)). The zero bytes must not be attempted to be uncompressed, as this is an invalid Snappy stream. |
| unknown-logical-type.parquet | A file containing a column annotated with a LogicalType whose identifier has been set to an abitrary high value to check the behaviour of an old reader reading a file written by a new writer containing an unsupported type (see [related issue](https://github.com/apache/arrow/issues/41764)). |
| int96_from_spark.parquet | Single column of (deprecated) int96 values that originated as Apache Spark microsecond-resolution timestamps. Some values are outside the range typically representable by 64-bit nanosecond-resolution timestamps. See [int96_from_spark.md](int96_from_spark.md) for details. |
| binary_truncated_min_max.parquet | A file containing six columns with exact, fully-truncated and partially-truncated max and min statistics and with the expected is_{min/max}_value_exact.  (see [note](Binary-truncated-min-and-max-statistics)).|

TODO: Document what each file is in the table above.

## Encrypted Files

Tests files with .parquet.encrypted suffix are encrypted using Parquet Modular Encryption.

A detailed description of the Parquet Modular Encryption specification can be found here:
```
 https://github.com/apache/parquet-format/blob/encryption/Encryption.md
```

Following are the keys and key ids (when using key\_retriever) used to encrypt
the encrypted columns and footer in all the encrypted files:
* Encrypted/Signed Footer:
  * key:   {0,1,2,3,4,5,6,7,8,9,0,1,2,3,4,5}
  * key_id: "kf"
* Encrypted column named double_field (including column and offset index):
  * key:  {1,2,3,4,5,6,7,8,9,0,1,2,3,4,5,0}
  * key_id: "kc1"
* Encrypted column named float_field (including column and offset index):
  * key: {1,2,3,4,5,6,7,8,9,0,1,2,3,4,5,1}
  * key_id: "kc2"

The following files are encrypted with AAD prefix "tester":
1. encrypt\_columns\_and\_footer\_disable\_aad\_storage.parquet.encrypted
2. encrypt\_columns\_and\_footer\_aad.parquet.encrypted


A sample that reads and checks these files can be found at the following tests
in Parquet C++:
```
cpp/src/parquet/encryption/read-configurations-test.cc
cpp/src/parquet/encryption/test-encryption-util.h
```

The `external_key_material_java.parquet.encrypted` file was encrypted using parquet-mr with
external key material enabled, so the key material is found in the
`_KEY_MATERIAL_FOR_external_key_material_java.parquet.encrypted.json` file.
This data was written using the `org.apache.parquet.crypto.keytools.mocks.InMemoryKMS` KMS client,
which is compatible with the `TestOnlyInServerWrapKms` KMS client used in C++ tests.

## Checksum Files

The schema for the `datapage_v1-*-checksum.parquet` test files is:
```
message m {
    required int32 a;
    required int32 b;
}
```

The detailed structure for these files is as follows:

* `data/datapage_v1-uncompressed-checksum.parquet`:
  ```
  [ Column "a" [ Page 0 [correct crc] | Uncompressed Contents ][ Page 1 [correct crc] | Uncompressed Contents ]]
  [ Column "b" [ Page 0 [correct crc] | Uncompressed Contents ][ Page 1 [correct crc] | Uncompressed Contents ]]
  ```

* `data/datapage_v1-snappy-compressed-checksum.parquet`:
  ```
  [ Column "a" [ Page 0 [correct crc] | Snappy Contents ][ Page 1 [correct crc] | Snappy Contents ]]
  [ Column "b" [ Page 0 [correct crc] | Snappy Contents ][ Page 1 [correct crc] | Snappy Contents ]]
  ```

* `data/datapage_v1-corrupt-checksum.parquet`:
  ```
  [ Column "a" [ Page 0 [bad crc] | Uncompressed Contents ][ Page 1 [correct crc] | Uncompressed Contents ]]
  [ Column "b" [ Page 0 [correct crc] | Uncompressed Contents ][ Page 1 [bad crc] | Uncompressed Contents ]]
  ```

The schema for the `*-dict-*-checksum.parquet` test files is:
* `data/rle-dict-snappy-checksum.parquet`:
  ```
  [ Column "long_field" [ Dict Page [correct crc] | Compressed PLAIN Contents ][ Page 0 [correct crc] | Compressed RLE_DICTIONARY Contents ]]
  [ Column "binary_field" [ Dict Page [correct crc] | Compressed PLAIN Contents ][ Page 0 [correct crc] | Compressed RLE_DICTIONARY Contents ]]
  ```

* `data/plain-dict-uncompressed-checksum.parquet`:
  ```
  [ Column "long_field" [ Dict Page [correct crc] | Uncompressed PLAIN_DICTIONARY(DICT) Contents ][ Page 0 [correct crc] | Uncompressed PLAIN_DICTIONARY Contents ]]
  [ Column "binary_field" [ Dict Page [correct crc] | Uncompressed PLAIN_DICTIONARY(DICT) Contents ][ Page 0 [correct crc] | Uncompressed PLAIN_DICTIONARY Contents ]]
  ```

* `data/rle-dict-uncompressed-corrupt-checksum.parquet`:
  ```
  [ Column "long_field" [ Dict Page [bad crc] | Uncompressed PLAIN Contents ][ Page 0 [correct crc] | Uncompressed RLE_DICTIONARY Contents ]]
  [ Column "binary_field" [ Dict Page [bad crc] | Uncompressed PLAIN Contents ][ Page 0 [correct crc] | Uncompressed RLE_DICTIONARY Contents ]]
  ```

## Bloom Filter Files

Bloom filter examples have been generated by parquet-mr.
They are not Parquet files but only contain the bloom filter header and payload.

For each of `bloom_filter.bin` and `bloom_filter.xxhash.bin`, the bloom filter
was generated by inserting the strings "hello", "parquet", "bloom", "filter".

`bloom_filter.bin` uses the original Murmur3-based bloom filter format as of
https://github.com/apache/parquet-format/commit/54839ad5e04314c944fed8aa4bc6cf15e4a58698.

`bloom_filter.xxhash.bin` uses the newer xxHash-based bloom filter format as of
https://github.com/apache/parquet-format/commit/3fb10e00c2204bf1c6cc91e094c59e84cefcee33.

## NaN in stats

Prior to version 1.4.0, the C++ Parquet writer would write NaN values in min and
max statistics. (Correction in [this issue](https://issues.apache.org/jira/browse/PARQUET-1225)).
It has been updated since to ignore NaN values when calculating
statistics, but for backwards compatibility the following rules were established
(in [PARQUET-1222](https://github.com/apache/parquet-format/pull/185)):

> For backwards compatibility when reading files:
> * If the min is a NaN, it should be ignored.
> * If the max is a NaN, it should be ignored.
> * If the min is +0, the row group may contain -0 values as well.
> * If the max is -0, the row group may contain +0 values as well.
> * When looking for NaN values, min and max should be ignored.

The file `nan_in_stats.parquet` was generated with:

```python
import pyarrow as pa # version 0.8.0
import pyarrow.parquet as pq
from numpy import NaN

tab = pa.Table.from_arrays(
    [pa.array([1.0, NaN])],
    names="x"
)

pq.write_table(tab, "nan_in_stats.parquet")

metadata = pq.read_metadata("nan_in_stats.parquet")
metadata.row_group(0).column(0)
# <pyarrow._parquet.ColumnChunkMetaData object at 0x7f28539e58f0>
#   file_offset: 88
#   file_path:
#   type: DOUBLE
#   num_values: 2
#   path_in_schema: x
#   is_stats_set: True
#   statistics:
#     <pyarrow._parquet.RowGroupStatistics object at 0x7f28539e5738>
#       has_min_max: True
#       min: 1
#       max: nan
#       null_count: 0
#       distinct_count: 0
#       num_values: 2
#       physical_type: DOUBLE
#   compression: 1
#   encodings: <map object at 0x7f28539eb4e0>
#   has_dictionary_page: True
#   dictionary_page_offset: 4
#   data_page_offset: 36
#   index_page_offset: 0
#   total_compressed_size: 84
#   total_uncompressed_size: 80
```

## Large string map

The file `large_string_map.brotli.parquet` was generated with:
```python
import pyarrow as pa
import pyarrow.parquet as pq

arr = pa.array([[("a" * 2**30, 1)]], type = pa.map_(pa.string(), pa.int32()))
arr = pa.chunked_array([arr, arr])
tab = pa.table({ "arr": arr })

pq.write_table(tab, "test.parquet", compression='BROTLI')
```

It is meant to exercise reading of structured data where each value
is smaller than 2GB but the combined uncompressed column chunk size
is greater than 2GB.

## Float16 Files

The files `float16_zeros_and_nans.parquet` and `float16_nonzeros_and_nans.parquet`
are meant to exercise a variety of test cases regarding `Float16` columns (which
are represented as 2-byte `FixedLenByteArray`s), including:
* Basic binary representations of standard values, +/- zeros, and NaN
* Comparisons between finite values
* Exclusion of NaNs from statistics min/max
* Normalizing min/max values when only zeros are present (i.e. `min` is always -0 and `max` is always +0)

The aforementioned files were generated with:

```python
import pyarrow as pa
import pyarrow.parquet as pq
import numpy as np

t1 = pa.Table.from_arrays(
    [pa.array([None,
               np.float16(0.0),
               np.float16(np.NaN)], type=pa.float16())],
    names="x")
t2 = pa.Table.from_arrays(
    [pa.array([None,
               np.float16(1.0),
               np.float16(-2.0),
               np.float16(np.NaN),
               np.float16(0.0),
               np.float16(-1.0),
               np.float16(-0.0),
               np.float16(2.0)],
              type=pa.float16())],
    names="x")

pq.write_table(t1, "float16_zeros_and_nans.parquet", compression='none')
pq.write_table(t2, "float16_nonzeros_and_nans.parquet", compression='none')

m1 = pq.read_metadata("float16_zeros_and_nans.parquet")
m2 = pq.read_metadata("float16_nonzeros_and_nans.parquet")

print(m1.row_group(0).column(0))
print(m2.row_group(0).column(0))
# <pyarrow._parquet.ColumnChunkMetaData object at 0x7f79e9a3d850>
#   file_offset: 68
#   file_path:
#   physical_type: FIXED_LEN_BYTE_ARRAY
#   num_values: 3
#   path_in_schema: x
#   is_stats_set: True
#   statistics:
#     <pyarrow._parquet.Statistics object at 0x7f79e9a3d940>
#       has_min_max: True
#       min: b'\x00\x80'
#       max: b'\x00\x00'
#       null_count: 1
#       distinct_count: None
#       num_values: 2
#       physical_type: FIXED_LEN_BYTE_ARRAY
#       logical_type: Float16
#       converted_type (legacy): NONE
#   compression: UNCOMPRESSED
#   encodings: ('PLAIN', 'RLE', 'RLE_DICTIONARY')
#   has_dictionary_page: True
#   dictionary_page_offset: 4
#   data_page_offset: 22
#   total_compressed_size: 64
#   total_uncompressed_size: 64
# <pyarrow._parquet.ColumnChunkMetaData object at 0x7f79ea003c40>
#   file_offset: 80
#   file_path:
#   physical_type: FIXED_LEN_BYTE_ARRAY
#   num_values: 8
#   path_in_schema: x
#   is_stats_set: True
#   statistics:
#     <pyarrow._parquet.Statistics object at 0x7f79e9a3d8a0>
#       has_min_max: True
#       min: b'\x00\xc0'
#       max: b'\x00@'
#       null_count: 1
#       distinct_count: None
#       num_values: 7
#       physical_type: FIXED_LEN_BYTE_ARRAY
#       logical_type: Float16
#       converted_type (legacy): NONE
#   compression: UNCOMPRESSED
#   encodings: ('PLAIN', 'RLE', 'RLE_DICTIONARY')
#   has_dictionary_page: True
#   dictionary_page_offset: 4
#   data_page_offset: 32
#   total_compressed_size: 76
#   total_uncompressed_size: 76
```

## Byte Stream Split

# FLOAT and DOUBLE data

`byte_stream_split.zstd.parquet` is generated by pyarrow 14.0.2 using the following code:

```python
import pyarrow as pa
from pyarrow import parquet as pq
import numpy as np

np.random.seed(0)
table = pa.Table.from_pydict({
  'f32': np.random.normal(size=300).astype(np.float32),
  'f64': np.random.normal(size=300).astype(np.float64),
})

pq.write_table(
  table,
  'byte_stream_split.parquet',
  version='2.6',
  compression='zstd',
  compression_level=22,
  column_encoding='BYTE_STREAM_SPLIT',
  use_dictionary=False,
)
```

This is a practical case where `BYTE_STREAM_SPLIT` encoding obtains a smaller file size than `PLAIN` or dictionary.
Since the distributions are random normals centered at 0, each byte has nontrivial behavior.

# Additional types

`byte_stream_split_extended.gzip.parquet` is generated by pyarrow 16.0.0.
It contains 7 pairs of columns, each in two variants containing the same
values: one `PLAIN`-encoded and one `BYTE_STREAM_SPLIT`-encoded:
```
Version: 2.6
Created By: parquet-cpp-arrow version 16.0.0-SNAPSHOT
Total rows: 200
Number of RowGroups: 1
Number of Real Columns: 14
Number of Columns: 14
Number of Selected Columns: 14
Column 0: float16_plain (FIXED_LEN_BYTE_ARRAY(2) / Float16)
Column 1: float16_byte_stream_split (FIXED_LEN_BYTE_ARRAY(2) / Float16)
Column 2: float_plain (FLOAT)
Column 3: float_byte_stream_split (FLOAT)
Column 4: double_plain (DOUBLE)
Column 5: double_byte_stream_split (DOUBLE)
Column 6: int32_plain (INT32)
Column 7: int32_byte_stream_split (INT32)
Column 8: int64_plain (INT64)
Column 9: int64_byte_stream_split (INT64)
Column 10: flba5_plain (FIXED_LEN_BYTE_ARRAY(5))
Column 11: flba5_byte_stream_split (FIXED_LEN_BYTE_ARRAY(5))
Column 12: decimal_plain (FIXED_LEN_BYTE_ARRAY(4) / Decimal(precision=7, scale=3) / DECIMAL(7,3))
Column 13: decimal_byte_stream_split (FIXED_LEN_BYTE_ARRAY(4) / Decimal(precision=7, scale=3) / DECIMAL(7,3))
```

To check conformance of a `BYTE_STREAM_SPLIT` decoder, read each
`BYTE_STREAM_SPLIT`-encoded column and compare the decoded values against
the values from the corresponding `PLAIN`-encoded column. The values should
be equal.

## Incorrect Map Schema

A number of producers, such as Presto/Trino/Athena, have been creating files with schemas
where the Map key fields are marked as optional rather than required.
This is not spec-compliant, yet appears in a number of existing data files in the wild.

This issue has been fixed in:
- [Trino v386+](https://github.com/trinodb/trino/commit/3247bd2e64d7422bd13e805cd67cfca3fa8ba520)
- [Presto v0.274+](https://github.com/prestodb/presto/commit/842b46972c11534a7729d0a18e3abc5347922d1a)

We can recreate these problematic files for testing [arrow-rs #5630](https://github.com/apache/arrow-rs/pull/5630)
with relevant Presto/Trino CLI, or with AWS Athena Console:

```sql
CREATE TABLE my_catalog.my_table_name WITH (format = 'Parquet') AS (
    SELECT MAP (
        ARRAY['name', 'parent'],
        ARRAY[
            'report',
            'another'
        ]
    ) my_map
)
```

The schema in the created file is:

```
message hive_schema {
  OPTIONAL group my_map (MAP) {
    REPEATED group key_value (MAP_KEY_VALUE) {
      OPTIONAL BYTE_ARRAY key (STRING);
      OPTIONAL BYTE_ARRAY value (STRING);
    }
  }
}
```

## REPEATED primitive fields with no LIST annotation
```
Metadata for file: repeated_primitive_no_list.parquet

version: 1
num of rows: 4
created by: parquet-rs version 53.2.0
message schema {
  REPEATED INT32 Int32_list;
  REPEATED BYTE_ARRAY String_list (UTF8);
  REQUIRED group group_of_lists {
    REPEATED INT32 Int32_list_in_group;
    REPEATED BYTE_ARRAY String_list_in_group (UTF8);
  }
}
```

## Binary truncated min and max statistics

For the file: binary_truncated_min_max.parquet

The file contains six columns written with parquet-rs 55.1.0 with `statistics_truncate_length=2`.
The contents are the following:

|column_name                |min      |is_min_value_exact|max            |is_max_value_exact|
| ------------------------- | ------- | ---------------- | ------------- | ---------------- |
|utf8_full_truncation       |"Al"     |false             |"Kf"           |false             |
|binary_full_truncation     |"0x416C" |false             |"0x4B66"       |false             |
|utf8_partial_truncation    |"Al"     |false             |"🚀Kevin Bacon"|true              |
|binary_partial_truncation  |"0x416C" |false             |"0xFFFF0102"   |true              |
|utf8_no_truncation         |"Al"     |true              |"Ke"           |true              |
|binary_no_truncation       |"0x416C" |true              |"0x4B65"       |true              |

Columns `utf8_full_truncation` and `binary_full_truncation` are truncating the min/max values and `is_{min/max}_value_exact` are false.
Columns `utf8_partial_truncation` and `binary_partial_truncation` are truncating min value but can't truncate the maximum value. `is_min_value_exact` is false but `is_max_value_exact` is true.
Columns `utf8_no_truncation` and `binary_no_truncation` contain min and max value that fit on min/max. Both `is_{min/max}_value_exact` are true.

Some info:
```
$ java -jar parquet-cli/target/parquet-cli-1.16.0-SNAPSHOT-runtime.jar meta /home/raulcd/code/parquet_truncate_file_generator/binary_truncated_min_max.parquet

File path:  /home/raulcd/code/parquet_truncate_file_generator/binary_truncated_min_max.parquet
Created by: parquet-rs version 55.1.0
Properties:
  ARROW:schema: /////6wBAAAQAAAAAAAKAAwACgAJAAQACgAAABAAAAAAAQQACAAIAAAABAAIAAAABAAAAAYAAABAAQAA9AAAALgAAAB4AAAAQAAAAAQAAADo/v//FAAAAAwAAAAAAAAEDAAAAAAAAADY/v//FAAAAGJpbmFyeV9ub190cnVuY2F0aW9uAAAAACD///8UAAAADAAAAAAAAAUMAAAAAAAAABD///8SAAAAdXRmOF9ub190cnVuY2F0aW9uAABU////FAAAAAwAAAAAAAAEDAAAAAAAAABE////GQAAAGJpbmFyeV9wYXJ0aWFsX3RydW5jYXRpb24AAACQ////FAAAAAwAAAAAAAAFDAAAAAAAAACA////FwAAAHV0ZjhfcGFydGlhbF90cnVuY2F0aW9uAMj///8UAAAADAAAAAAAAAQMAAAAAAAAALj///8WAAAAYmluYXJ5X2Z1bGxfdHJ1bmNhdGlvbgAAEAAUABAAAAAPAAQAAAAIABAAAAAYAAAADAAAAAAAAAUQAAAAAAAAAAQABAAEAAAAFAAAAHV0ZjhfZnVsbF90cnVuY2F0aW9uAAAAAA==
Schema:
message arrow_schema {
  required binary utf8_full_truncation (STRING);
  required binary binary_full_truncation;
  required binary utf8_partial_truncation (STRING);
  required binary binary_partial_truncation;
  required binary utf8_no_truncation (STRING);
  required binary binary_no_truncation;
}


Row group 0:  count: 12  117.83 B records  start: 4  total(compressed): 1.381 kB total(uncompressed):1.381 kB
--------------------------------------------------------------------------------
                           type      encodings count     avg size   nulls   min / max
utf8_full_truncation       BINARY    _ BB_     12        20.83 B    0       "Al" / "Kf"
binary_full_truncation     BINARY    _ BB_     12        20.83 B    0       "0x416C" / "0x4B66"
utf8_partial_truncation    BINARY    _ BB_     12        21.50 B    0       "Al" / "🚀Kevin Bacon"
binary_partial_truncation  BINARY    _ BB_     12        19.67 B    0       "0x416C" / "0xFFFF0102"
utf8_no_truncation         BINARY    _ BB_     12        17.50 B    0       "Al" / "Ke"
binary_no_truncation       BINARY    _ BB_     12        17.50 B    0       "0x416C" / "0x4B65"
```
and
```
java -jar parquet-cli/target/parquet-cli-1.16.0-SNAPSHOT-runtime.jar cat /home/raulcd/code/parquet_truncate_file_generator/binary_truncated_min_max.parquet
{"utf8_full_truncation": "Blart Versenwald III", "binary_full_truncation": "Blart Versenwald III", "utf8_partial_truncation": "Blart Versenwald III", "binary_partial_truncation": "Blart Versenwald III", "utf8_no_truncation": "Blart Versenwald III", "binary_no_truncation": "Blart Versenwald III"}
{"utf8_full_truncation": "Alice Johnson", "binary_full_truncation": "Alice Johnson", "utf8_partial_truncation": "Alice Johnson", "binary_partial_truncation": "Alice Johnson", "utf8_no_truncation": "Al", "binary_no_truncation": "Al"}
{"utf8_full_truncation": "Bob Smith", "binary_full_truncation": "Bob Smith", "utf8_partial_truncation": "Bob Smith", "binary_partial_truncation": "Bob Smith", "utf8_no_truncation": "Bob Smith", "binary_no_truncation": "Bob Smith"}
{"utf8_full_truncation": "Charlie Brown", "binary_full_truncation": "Charlie Brown", "utf8_partial_truncation": "Charlie Brown", "binary_partial_truncation": "Charlie Brown", "utf8_no_truncation": "Charlie Brown", "binary_no_truncation": "Charlie Brown"}
{"utf8_full_truncation": "Diana Prince", "binary_full_truncation": "Diana Prince", "utf8_partial_truncation": "Diana Prince", "binary_partial_truncation": "Diana Prince", "utf8_no_truncation": "Diana Prince", "binary_no_truncation": "Diana Prince"}
{"utf8_full_truncation": "Edward Norton", "binary_full_truncation": "Edward Norton", "utf8_partial_truncation": "Edward Norton", "binary_partial_truncation": "Edward Norton", "utf8_no_truncation": "Edward Norton", "binary_no_truncation": "Edward Norton"}
{"utf8_full_truncation": "Fiona Apple", "binary_full_truncation": "Fiona Apple", "utf8_partial_truncation": "Fiona Apple", "binary_partial_truncation": "Fiona Apple", "utf8_no_truncation": "Fiona Apple", "binary_no_truncation": "Fiona Apple"}
{"utf8_full_truncation": "George Lucas", "binary_full_truncation": "George Lucas", "utf8_partial_truncation": "George Lucas", "binary_partial_truncation": "George Lucas", "utf8_no_truncation": "George Lucas", "binary_no_truncation": "George Lucas"}
{"utf8_full_truncation": "Helen Keller", "binary_full_truncation": "Helen Keller", "utf8_partial_truncation": "Helen Keller", "binary_partial_truncation": "Helen Keller", "utf8_no_truncation": "Helen Keller", "binary_no_truncation": "Helen Keller"}
{"utf8_full_truncation": "Ivan Drago", "binary_full_truncation": "Ivan Drago", "utf8_partial_truncation": "Ivan Drago", "binary_partial_truncation": "Ivan Drago", "utf8_no_truncation": "Ivan Drago", "binary_no_truncation": "Ivan Drago"}
{"utf8_full_truncation": "Julia Roberts", "binary_full_truncation": "Julia Roberts", "utf8_partial_truncation": "Julia Roberts", "binary_partial_truncation": "Julia Roberts", "utf8_no_truncation": "Julia Roberts", "binary_no_truncation": "Julia Roberts"}
{"utf8_full_truncation": "Kevin Bacon", "binary_full_truncation": "Kevin Bacon", "utf8_partial_truncation": "🚀Kevin Bacon", "binary_partial_truncation": "ÿÿ\u0001\u0002", "utf8_no_truncation": "Ke", "binary_no_truncation": "Ke"}
```
