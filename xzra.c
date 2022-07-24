// Test program to read bytes from an .xz file at random offsets.
//
// Requires that the file is compressed with multiple small blocks, e.g.:
//
// xz --block-size=65536 --check=crc32 -0 --extreme --keep input.dat
//
// Currently, only single-stream files without padding at the end are supported.

#include <lzma.h>

#include <assert.h>
#include <stdio.h>
#include <stdint.h>
#include <stdlib.h>
#include <stdbool.h>

// For mmap()
#include <fcntl.h>
#include <sys/stat.h>
#include <sys/mman.h>
#include <unistd.h>

#define INDEX_MEMORY_LIMIT (100 << 20)  // 100 MB
#define BUFFER_MEMORY_LIMIT (100 << 20)  // 100 MB

static const lzma_allocator *allocator = NULL;  // use malloc()/free()

// Decompresses up to `output_size` bytes from the given block.
//
// The whole data file is described by `data` and `size`.
//
// The compressed block offset is `offset`.
//
// The output buffer is described by `output_data` and `output_size`.
//
// The size of the block may be greater or smaller than `output_size`. If the
// block size is smaller, the actual number of bytes decoded is written to
// *output_size. If the block size is greater, only the first *output_size
// bytes are decoded. In both cases this function returns LMZA_OK (assuming no
// other error occurred).
static lzma_ret decompress_block(
    lzma_check check,
    const char *data, size_t size,
    size_t offset,
    char *output_data, size_t *output_size) {
  // Filter list. This is initialized by lzma_block_header_decode().
  lzma_filter filters[LZMA_FILTERS_MAX + 1];

  // Figure out the block header size.
  uint8_t size_byte = data[offset];
  assert(size_byte != 0);  // 0 means a stream header instead
  lzma_block block = {};
  block.version = 1;
  block.header_size = lzma_block_header_size_decode(size_byte);
  block.check = check;
  block.filters = filters;

  // Decode block header.
  assert(size - offset >= LZMA_BLOCK_HEADER_SIZE_MIN);
  lzma_ret ret = lzma_block_header_decode(&block, allocator, data + offset);
  assert(size - offset >= block.header_size);
  if (ret == LZMA_OK) {
    // Block header looks good. Try to decode.
    lzma_stream stream = LZMA_STREAM_INIT;
    stream.allocator = allocator;
    ret = lzma_block_decoder(&stream, &block);
    if (ret == LZMA_OK) {
      stream.next_in = data + offset + block.header_size;
      stream.avail_in = size;
      stream.next_out = output_data;
      stream.avail_out = *output_size;
      // Actual decoding loop:
      while (ret == LZMA_OK && stream.avail_out > 0) {
        ret = lzma_code(&stream, LZMA_FINISH);
      }
      if (ret == LZMA_STREAM_END) {
        ret = LZMA_OK;
      }
      *output_size = (char*) stream.next_out - output_data;
    }
    lzma_end(&stream);
  }

  for (size_t i = 0; i < LZMA_FILTERS_MAX; ++i) {
    assert(allocator == NULL);
    free(filters[i].options);
  }
  return ret;
}

// Use the block index to seek to the blocks to be decoded.
//
// `offsets` and `noffset` describe the list uncompressed file offsets
// whose bytes we want to print.
// `check` is the checksum method from the global stream header.
// `data` and `size` cover the entire memory mapped file.
// `begin` to `end` (exclusive) gives the range of block data (i.e.,
// excluding the header, index and footer): 0 < begin <= end < size.
static void process_with_index(
    const int64_t *offsets, size_t noffset,
    lzma_check check,
    const char *data, size_t size,
    size_t begin, size_t end,
    lzma_index *index) {
  char *buffer_data = NULL;
  size_t buffer_size = 0;

  size_t i = 0;
  while (i < noffset) {
    lzma_index_iter iter;
    lzma_index_iter_init(&iter, index);

    if (lzma_index_iter_locate(&iter, offsets[i]) == true) {
      fprintf(stderr, "Offset out of bounds: %lld!\n", (long long) i);
      exit(1);
      return;
    }

    const int64_t offset = iter.block.uncompressed_file_offset;
    const int64_t block_size = iter.block.uncompressed_size;

    // See how many offsets we can satisfy from this block.
    assert(offsets[i] >= offset);
    assert(offsets[i] - offset <= block_size);
    size_t j = i + 1;
    while (j < noffset && offsets[j] - offset < block_size) ++j;

    // How many bytes we need to decode. This is only part of the block.
    size_t output_size = offsets[j - 1] - offset + 1;

    // Ensure we have enough buffer space.
    if (output_size > buffer_size) {
      assert(output_size < BUFFER_MEMORY_LIMIT);
      buffer_size = output_size;
      buffer_data = realloc(buffer_data, buffer_size);
      assert(buffer_data != NULL);
    }

    const int64_t coffset = iter.block.compressed_file_offset;
    assert(begin <= coffset && coffset <= end);
    size_t decoded_output_size = output_size;
    lzma_ret ret = decompress_block(
        check, data, size, coffset, buffer_data, &decoded_output_size);
    assert(ret == LZMA_OK);
    assert(decoded_output_size == output_size);

    // Output the requested bytes only.
    while (i < j) {
      char byte = buffer_data[offsets[i] - offset];
      fputc(byte, stdout);
      ++i;
    }
  }
  free(buffer_data);
}

// Reads `noffset` bytes at the given uncompressed file offsets from a
// compressed .xz file (mapped into memory at a location described by `data` and
// `size) and prints them to standard output. Offsets MUST be given in
// nondecreasing order.
static void process(
    const int64_t *offsets, size_t noffset,
    const char *data, size_t size) {
  assert(size >= 2*LZMA_STREAM_HEADER_SIZE);

  lzma_stream_flags header_flags = {};
  lzma_ret ret = lzma_stream_header_decode(&header_flags, data);
  assert(ret == LZMA_OK);

  lzma_stream_flags footer_flags = {};
  ret = lzma_stream_footer_decode(
		&footer_flags, data + size - LZMA_STREAM_HEADER_SIZE);
  assert(ret == LZMA_OK);

  ret = lzma_stream_flags_compare(&header_flags, &footer_flags);
  assert(ret == LZMA_OK);

  assert(footer_flags.backward_size > LZMA_BACKWARD_SIZE_MIN &&
      footer_flags.backward_size < size - 2*LZMA_STREAM_HEADER_SIZE);

  size_t index_start_pos = size - LZMA_STREAM_HEADER_SIZE - footer_flags.backward_size;
  size_t in_pos = index_start_pos;
  lzma_index *index = NULL;
  uint64_t memlimit = INDEX_MEMORY_LIMIT;
  ret = lzma_index_buffer_decode(&index, &memlimit, allocator, data, &in_pos, size);
  assert(ret == LZMA_OK && index != NULL);

  process_with_index(
      offsets, noffset, header_flags.check, data, size,
      LZMA_STREAM_HEADER_SIZE, index_start_pos, index);

  lzma_index_end(index, allocator);
}

int main(int argc, char *argv[]) {
  if (argc < 2) {
    fprintf(stderr, "Usage: xzra <input.xz> [<byte-offset>...]\n");
    exit(1);
  }

  size_t noffset = argc - 2;
  int64_t *offsets = calloc(noffset, sizeof(*offsets));
  for (int i = 0; i < noffset; ++i) {
    long long offset = 0;
    if (sscanf(argv[i + 2], "%lld", &offset) != 1) {
      fprintf(stderr, "Failed to parse offset %d: %s\n", i + 1, argv[i + 2]);
      exit(1);
    }
    offsets[i] = offset;
    if (i > 0 && offsets[i] < offsets[i - 1]) {
      fprintf(stderr, "Offsets must be nondecreasing!\n");
      exit(1);
    }
  }

  int fd = open(argv[1], O_RDONLY);
  if (fd == -1) {
    perror("open");
    exit(1);
  }

  struct stat st;
  if (fstat(fd, &st) == -1) {
    perror("fstat");
    exit(1);
  }
  size_t size = st.st_size;

  void *data = mmap(NULL, size, PROT_READ, MAP_SHARED, fd, 0);
  if (data == MAP_FAILED) {
    perror("mmap");
    exit(1);
  }
  close(fd);

  process(offsets, noffset, data, size);

  munmap(data, size);
  free(offsets);
  return 0;
}
