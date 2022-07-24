// Test program to read bytes from an .xz file at random offsets.
//
// Requires that the file is compressed with multiple small blocks, e.g.:
//
// xz --block-size=65536 --check=crc32 -0 --extreme --keep input.dat
//
// Note: only unpadded single-stream files are supported.

#include <lzma.h>

#include <assert.h>
#include <stdio.h>
#include <stdint.h>
#include <stdlib.h>

// For mmap()
#include <fcntl.h>
#include <sys/stat.h>
#include <sys/mman.h>
#include <unistd.h>

#define INDEX_MEMORY_LIMIT (100 << 20)  // 100 MB
#define BUFFER_MEMORY_LIMIT (100 << 20)  // 100 MB

static const lzma_allocator *allocator = NULL;  // use malloc()/free()

static lzma_ret decompress_block(
    lzma_check check,
    const char *data, size_t size,
    size_t offset,
    char *output_data, size_t *output_size) {
  lzma_filter filters[LZMA_FILTERS_MAX + 1] = {};

  uint8_t size_byte = data[offset];
  assert(size_byte != 0);
  lzma_block block = {};
  block.version = 1;
  block.header_size = lzma_block_header_size_decode(size_byte);
  block.check = check;
  block.filters = filters;

  assert(size - offset >= LZMA_BLOCK_HEADER_SIZE_MIN);
  lzma_ret ret = lzma_block_header_decode(&block, allocator, data + offset);
  if (ret == LZMA_OK) {
    size_t in_pos = offset + block.header_size;
    size_t out_pos = 0;
    ret = lzma_block_buffer_decode(&block, allocator,
      data, &in_pos, size,
      output_data, &out_pos, *output_size);
    *output_size = out_pos;
  }

  for (size_t i = 0; i < LZMA_FILTERS_MAX; ++i) {
    assert(allocator == NULL);
    free(filters[i].options);
  }
  return ret;
}

// `data` and `size` describe the entire memory mapped file.
// `begin` to `end` (exclusive) gives the range of block data (i.e.,
// excluding the header, index and footer).
// 0 < begin <= end < size.
static void process_with_index(
    lzma_check check,
    const char *data, size_t size,
    size_t begin, size_t end,
    lzma_index *index) {
  char *buffer_data = NULL;
  size_t buffer_size = 0;

  lzma_index_iter iter;
  lzma_index_iter_init(&iter, index);
  while (!lzma_index_iter_next(&iter, LZMA_INDEX_ITER_NONEMPTY_BLOCK)) {

    size_t offset = iter.block.compressed_file_offset;

    assert(begin <= offset && offset <= end);

    size_t output_size = iter.block.uncompressed_size;

    if (output_size > buffer_size) {
      buffer_size = output_size;
      buffer_data = realloc(buffer_data, buffer_size);
      assert(buffer_data != NULL);
    }

    // Note: the current code just decompresses and prints all
    // block data. For random-access, we should skeep unwanted blocks, or
    // even better, use lzma_index_iter_locate() to find the correct block
    // directly.

    lzma_ret ret = decompress_block(
        check, data, size, offset, buffer_data, &output_size);
    assert(ret == LZMA_OK);
    assert(output_size == iter.block.uncompressed_size);

    fwrite(buffer_data, 1, output_size, stdout);
  }

  free(buffer_data);
}

static void process(const char *data, size_t size) {
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
  size_t in_pos = size - LZMA_STREAM_HEADER_SIZE - footer_flags.backward_size;
  lzma_index *index = NULL;
  uint64_t memlimit = INDEX_MEMORY_LIMIT;
  ret = lzma_index_buffer_decode(&index, &memlimit, allocator, data, &in_pos, size);
  assert(ret == LZMA_OK && index != NULL);

  fprintf(stderr, "Index uses %llu bytes of memory.\n", lzma_index_memused(index));

  process_with_index(header_flags.check, data, size,
      LZMA_STREAM_HEADER_SIZE, index_start_pos, index);

  lzma_index_end(index, allocator);
}

int main(int argc, char *argv[]) {
  if (argc < 2) {
    fprintf(stderr, "Usage: xzra <input.xz>\n");
    exit(1);
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

  process(data, size);

  munmap(data, size);
  return 0;
}
