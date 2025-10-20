#ifndef PROCESS_CSV
#define PROCESS_CSV

#include <stddef.h>

#ifdef __ARM_NEON
#include <arm_neon.h>
#endif /* __ARM_NEON */

uint64_t find_character_in_chunk(uint8x16x4_t src, uint8_t c);

uint64_t parse_chunk(uint8_t *in, uint64_t *initial_quoted);

size_t find_one_indices(size_t start_index, uint64_t bits, size_t *indices, size_t *base);

size_t get_delimiter_indices(uint8_t *buf, size_t len, size_t* indices);

#endif
