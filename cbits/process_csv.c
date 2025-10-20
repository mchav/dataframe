#include "process_csv.h"
#include <stdlib.h>
#include <stdio.h>
/*
 * compile with clang -O3
 * in cabal use 
 *   cc-options: -O3
 *
 * Produce an array of field delimiter indices
 * Fields can be delimited by commas and newlines
 * TODO: allow the user to provide a custom delimiter
 * character to replace commas.
 * This should work with UTF-8, so long as the delimiter
 * character is a single byte.
 *
 * Delimiters can be escaped inside of quotes. Quotes
 * can also be placed inside quotes by double quoting.
 * For the purposes of this parser we can ignore double
 * quotes inside quotes, thereby treating the first quote
 * as the closing of the string and the next one the
 * immediate opening of a new one
 * 
 * We can find the quoted regions by first finding
 * the positions of the quotes (cmpeq and then movemask)
 * and then using the carryless multiplication operation
 * to know the regions that are quoted. We can then simply
 * and the inverse of the quotemask to exclude commas and
 * newlines inside quotes
 *
 */

#define const_vector(x) vmovq_n_u8(x);
#define compare_vectors(x, y) vceqq_u8(x, y);
#define and_vectors(x, y) vandq_u8(x, y);
#define pairwise_add_vectors(x, y) vpaddq_u8(x, y);
#define bitwise_select(r1, r2, d) vbslq_u8(r1, r2, d);
#define carryless_product(r1, r2) vmull_p64(r1, r2);

#define comma 0x2C
#define newline 0x0A
#define quote 0x22
#define all_ones ~0ULL
#define all_zeros 0ULL

// if the character is found at a particular
// position in the array of bytes, the
// corresponding bit in the returned uint64_t should
// be turned on.
// Example: searching for commas in
// input:  one, two, three
// result: 000100001000000
//
// "one", "two", three
// 1000100100010000000
// 1111000111100000000
//
//
// 01001111 / 00001000 / ..
// 00001000 / 00001000 /....
// 00000000 / 11111111 / 
// 01...
uint64_t find_character_in_chunk(uint8x16x4_t src, uint8_t c) {
  uint8x16_t mask = const_vector(c);
  uint8x16_t cmp0 = compare_vectors(src.val[0], mask);
  uint8x16_t cmp1 = compare_vectors(src.val[1], mask);
  uint8x16_t cmp2 = compare_vectors(src.val[2], mask);
  uint8x16_t cmp3 = compare_vectors(src.val[3], mask);

  // For an explanation of how to do movemask in
  // NEON, see: https://branchfree.org/2019/04/01/fitting-my-head-through-the-arm-holes-or-two-sequences-to-substitute-for-the-missing-pmovmskb-instruction-on-arm-neon/
  // The specific implementation below is owed to the 
  // user 'aqrit' in a comment on the blog above
  // There's also https://community.arm.com/arm-community-blogs/b/servers-and-cloud-computing-blog/posts/porting-x86-vector-bitmask-optimizations-to-arm-neon
  // 
  // The input to the move mask must be de-interleaved.
  // That is, for a string after vceqq_u8  we have,
  // cmp0 = aaaaaaaa / eeeeeeee / ...
  // cmp1 = bbbbbbbb / ffffffff / ...
  // cmp2 = cccccccc / gggggggg / ...
  // cmp3 = dddddddd / hhhhhhhh / ...
  // Luckily vld4q_u8 does this for us. Now we want
  // to interleave this into a 64bit integer with bits
  // abcdefgh...
  // cmp0 holds bits for positions
  // 0,4,8,..., cmp1 holds bits for 1,5,9,..., and so on.
  // So to bring together the bits for different positions
  // we right shift and combine with vsriq_n_u8
  //
  // vsriq_n_u8 shifts each byte of the first operand 
  // right by n bits, and combines it with the bits of
  // the second operand.
  // example:
  // uint8_t mask = 0xFF >> n; // if n = 1, 0111 1111
  // uint8_t shifted = operand1 >> n // 0bbb, bbbb
  // operand1 = (operand2 & (!mask)) | shifted
  // (aaaa aaaa & 1000 0000) | 0bbb bbbb = abbb bbbb
  // 
  // So we first bring together the bits the first two
  // rows and the next two rows (so to speak)
  // t0 = abbbbbbb/efffffff/...
  uint8x16_t t0 = vsriq_n_u8(cmp1, cmp0, 1);
  // t1 = cddddddd/ghhhhhhh/...
  uint8x16_t t1 = vsriq_n_u8(cmp3, cmp2, 1);
  // Now we must combine each of our combined rows
  // so that we get back our column interleaved
  // t2 = abcddddd/efghhhhh/...
  uint8x16_t t2 = vsriq_n_u8(t1, t0, 2);
  // Then to get rid of the repeated bits in the upper half
  // t3 = abcdabcd/efghefgh/...
  uint8x16_t t3 = vsriq_n_u8(t2, t2, 4);
  // and now it's the relatively simple matter of getting
  // rid of half the bits. We combine our 8bit words into 16
  // bit words for this step, and then we shift right by 4
  // and turn the result into an 8 bit word
  // afterreinterpert: abcdabcdefghefgh/...
  // afterrightshift: 0000abcdabcdefgh/...
  // take the lower bits: abcdefgh/...
  uint8x8_t t4 = vshrn_n_u16(vreinterpretq_u16_u8(t3), 4);
  // Finally we recombine them into a 64 bit integer
  // (vreinterpret_u64_u8 here does uint8x8 -> uint64x1
  // and vget_lane_u64 does uint64x1 -> uint64) 
  return vget_lane_u64(vreinterpret_u64_u8(t4), 0);
}

// I owe a debt to https://github.com/geofflangdale/simdcsv
// Let's go ahead and assume `in` will only ever get 64 bytes
// initial_quoted will be either all_ones ~0ULL or all_zeros 0ULL
uint64_t parse_chunk(uint8_t *in, uint64_t *initial_quoted) {
  uint8x16x4_t invec = vld4q_u8(in);

  uint64_t quotebits = find_character_in_chunk(invec, quote);
  // See https://wunkolo.github.io/post/2020/05/pclmulqdq-tricks/
  // Also, section 3.1.1 of Parsing Gigabytes of JSON per Second,
  // Geoff Langdale, Daniel Lemire, https://arxiv.org/pdf/1902.08318
  uint64_t quotemask = carryless_product(all_ones, quotebits);
  quotemask ^= (*initial_quoted);
  // Find out if the chunk ends in a quoted region by looking
  // at the last bit
  (*initial_quoted) = (uint64_t)((int64_t)quotemask >> 63);

  uint64_t commabits = find_character_in_chunk(invec, comma);
  uint64_t newlinebits = find_character_in_chunk(invec, newline);

  uint64_t delimiter_bits = (commabits | newlinebits) & ~quotemask;
  return delimiter_bits;
}

size_t find_one_indices(size_t start_index, uint64_t bits, size_t *indices, size_t *base) {
  size_t position = 0;
  uint64_t bitset = bits;
  while (bitset != 0) {
    // temp only has the least significant bit of
    // bitset turned on.
    // In twos complement: 0 - x = ~ x + 1
    uint64_t temp = bitset & -bitset;
    // count trailing zeros
    size_t r = __builtin_ctzll(bitset);
    indices[(*base) + position] = start_index + r;
    position++;

    bitset ^= temp;
  }
  *base += position;
  return position;
}

size_t get_delimiter_indices(uint8_t *buf, size_t len, size_t *indices) {
  // Recall we padded our file with 64 empty bytes.
  // So if, for example, we had a file of 187 bytes
  // We pad it with zeros and so we have 251 bytes
  // The chunks we have are ptr + 0, ptr + 64, and pt
  // (we don't do ptr + 192 since we do len - 64
  // below. This way, in ptr + 128 we have 59 bytes of
  // actual data and 5 bytes of zeros. If we didn't do this
  // we'd be reading past the end of file on the last row
  // #ifndef __AVX2__
  //   return -1;
  // #endif
  size_t unpaddedLen = len < 64 ? 0 : len - 64;
  uint64_t initial_quoted = 0ULL;
  size_t base = 0;
  for (size_t i = 0; i < unpaddedLen; i += 64) {
    uint8_t *in = buf + i;
    uint64_t delimiter_bits = parse_chunk(in, &initial_quoted);
    find_one_indices(i, delimiter_bits, indices, &base);
  }
  return base;
}
