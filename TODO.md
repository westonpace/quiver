Issue tracking until I get around to making GH

 * Figure out why I sometimes get warnings about coverage if I don't wipe CMakeFiles/quiver.dir first
 * Cmake format
 * Add check for SPDX license header
 * Add CI jobs for tidy/iwyu
 * Migrate status_test.cc from Arrow
 * Migrate logging_test.cc from Arrow
 * total_byte_size in mmap store should be a multiple of block size
 * block_size in mmap store should be a multiple of 512
 * Are we supposed to namespace the Arrow API?