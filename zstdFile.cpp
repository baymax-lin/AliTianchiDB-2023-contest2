// /*
//  * Copyright (c) Meta Platforms, Inc. and affiliates.
//  * All rights reserved.
//  *
//  * This source code is licensed under both the BSD-style license (found in
//  the
//  * LICENSE file in the root directory of this source tree) and the GPLv2
//  (found
//  * in the COPYING file in the root directory of this source tree).
//  * You may select, at your option, one of the above-listed licenses.
//  */

// #include <stdio.h>                    // printf
// #include <stdlib.h>                   // free
// #include <string.h>                   // memset, strcat, strlen
// #include "../source/zstd/lib/zstd.h"  // presumes zstd library is installed
// #include "common.h"  // Helper functions, CHECK(), and CHECK_ZSTD()

// int main(int argc, const char** argv) {
//     // const char* const exeName = argv[0];

//     // if (argc < 2) {
//     //     printf("wrong arguments\n");
//     //     printf("usage:\n");
//     //     printf("%s FILE [LEVEL] [THREADS]\n", exeName);
//     //     return 1;
//     // }

//     int cLevel = 1;
//     int nbThreads = 1;

//     // if (argc >= 3) {
//     //     cLevel = atoi(argv[2]);
//     //     CHECK(cLevel != 0, "can't parse LEVEL!");
//     // }

//     // if (argc >= 4) {
//     //     nbThreads = atoi(argv[3]);
//     //     CHECK(nbThreads != 0, "can't parse THREADS!");
//     // }

//     // const char* const inFilename = argv[1];

//     char* inFilename = "./0";
//     char* outFilename = "./0.compress";
//     char* decompressName = "./0.decompress";

//     // char* const outFilename = createOutFilename_orDie(inFilename);
//     compressFile_orDie(inFilename, outFilename, cLevel, nbThreads);
//     decompressFile_orDie(outFilename, decompressName);

//     // free(outFilename); /* not strictly required, since program execution
//     // stops
//     //                     * there, but some static analyzer may complain
//     //                     otherwise
//     //                     */
//     return 0;
// }

// // #include <iostream>
// // int main() {
// //     std::cout << " eel" << std::endl;
// // }