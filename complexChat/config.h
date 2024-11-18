#ifndef COMPLEXCHATCONFIG_H__
#define COMPLEXCHATCONFIG_H__
#include <cstdint>
constexpr uint8_t magicNumber = 42;
constexpr int numSizeBytes = 2;
constexpr std::size_t messagePadding = 1 + numSizeBytes;
#endif