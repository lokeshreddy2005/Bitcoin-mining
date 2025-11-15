#ifndef COMMON_HPP
#define COMMON_HPP


#include <atomic>
#include <cstdint>
#include <string>
#include <vector>
#include <chrono>


using steady_clock = std::chrono::steady_clock;
using ms = std::chrono::milliseconds;


struct WorkRange {
uint64_t start;
uint64_t end; // exclusive
bool claimed; // whether currently claimed by a miner
WorkRange(uint64_t s=0, uint64_t e=0): start(s), end(e), claimed(false) {}
};


#endif // COMMON_HPP