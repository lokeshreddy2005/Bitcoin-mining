#ifndef MINING_UTILS_HPP
#define MINING_UTILS_HPP

#include <openssl/sha.h>
#include <string>
#include <sstream>
#include <iomanip>
#include <chrono>
#include <random>
#include <ctime>

using namespace std;
using steady_clock = chrono::steady_clock;

const int ELECTION_TIMEOUT_MIN_MS = 1500;
const int ELECTION_TIMEOUT_MAX_MS = 3000;
const int HEARTBEAT_INTERVAL_MS = 300;
const int HEARTBEAT_TIMEOUT_MS = 1500;
const int CHECKPOINT_INTERVAL_MS = 1000;
const uint64_t NONCE_RANGE_SIZE = 500000ULL;
const int MESSAGE_CHECK_FREQUENCY = 10000;
const std::string LOG_DIR = "logs/";
const std::string CHECKPOINT_DIR = "checkpoints/";


inline string to_hex(const unsigned char* data, size_t len) {
    ostringstream oss;
    for (size_t i = 0; i < len; ++i) {
        oss << hex << setw(2) << setfill('0') << (int)data[i];
    }
    return oss.str();
}

inline string sha256_hash(const string& data, uint64_t nonce) {
    string input = data + to_string(nonce);
    unsigned char hash[SHA256_DIGEST_LENGTH];
    SHA256((unsigned char*)input.c_str(), input.length(), hash);
    return to_hex(hash, SHA256_DIGEST_LENGTH);
}

inline bool valid_hash(const string& hash_hex, int diff) {
    if ((int)hash_hex.size() < diff) return false;
    for (int i = 0; i < diff; ++i) {
        if (hash_hex[i] != '0') return false;
    }
    return true;
}

inline long long current_time_ms() {
    return chrono::duration_cast<chrono::milliseconds>(
        steady_clock::now().time_since_epoch()).count();
}

inline string get_timestamp() {
    auto now = chrono::system_clock::now();
    auto tt = chrono::system_clock::to_time_t(now);
    auto ms = chrono::duration_cast<chrono::milliseconds>(
        now.time_since_epoch()).count() % 1000;
    
    stringstream ss;
    ss << put_time(localtime(&tt), "%Y%m%d_%H%M%S");
    ss << "_" << setfill('0') << setw(3) << ms;
    return ss.str();
}

inline int get_random_election_timeout(int rank) {
    random_device rd;
    mt19937 gen(rd() + rank * 1000 + time(NULL));
    uniform_int_distribution<> dis(ELECTION_TIMEOUT_MIN_MS, ELECTION_TIMEOUT_MAX_MS);
    return dis(gen);
}

#endif 
