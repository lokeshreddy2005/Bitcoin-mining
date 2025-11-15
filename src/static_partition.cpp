// static_partition.cpp
// Distributed Bitcoin Mining — Static Partition (single-file, self-contained)
// Compile: g++ static_partition.cpp -o static_partition -std=c++17 -pthread -O2
// Run: ./static_partition
//
// Produces logs in data/logs/
// Configurable constants are near the top of the file.

#include <atomic>
#include <chrono>
#include <cstdint>
#include <fstream>
#include <functional>
#include <iomanip>
#include <iostream>
#include <mutex>
#include <random>
#include <sstream>
#include <string>
#include <thread>
#include <vector>
#include <cstdlib>
#include <filesystem>

using steady_clock = std::chrono::steady_clock;
using ms = std::chrono::milliseconds;

// ====== Configurable parameters ======
static const int NUM_MINERS = 8;                 // number of miner threads
static const uint64_t TOTAL_NONCES = 2000000ULL; // total nonce space (tune for runtime)
static const int DIFFICULTY = 3;                 // required leading hex '0' chars
static const int HEARTBEAT_MS = 100;             // heartbeat update interval (ms)
static const int HEARTBEAT_TIMEOUT_MS = 800;     // detection threshold (ms)
static const double SIMULATE_CRASH_PROB = 0.0;   // probability of crash per loop check

// Logging mutex for safe appends
static std::mutex g_log_mtx;

// ----------------- utility helpers -----------------

// iso timestamp (local) with milliseconds
static std::string now_iso() {
    using namespace std::chrono;
    auto t = std::chrono::system_clock::now();
    auto tt = std::chrono::system_clock::to_time_t(t);
    std::tm tm = *std::localtime(&tt);
    auto ms_part = duration_cast<milliseconds>(t.time_since_epoch()).count() % 1000;
    std::ostringstream oss;
    oss << std::put_time(&tm, "%Y-%m-%dT%H:%M:%S") << "." << std::setw(3) << std::setfill('0') << ms_part;
    return oss.str();
}

static long long now_ms_epoch() {
    return std::chrono::duration_cast<std::chrono::milliseconds>(steady_clock::now().time_since_epoch()).count();
}

static void append_log(const std::string &filename, const std::string &line) {
    std::lock_guard<std::mutex> g(g_log_mtx);
    std::ofstream ofs(filename, std::ios::app);
    if (!ofs) return;
    ofs << line << "\n";
}

// convert bytes (string) -> hex
static std::string to_hex(const std::string &s) {
    static const char *hex_digits = "0123456789abcdef";
    std::ostringstream oss;
    for (unsigned char c : s) {
        oss << hex_digits[(c >> 4) & 0xF] << hex_digits[c & 0xF];
    }
    return oss.str();
}

// simulated "hash" of a nonce (fast)
static std::string simulated_hash(uint64_t nonce) {
    std::hash<std::string> h;
    size_t v = h(std::to_string(nonce));
    // pack size_t into bytes (little endian)
    std::string bytes;
    bytes.resize(sizeof(size_t));
    for (size_t i = 0; i < sizeof(size_t); ++i) bytes[i] = char((v >> (i * 8)) & 0xFF);
    return to_hex(bytes);
}

static bool valid_hash(const std::string &hexstr, int difficulty) {
    if ((int)hexstr.size() < difficulty) return false;
    for (int i = 0; i < difficulty; ++i) if (hexstr[i] != '0') return false;
    return true;
}

// ----------------- WorkRange & SharedState -----------------

struct WorkRange {
    uint64_t start{0};
    uint64_t end{0}; // exclusive
    bool claimed{false};
    WorkRange() = default;
    WorkRange(uint64_t s, uint64_t e) : start(s), end(e), claimed(false) {}
};

struct SharedState {
    std::atomic<bool> block_found{false};
    std::atomic<uint64_t> winning_nonce{UINT64_MAX};

    std::vector<WorkRange> ranges;                  // OK: copyable
    std::vector<std::atomic<long long>> heartbeats; // MUST be fixed-size
    std::vector<std::atomic<bool>> alive;           // MUST be fixed-size

    std::mutex ranges_mtx;

    SharedState(int n)
        : ranges(n),
          heartbeats(n),
          alive(n)
    {
        for (int i = 0; i < n; ++i) {
            heartbeats[i].store(0LL);
            alive[i].store(true);
        }
    }
};

// Attempt to redistribute a dead miner's remaining work to the detector (self_id).
// This is intentionally simple: take half of remaining non-empty range and assign to detector
static void redistribute_dead_ranges(SharedState &S, int dead_id, int self_id) {
    std::lock_guard<std::mutex> g(S.ranges_mtx);

    if (dead_id < 0 || dead_id >= (int)S.ranges.size()) return;
    if (self_id < 0 || self_id >= (int)S.ranges.size()) return;

    WorkRange &wr_dead = S.ranges[dead_id];
    if (wr_dead.start >= wr_dead.end) return; // nothing left

    // Only claim if not already claimed
    if (wr_dead.claimed) return;
    wr_dead.claimed = true;

    uint64_t start = wr_dead.start;
    uint64_t end = wr_dead.end;
    uint64_t len = (end > start) ? (end - start) : 0;
    if (len == 0) {
        wr_dead.start = wr_dead.end = 0;
        return;
    }

    // Give half (or full if small) to the detector self_id
    uint64_t take = len / 2;
    if (take == 0) take = len;

    WorkRange &wr_self = S.ranges[self_id];
    // If self currently has no work, assign chunk as its full range
    if (wr_self.start >= wr_self.end) {
        wr_self.start = start;
        wr_self.end = start + take;
        wr_self.claimed = false;
    } else {
        // append chunk to the end of self's range (simple merge)
        // Note: this is a simple simulation scheme; it may create overlapping/addressing artifacts in more complex designs.
        uint64_t cur_s = wr_self.start;
        uint64_t cur_e = wr_self.end;
        wr_self.start = cur_s;
        wr_self.end = cur_e + take;
        wr_self.claimed = false;
    }

    // shrink dead's range
    wr_dead.start = start + take;
    if (wr_dead.start >= wr_dead.end) {
        wr_dead.start = wr_dead.end = 0;
    }

    append_log("data/logs/global.log", now_iso() + ",REDISTRIBUTE,dead=" + std::to_string(dead_id) +
                                        ",by=" + std::to_string(self_id) + ",took=" + std::to_string(take));
}

// Miner thread function
static void miner_thread_fn(int id, SharedState &S, uint64_t start_nonce, uint64_t end_nonce) {
    const std::string miner_log = "data/logs/miner_" + std::to_string(id) + ".log";
    append_log(miner_log, now_iso() + ",START," + std::to_string(start_nonce) + "," + std::to_string(end_nonce));

    // Initialize this miner's range slot
    {
        std::lock_guard<std::mutex> g(S.ranges_mtx);
        S.ranges[id].start = start_nonce;
        S.ranges[id].end = end_nonce;
        S.ranges[id].claimed = false;
    }

    std::mt19937_64 rng((uint64_t)id ^ (uint64_t)now_ms_epoch());
    std::bernoulli_distribution crash_dist(SIMULATE_CRASH_PROB);

    uint64_t local_next = start_nonce;

    while (!S.block_found.load(std::memory_order_acquire)) {
        // heartbeat: update my timestamp
        S.heartbeats[id].store(now_ms_epoch());

        // Check my assigned range
        uint64_t mine_start = 0, mine_end = 0;
        {
            std::lock_guard<std::mutex> g(S.ranges_mtx);
            mine_start = S.ranges[id].start;
            mine_end = S.ranges[id].end;
        }

        if (local_next < mine_start) local_next = mine_start;

        if (local_next >= mine_end) {
            // no local work: try detect dead peers and claim work
            bool claimed_work = false;
            long long now = now_ms_epoch();
            for (int peer = 0; peer < NUM_MINERS; ++peer) {
                if (peer == id) continue;
                if (!S.alive[peer].load()) continue; // already marked dead

                long long peer_hb = S.heartbeats[peer].load();
                if (peer_hb == 0 || (now - peer_hb > HEARTBEAT_TIMEOUT_MS)) {
                    // attempt to mark dead (exchange ensures one detector does redistribution)
                    bool expected = true;
                    if (S.alive[peer].exchange(false)) {
                        append_log(miner_log, now_iso() + ",DETECT_DEAD," + std::to_string(peer));
                        redistribute_dead_ranges(S, peer, id);
                        claimed_work = true;
                        break; // re-evaluate assigned ranges
                    }
                }
            }
            if (!claimed_work) {
                // idle briefly
                std::this_thread::sleep_for(std::chrono::milliseconds(5));
                continue;
            } else {
                continue; // picked up new assigned ranges, go to top
            }
        }

        // Do work on local_next
        std::string h = simulated_hash(local_next);
        if (valid_hash(h, DIFFICULTY)) {
            bool expected = false;
            if (S.block_found.compare_exchange_strong(expected, true)) {
                S.winning_nonce.store(local_next);
                append_log(miner_log, now_iso() + ",FOUND," + std::to_string(local_next) + ",hash=" + h);
                append_log("data/logs/global.log", now_iso() + ",FOUND,miner=" + std::to_string(id) +
                                                       ",nonce=" + std::to_string(local_next));
                break;
            } else {
                // someone else already set block_found
                break;
            }
        }

        // occasional progress logging to reduce logfile size
        if ((local_next % 100000) == 0) {
            append_log(miner_log, now_iso() + ",PROGRESS," + std::to_string(local_next));
        }

        local_next++;

        // optional simulated crash
        if (SIMULATE_CRASH_PROB > 0.0 && crash_dist(rng)) {
            append_log(miner_log, now_iso() + ",CRASH_SIMULATED");
            S.alive[id].store(false);
            return; // thread exits, simulating a crash
        }

        // light throttle to avoid burning CPU in tiny test environments
        // you can remove or lower this sleep for faster hashing
        // std::this_thread::sleep_for(std::chrono::microseconds(1));
    }

    append_log(miner_log, now_iso() + ",STOP");
}

int main(int argc, char **argv) {
    // prepare logs directory
    std::filesystem::create_directories("data/logs");

    // clear previous logs
    {
        std::ofstream ofs("data/logs/global.log", std::ios::trunc);
        (void)ofs;
    }
    for (int i = 0; i < NUM_MINERS; ++i) {
        std::string fname = "data/logs/miner_" + std::to_string(i) + ".log";
        std::ofstream ofs(fname, std::ios::trunc);
        (void)ofs;
    }

    SharedState S(NUM_MINERS);

    // Partition nonce space statically into NUM_MINERS chunks
    uint64_t chunk = (TOTAL_NONCES / (uint64_t)NUM_MINERS);
    for (int i = 0; i < NUM_MINERS; ++i) {
        uint64_t s = (uint64_t)i * chunk;
        uint64_t e = (i == NUM_MINERS - 1) ? TOTAL_NONCES : (uint64_t)(i + 1) * chunk;
        S.ranges[i].start = s;
        S.ranges[i].end = e;
        S.ranges[i].claimed = false;
    }

    // Start miner threads
    std::vector<std::thread> miners;
    auto tstart = steady_clock::now();
    for (int i = 0; i < NUM_MINERS; ++i) {
        miners.emplace_back(miner_thread_fn, i, std::ref(S), S.ranges[i].start, S.ranges[i].end);
    }

    // Wait for miners to finish
    for (auto &t : miners) {
        if (t.joinable()) t.join();
    }

    auto tend = steady_clock::now();
    auto dur = std::chrono::duration_cast<std::chrono::milliseconds>(tend - tstart).count();

    // Summarize results
    std::ofstream sumf("data/logs/trial_summary.csv", std::ios::trunc);
    sumf << "duration_ms,block_found,winning_nonce\n";
    sumf << dur << "," << (S.block_found.load() ? 1 : 0) << ",";
    if (S.block_found.load()) sumf << S.winning_nonce.load();
    else sumf << "NA";
    sumf << "\n";

    std::cout << "\n--- Trial Summary ---\n";
    std::cout << "Duration (ms): " << dur << "\n";
    std::cout << "Block found: " << (S.block_found.load() ? "YES" : "NO") << "\n";
    if (S.block_found.load()) std::cout << "Winning nonce: " << S.winning_nonce.load() << "\n";
    std::cout << "Logs: data/logs/\n";

    return 0;
}
