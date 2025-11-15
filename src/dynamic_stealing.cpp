// dynamic_stealing.cpp
// CS5320 – Distributed Bitcoin Mining — Dynamic Work Stealing
//
// Compile: g++ dynamic_stealing.cpp -o dynamic_stealing -std=c++17 -pthread -O2
// Run: ./dynamic_stealing
//
// Output logs: data/logs/

#include <atomic>
#include <chrono>
#include <cstdint>
#include <cstdlib>
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
#include <filesystem>

using steady_clock = std::chrono::steady_clock;

// ---------------- CONFIGURABLE PARAMETERS ----------------

static const int NUM_MINERS = 2;
static const uint64_t TOTAL_NONCES = 5000000ULL;    // global nonce space
static const int DIFFICULTY = 4;                    // leading hex zeroes
static const uint64_t STEAL_SIZE = 20000;           // how much work to steal
static const int HEARTBEAT_TIMEOUT_MS = 700;        // detect crash

static const double SIMULATE_CRASH_PROB = 0.0;      // set to 0.01 to test crash

// ---------------------------------------------------------

std::mutex log_mtx;

// timestamp to string
std::string now_iso() {
    using namespace std::chrono;
    auto t = system_clock::now();
    auto tt = system_clock::to_time_t(t);
    std::tm tm = *std::localtime(&tt);
    auto ms = duration_cast<milliseconds>(t.time_since_epoch()).count() % 1000;
    std::ostringstream oss;
    oss << std::put_time(&tm, "%Y-%m-%dT%H:%M:%S") << "." << std::setw(3) << std::setfill('0') << ms;
    return oss.str();
}

// log helper
void append_log(const std::string &file, const std::string &msg) {
    std::lock_guard<std::mutex> lock(log_mtx);
    std::ofstream f(file, std::ios::app);
    f << msg << "\n";
}

// hash simulation (fast)
std::string to_hex(const std::string &s) {
    static const char *hex = "0123456789abcdef";
    std::ostringstream oss;
    for (unsigned char c : s)
        oss << hex[(c >> 4) & 0xF] << hex[c & 0xF];
    return oss.str();
}

std::string simulated_hash(uint64_t nonce) {
    std::hash<std::string> h;
    size_t v = h(std::to_string(nonce));
    std::string bytes;
    bytes.resize(sizeof(size_t));
    for (size_t i = 0; i < sizeof(size_t); i++)
        bytes[i] = (v >> (8 * i)) & 0xFF;
    return to_hex(bytes);
}

bool valid_hash(const std::string &hx) {
    if ((int)hx.size() < DIFFICULTY) return false;
    for (int i = 0; i < DIFFICULTY; ++i)
        if (hx[i] != '0') return false;
    return true;
}

// ---------------- Shared State ----------------

struct WorkRange {
    uint64_t start = 0;
    uint64_t end = 0;      // exclusive
};

struct SharedState {
    std::atomic<bool> block_found{false};
    std::atomic<uint64_t> winning_nonce{0};

    std::vector<WorkRange> remaining;          // remaining work per miner
    std::vector<std::atomic<long long>> hb;    // heartbeats
    std::vector<std::atomic<bool>> alive;      // crash detection

    std::mutex work_mtx;

    SharedState(int n) : remaining(n), hb(n), alive(n) {
        for (int i = 0; i < n; i++) {
            hb[i].store(0);
            alive[i].store(true);
        }
    }
};

// ---------------- Work Stealing Logic ----------------

// Try to steal STEAL_SIZE work from victim
bool steal_work(SharedState &S, int thief, int victim, uint64_t &s, uint64_t &e) {
    std::lock_guard<std::mutex> lock(S.work_mtx);

    if (!S.alive[victim]) return false;

    uint64_t vs = S.remaining[victim].start;
    uint64_t ve = S.remaining[victim].end;
    if (ve <= vs) return false; // victim has no work

    uint64_t available = ve - vs;
    if (available < 2 * STEAL_SIZE) return false;

    // split from victim's end
    s = ve - STEAL_SIZE;
    e = ve;

    // shrink victim's space
    S.remaining[victim].end = s;

    append_log("data/logs/global.log",
               now_iso() + ",STEAL,thief=" + std::to_string(thief) +
               ",victim=" + std::to_string(victim) +
               ",range=" + std::to_string(s) + "-" + std::to_string(e));

    return true;
}

// ---------------- Miner Thread ----------------

void miner_thread(int id, SharedState &S, uint64_t start, uint64_t end) {
    std::string logfile = "data/logs/miner_" + std::to_string(id) + ".log";

    append_log(logfile, now_iso() + ",START");

    // initialize work range
    {
        std::lock_guard<std::mutex> lock(S.work_mtx);
        S.remaining[id].start = start;
        S.remaining[id].end = end;
    }

    std::mt19937_64 rng(id * 99991);
    std::bernoulli_distribution crash_d(SIMULATE_CRASH_PROB);

    uint64_t cur = start;

    while (!S.block_found) {
        long long now = std::chrono::duration_cast<std::chrono::milliseconds>(
                            steady_clock::now().time_since_epoch()).count();
        S.hb[id] = now;

        // check crash simulation
        if (SIMULATE_CRASH_PROB > 0.0 && crash_d(rng)) {
            append_log(logfile, now_iso() + ",CRASHED");
            S.alive[id] = false;
            return;
        }

        // get my range
        uint64_t s, e;
        {
            std::lock_guard<std::mutex> lock(S.work_mtx);
            s = S.remaining[id].start;
            e = S.remaining[id].end;
        }

        if (cur < s) cur = s;

        // no work left → try to steal
        if (cur >= e) {
            for (int victim = 0; victim < NUM_MINERS; victim++) {
                if (victim == id) continue;
                if (!S.alive[victim]) continue;

                uint64_t ns, ne;
                if (steal_work(S, id, victim, ns, ne)) {
                    // got new work
                    {
                        std::lock_guard<std::mutex> lock(S.work_mtx);
                        S.remaining[id].start = ns;
                        S.remaining[id].end = ne;
                    }
                    cur = ns;
                    break;
                }
            }

            // still no work → idle
            {
                std::lock_guard<std::mutex> lock(S.work_mtx);
                if (S.remaining[id].start >= S.remaining[id].end) {
                    std::this_thread::sleep_for(std::chrono::milliseconds(10));
                    continue;
                }
            }
            continue;
        }

        // ---- Perform hashing ----
        std::string h = simulated_hash(cur);
        if (valid_hash(h)) {
            bool expected = false;
            if (S.block_found.compare_exchange_strong(expected, true)) {
                S.winning_nonce = cur;
                append_log(logfile, now_iso() + ",FOUND," + std::to_string(cur));
                append_log("data/logs/global.log",
                           now_iso() + ",FOUND,miner=" + std::to_string(id) +
                           ",nonce=" + std::to_string(cur));
            }
            return;
        }

        if (cur % 150000 == 0)
            append_log(logfile, now_iso() + ",PROGRESS," + std::to_string(cur));

        cur++;
    }

    append_log(logfile, now_iso() + ",STOP");
}

int main() {
    std::filesystem::create_directories("data/logs");

    // clear old logs
    {
        std::ofstream g("data/logs/global.log", std::ios::trunc);
    }
    for (int i = 0; i < NUM_MINERS; i++) {
        std::ofstream f("data/logs/miner_" + std::to_string(i) + ".log", std::ios::trunc);
    }

    SharedState S(NUM_MINERS);

    // static initial ranges
    uint64_t chunk = TOTAL_NONCES / NUM_MINERS;
    std::vector<std::thread> miners;

    auto tstart = steady_clock::now();

    for (int i = 0; i < NUM_MINERS; i++) {
        uint64_t s = i * chunk;
        uint64_t e = (i == NUM_MINERS - 1 ? TOTAL_NONCES : (i + 1) * chunk);
        miners.emplace_back(miner_thread, i, std::ref(S), s, e);
    }

    for (auto &t : miners) t.join();

    auto tend = steady_clock::now();
    auto duration_ms =
        std::chrono::duration_cast<std::chrono::milliseconds>(tend - tstart).count();

    // summary
    std::ofstream sum("data/logs/dynamic_summary.csv", std::ios::trunc);
    sum << "duration_ms,found,nonce\n";
    sum << duration_ms << "," << (S.block_found ? 1 : 0) << ",";
    if (S.block_found) sum << S.winning_nonce;
    else sum << "NA";
    sum << "\n";

    std::cout << "\n--- Dynamic Work Stealing Summary ---\n";
    std::cout << "Duration (ms): " << duration_ms << "\n";
    std::cout << "Block found: " << (S.block_found ? "YES" : "NO") << "\n";
    if (S.block_found)
        std::cout << "Winning nonce: " << S.winning_nonce << "\n";
    std::cout << "Logs in data/logs/\n";

    return 0;
}
