// pool_scheduling.cpp
// Pool-based scheduling simulation (single-process, multi-threaded)
// Compile: g++ pool_scheduling.cpp -o pool_scheduling -std=c++17 -pthread -O2
// Run: ./pool_scheduling
//
// Logs written to data/logs/

#include <atomic>
#include <chrono>
#include <cstdint>
#include <cstdlib>
#include <deque>
#include <fstream>
#include <functional>
#include <iomanip>
#include <iostream>
#include <map>
#include <mutex>
#include <random>
#include <sstream>
#include <string>
#include <thread>
#include <vector>
#include <condition_variable>
#include <filesystem>

using steady_clock = std::chrono::steady_clock;
using ms = std::chrono::milliseconds;

// ---------------- CONFIG ----------------
static const int NUM_MINERS = 4;
static const uint64_t TOTAL_NONCES = 5000000ULL;
static const int DIFFICULTY = 4;
static const uint64_t POOL_CHUNK = 20000ULL;               // size of each job chunk
static const int HEARTBEAT_INTERVAL_MS = 100;
static const int HEARTBEAT_TIMEOUT_MS = 800;               // pool detects dead miner
static const double SIMULATE_CRASH_PROB = 0.0;             // set >0 to simulate miner crash
static const int POOL_MONITOR_INTERVAL_MS = 200;          // how often pool checks for dead miners

// ---------------- utility ----------------
std::mutex g_log_mtx;
std::string now_iso() {
    using namespace std::chrono;
    auto t = std::chrono::system_clock::now();
    auto tt = std::chrono::system_clock::to_time_t(t);
    std::tm tm = *std::localtime(&tt);
    auto ms_part = duration_cast<milliseconds>(t.time_since_epoch()).count() % 1000;
    std::ostringstream oss;
    oss << std::put_time(&tm, "%Y-%m-%dT%H:%M:%S") << "." << std::setw(3) << std::setfill('0') << ms_part;
    return oss.str();
}
void append_log(const std::string &fname, const std::string &line) {
    std::lock_guard<std::mutex> g(g_log_mtx);
    std::ofstream ofs(fname, std::ios::app);
    if (!ofs) return;
    ofs << line << "\n";
}

// simulated hash helpers (fast, deterministic-ish)
std::string to_hex(const std::string &s) {
    static const char *hex = "0123456789abcdef";
    std::ostringstream oss;
    for (unsigned char c : s) oss << hex[(c >> 4) & 0xF] << hex[c & 0xF];
    return oss.str();
}
std::string simulated_hash(uint64_t nonce) {
    std::hash<std::string> h;
    size_t v = h(std::to_string(nonce));
    std::string bytes; bytes.resize(sizeof(size_t));
    for (size_t i = 0; i < sizeof(size_t); ++i) bytes[i] = char((v >> (i*8)) & 0xFF);
    return to_hex(bytes);
}
bool valid_hash(const std::string &hexstr) {
    if ((int)hexstr.size() < DIFFICULTY) return false;
    for (int i = 0; i < DIFFICULTY; ++i) if (hexstr[i] != '0') return false;
    return true;
}

// ---------------- job & pool data ----------------
struct Job {
    uint64_t id;
    uint64_t start;
    uint64_t end; // exclusive
    int assigned_to; // -1 if unassigned
    long long assigned_time_ms;
    Job() : id(0), start(0), end(0), assigned_to(-1), assigned_time_ms(0) {}
    Job(uint64_t id_, uint64_t s_, uint64_t e_) : id(id_), start(s_), end(e_), assigned_to(-1), assigned_time_ms(0) {}
};

struct Pool {
    std::deque<Job> queue;                // available jobs
    std::map<uint64_t, Job> outstanding;  // job_id -> job currently assigned
    std::mutex mtx;
    std::condition_variable cv_job;
    std::atomic<uint64_t> next_job_id{1};
    std::atomic<uint64_t> global_ptr{0}; // global nonce pointer
    std::atomic<bool> block_found{false};
    std::atomic<uint64_t> winning_nonce{UINT64_MAX};
    std::vector<std::atomic<long long>> last_hb; // last heartbeat (epoch ms) per miner
    std::vector<std::atomic<bool>> miner_alive;  // miner alive flags
    int n_miners;

    Pool(int n) : last_hb(n), miner_alive(n) {
        n_miners = n;
        for (int i = 0; i < n; ++i) {
            last_hb[i].store(0);
            miner_alive[i].store(true);
        }
    }

    // create jobs filling the queue until TOTAL_NONCES consumed
    void init_jobs() {
        uint64_t total = TOTAL_NONCES;
        uint64_t ptr = 0;
        while (ptr < total) {
            uint64_t s = ptr;
            uint64_t e = std::min(ptr + POOL_CHUNK, total);
            uint64_t jid = next_job_id.fetch_add(1);
            queue.emplace_back(jid, s, e);
            ptr = e;
        }
        append_log("data/logs/pool.log", now_iso() + ",INIT_JOBS,total_jobs=" + std::to_string(queue.size()));
    }

    // miner requests a job (blocks until job available or block_found)
    // returns job id==0 if no job (e.g., block found)
    Job request_job(int miner_id) {
        std::unique_lock<std::mutex> lock(mtx);
        // wake if jobs available or block found
        cv_job.wait(lock, [&]() { return !queue.empty() || block_found.load(); });
        if (block_found.load()) return Job(); // id==0 indicates no job
        Job job = queue.front();
        queue.pop_front();
        job.assigned_to = miner_id;
        job.assigned_time_ms = std::chrono::duration_cast<std::chrono::milliseconds>(
            steady_clock::now().time_since_epoch()).count();
        outstanding[job.id] = job;
        append_log("data/logs/pool.log", now_iso() + ",ASSIGN,job=" + std::to_string(job.id) +
                                           ",range=" + std::to_string(job.start) + "-" + std::to_string(job.end) +
                                           ",to=" + std::to_string(miner_id));
        return job;
    }

    // miner reports job completion (found==true if miner found a valid nonce; nonce is meaningful when found)
    void report_result(int miner_id, uint64_t job_id, bool found, uint64_t nonce, uint64_t hashes_done) {
        std::lock_guard<std::mutex> lock(mtx);
        auto it = outstanding.find(job_id);
        if (it != outstanding.end()) {
            // remove from outstanding
            outstanding.erase(it);
        }
        append_log("data/logs/pool.log", now_iso() + ",REPORT,miner=" + std::to_string(miner_id) +
                                           ",job=" + std::to_string(job_id) +
                                           ",found=" + (found ? "1" : "0") +
                                           (found ? (",nonce=" + std::to_string(nonce)) : "") +
                                           ",hashes=" + std::to_string(hashes_done));
        if (found) {
            bool expected = false;
            if (block_found.compare_exchange_strong(expected, true)) {
                winning_nonce.store(nonce);
                append_log("data/logs/global.log", now_iso() + ",FOUND,miner=" + std::to_string(miner_id) + ",nonce=" + std::to_string(nonce));
                // wake all waiting miners
                cv_job.notify_all();
            }
        } else {
            // job finished normally, nothing else
        }
    }

    // pool monitor: periodically check heartbeats and reclaim outstanding jobs from crashed miners
    void monitor_loop() {
        while (!block_found.load()) {
            std::this_thread::sleep_for(std::chrono::milliseconds(POOL_MONITOR_INTERVAL_MS));
            long long now = std::chrono::duration_cast<std::chrono::milliseconds>(steady_clock::now().time_since_epoch()).count();
            std::vector<int> dead;
            {
                std::lock_guard<std::mutex> lock(mtx);
                for (int i = 0; i < n_miners; ++i) {
                    if (!miner_alive[i].load()) continue;
                    long long hb = last_hb[i].load();
                    if (hb == 0 || (now - hb > HEARTBEAT_TIMEOUT_MS)) {
                        miner_alive[i].store(false);
                        dead.push_back(i);
                        append_log("data/logs/pool.log", now_iso() + ",DETECT_DEAD,miner=" + std::to_string(i));
                    }
                }
                // reclaim outstanding jobs assigned to dead miners
                if (!dead.empty()) {
                    std::vector<uint64_t> to_requeue;
                    for (auto it = outstanding.begin(); it != outstanding.end();) {
                        if (std::find(dead.begin(), dead.end(), it->second.assigned_to) != dead.end()) {
                            append_log("data/logs/pool.log", now_iso() + ",RECLAIM,job=" + std::to_string(it->second.id) +
                                                               ",from=" + std::to_string(it->second.assigned_to));
                            // push back the remaining portion of job as new job
                            Job recovered = it->second;
                            recovered.assigned_to = -1;
                            recovered.assigned_time_ms = 0;
                            queue.emplace_back(recovered);
                            it = outstanding.erase(it);
                        } else ++it;
                    }
                    if (!to_requeue.empty()) {
                        // not used; kept for clarity
                    }
                    if (!queue.empty()) cv_job.notify_all();
                }
            }
        }
    }

    // update miner heartbeat
    void heartbeat(int miner_id) {
        long long now = std::chrono::duration_cast<std::chrono::milliseconds>(steady_clock::now().time_since_epoch()).count();
        last_hb[miner_id].store(now);
    }
};

// ---------------- Miner ----------------

void miner_loop(int id, Pool &pool) {
    std::string mylog = "data/logs/miner_" + std::to_string(id) + ".log";
    append_log(mylog, now_iso() + ",START");

    std::mt19937_64 rng((uint64_t)id * 1009 ^ (uint64_t)now_iso().size());
    std::bernoulli_distribution crash_dist(SIMULATE_CRASH_PROB);

    while (!pool.block_found.load()) {
        // send heartbeat
        pool.heartbeat(id);

        // request a job
        Job job = pool.request_job(id);
        if (job.id == 0) break; // block found by someone else

        append_log(mylog, now_iso() + ",GOT_JOB,id=" + std::to_string(job.id) + ",range=" + std::to_string(job.start) + "-" + std::to_string(job.end));

        // do hashing through the job range (stop early if pool.block_found)
        uint64_t found_nonce = UINT64_MAX;
        uint64_t hashes = 0;
        for (uint64_t n = job.start; n < job.end; ++n) {
            if (pool.block_found.load()) break;
            std::string h = simulated_hash(n);
            hashes++;
            if (valid_hash(h)) {
                found_nonce = n;
                // report immediately
                pool.report_result(id, job.id, true, n, hashes);
                append_log(mylog, now_iso() + ",FOUND,nonce=" + std::to_string(n) + ",hashes=" + std::to_string(hashes));
                break;
            }
            if ((hashes & 0xFFFF) == 0) {
                // periodic heartbeat update while working
                pool.heartbeat(id);
            }
        }
        if (found_nonce != UINT64_MAX) break;

        // job finished without finding
        pool.report_result(id, job.id, false, 0, hashes);
        append_log(mylog, now_iso() + ",DONE_JOB,id=" + std::to_string(job.id) + ",hashes=" + std::to_string(hashes));

        // optionally simulate crash (before next request) for testing fault tolerance
        if (SIMULATE_CRASH_PROB > 0.0 && crash_dist(rng)) {
            append_log(mylog, now_iso() + ",CRASH_SIMULATED");
            pool.mtx.lock(); // mark miner dead quickly
            pool.miner_alive[id].store(false);
            pool.mtx.unlock();
            return;
        }
    }

    append_log(mylog, now_iso() + ",STOP");
}

// ---------------- main ----------------

int main() {
    std::filesystem::create_directories("data/logs");
    // clear logs
    {
        std::ofstream ofs("data/logs/global.log", std::ios::trunc); (void)ofs;
        std::ofstream pof("data/logs/pool.log", std::ios::trunc); (void)pof;
    }
    for (int i = 0; i < NUM_MINERS; ++i) {
        std::ofstream mf("data/logs/miner_" + std::to_string(i) + ".log", std::ios::trunc); (void)mf;
    }

    Pool pool(NUM_MINERS);
    pool.init_jobs();

    // start pool monitor thread
    std::thread monitor_thr(&Pool::monitor_loop, &pool);

    // start miner threads
    std::vector<std::thread> miners;
    for (int i = 0; i < NUM_MINERS; ++i) {
        miners.emplace_back(miner_loop, i, std::ref(pool));
    }

    // join miners
    for (auto &t : miners) if (t.joinable()) t.join();

    // notify monitor to stop (block_found will stop it)
    if (monitor_thr.joinable()) monitor_thr.join();

    // summary
    auto dur_ms = 0LL; // we didn't measure precisely here; user can extend if desired
    std::ofstream sumf("data/logs/pool_summary.csv", std::ios::trunc);
    sumf << "found,winning_nonce\n";
    sumf << (pool.block_found.load() ? 1 : 0) << ",";
    if (pool.block_found.load()) sumf << pool.winning_nonce.load();
    else sumf << "NA";
    sumf << "\n";

    std::cout << "\n--- Pool Scheduling Summary ---\n";
    std::cout << "Block found: " << (pool.block_found.load() ? "YES" : "NO") << "\n";
    if (pool.block_found.load()) std::cout << "Winning nonce: " << pool.winning_nonce.load() << "\n";
    std::cout << "Logs: data/logs/ (global.log, pool.log, miner_*.log, pool_summary.csv)\n";

    return 0;
}
