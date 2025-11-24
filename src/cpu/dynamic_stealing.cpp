// dynamic_stealing.cpp
// Multithreaded dynamic work-stealing miner adapted to mpi_mining I/O/workflow.
// Compile: g++ dynamic_stealing.cpp -o dynamic_stealing -std=c++17 -pthread -lssl -lcrypto -O2
// Run: ./dynamic_stealing [num_threads] [difficulty] [total_nonces] [chunk_size]
// Example: ./dynamic_stealing 4 4 5000000 20000
//
// Output:
//  logs/miner_<id>.log
//  logs/final_result.txt

#include <openssl/sha.h>
#include <atomic>
#include <chrono>
#include <cstdint>
#include <deque>
#include <fstream>
#include <iomanip>
#include <iostream>
#include <mutex>
#include <random>
#include <sstream>
#include <string>
#include <thread>
#include <vector>
#include <filesystem>

using namespace std;
using steady_clock = chrono::steady_clock;

static const string LOG_DIR = "logs_dynamic/";

// ---------------- helpers ----------------

string now_iso() {
    using namespace chrono;
    auto t = system_clock::now();
    auto tt = system_clock::to_time_t(t);
    std::tm tm = *std::localtime(&tt);
    auto ms = duration_cast<milliseconds>(t.time_since_epoch()).count() % 1000;
    ostringstream oss;
    oss << put_time(&tm, "%Y-%m-%dT%H:%M:%S") << "." << setw(3) << setfill('0') << ms;
    return oss.str();
}

void append_log(const string &path, const string &line) {
    static mutex m;
    lock_guard<mutex> lg(m);
    ofstream f(path, ios::app);
    if (f) f << line << "\n";
}

// ===== Modified: use same SHA input format as MPI miner (no colon) =====
string sha256_hex(const string &data, uint64_t nonce) {
    string input = data + to_string(nonce);   // MATCH MPI miner
    unsigned char hash[SHA256_DIGEST_LENGTH];
    SHA256((const unsigned char*)input.data(), input.size(), hash);
    ostringstream oss;
    oss<< hex << setfill('0');
    for (int i=0;i<SHA256_DIGEST_LENGTH;i++) oss << setw(2) << (int)hash[i];
    return oss.str();
}

bool valid_hash(const string &hexstr, int difficulty) {
    if ((int)hexstr.size() < difficulty) return false;
    for (int i=0;i<difficulty;i++) if (hexstr[i] != '0') return false;
    return true;
}

// ---------------- transaction input ----------------

struct Transaction {
    string txid;
    string data;
};

vector<Transaction> read_transactions(const string &fname) {
    vector<Transaction> txs;
    ifstream f(fname);
    if (!f) return txs;
    string line;
    while (getline(f, line)) {
        if (line.empty()) continue;
        Transaction t;
        t.txid = "tx" + to_string(txs.size());
        t.data = line;
        txs.push_back(t);
    }
    return txs;
}

// ---------------- work structures ----------------

struct WorkRange {
    uint64_t start;
    uint64_t end; // exclusive
    WorkRange() : start(0), end(0) {}
    WorkRange(uint64_t s, uint64_t e): start(s), end(e) {}
};

atomic<bool> global_found(false);
atomic<uint64_t> global_nonce(0);
atomic<uint64_t> total_hashes(0);

// Each worker keeps a deque of assigned chunks; leader initially fills global queue
struct WorkerState {
    deque<WorkRange> q;
    mutex mtx;
};

void miner_thread(int id, WorkerState &self, vector<WorkerState> &workers, const Transaction &tx,
                  int difficulty, int num_threads) {
    string logf = LOG_DIR + "miner_" + to_string(id) + ".log";
    append_log(logf, now_iso() + ",START");

    std::mt19937_64 rng((uint64_t)id ^ chrono::duration_cast<chrono::milliseconds>(
        steady_clock::now().time_since_epoch()).count());
    uniform_int_distribution<int> victim_dist(0, max(0, num_threads-1));

    while (!global_found.load()) {
        WorkRange work{0,0};
        // take from own queue if available
        {
            lock_guard<mutex> lg(self.mtx);
            if (!self.q.empty()) {
                work = self.q.front();
                self.q.pop_front();
            }
        }

        // If no work, attempt to steal from others
        if (work.end <= work.start) {
            bool stolen = false;
            // Try random victims first to avoid contention
            for (int attempt = 0; attempt < num_threads; ++attempt) {
                int victim = (id + 1 + attempt) % num_threads;
                if (victim == id) continue;
                WorkerState &vs = workers[victim];

                // lock victim temporarily
                unique_lock<mutex> vlock(vs.mtx, std::try_to_lock);
                if (!vlock || vs.q.empty()) continue;

                // if victim has more than 1 chunk, steal last chunk
                if (vs.q.size() >= 2) {
                    WorkRange stolen_range = vs.q.back();
                    vs.q.pop_back();
                    {
                        lock_guard<mutex> lg(self.mtx);
                        self.q.push_back(stolen_range);
                    }
                    append_log(LOG_DIR + "global.log", now_iso() + ",STEAL,thief=" + to_string(id) +
                               ",victim=" + to_string(victim) + ",range=" + to_string(stolen_range.start) +
                               "-" + to_string(stolen_range.end));
                    stolen = true;
                    break;
                } else if (vs.q.size() == 1) {
                    // If the victim only has one chunk and it is large, split it
                    WorkRange r = vs.q.front();
                    if (r.end - r.start > 1) {
                        uint64_t mid = r.start + (r.end - r.start) / 2;
                        // shrink victim
                        vs.q.front().end = mid;
                        WorkRange give(mid, r.end);
                        {
                            lock_guard<mutex> lg(self.mtx);
                            self.q.push_back(give);
                        }
                        append_log(LOG_DIR + "global.log", now_iso() + ",STEAL_SPLIT,thief=" + to_string(id) +
                                   ",victim=" + to_string(victim) + ",range=" + to_string(give.start) + "-" + to_string(give.end));
                        stolen = true;
                        break;
                    }
                }
            }
            if (!stolen) {
                // no work found; idle briefly
                this_thread::sleep_for(chrono::milliseconds(5));
                continue;
            } else {
                // try to fetch our new work again
                lock_guard<mutex> lg(self.mtx);
                if (!self.q.empty()) {
                    work = self.q.front();
                    self.q.pop_front();
                } else {
                    continue;
                }
            }
        }

        // Mine the work range
        for (uint64_t n = work.start; n < work.end && !global_found.load(); ++n) {
            string h = sha256_hex(tx.data, n);
            total_hashes++;
            if (valid_hash(h, difficulty)) {
                bool expected = false;
                if (global_found.compare_exchange_strong(expected, true)) {
                    global_nonce = n;

                    // Console output: concise
                    cout << "\n=== VALID BLOCK FOUND (DYNAMIC) ===\n";
                    cout << "Winning nonce: " << n << "\n";
                    cout << "Winning hash : " << h << "\n";
                    cout << "Found by miner thread: " << id << "\n\n";

                    // Log existing lines (keeps old output)
                    append_log(logf, now_iso() + ",FOUND," + to_string(n) + "," + h);
                    append_log(LOG_DIR + "final_result.txt", "Nonce: " + to_string(n));
                    append_log(LOG_DIR + "final_result.txt", "Hash: " + h);
                    append_log(LOG_DIR + "final_result.txt", "Found by: Thread " + to_string(id));
                    append_log(LOG_DIR + "global.log", now_iso() + ",FOUND,miner=" + to_string(id) + ",nonce=" + to_string(n));

                    // ★ NEW: also write explicit "Winning" entries into final_result.txt
                    append_log(LOG_DIR + "final_result.txt", "Winning nonce: " + to_string(n));
                    append_log(LOG_DIR + "final_result.txt", "Winning hash: " + h);
                    // Additional fields will be appended by main
                }
                break;
            }
            if ((n - work.start) % 100000 == 0) append_log(logf, now_iso() + ",PROGRESS," + to_string(n));
        }
    }

    append_log(logf, now_iso() + ",STOP");
}

int main(int argc, char **argv) {
    int num_threads = 4;
    int difficulty = 4;
    uint64_t total_nonces = 5000000ULL;
    uint64_t chunk_size = 20000;

    if (argc >= 2) num_threads = stoi(argv[1]);
    if (argc >= 3) difficulty = stoi(argv[2]);
    if (argc >= 4) total_nonces = stoull(argv[3]);
    if (argc >= 5) chunk_size = stoull(argv[4]);

    filesystem::create_directories(LOG_DIR);

    // clear logs
    ofstream final_clean(LOG_DIR + "final_result.txt", ios::trunc);
    final_clean.close();
    ofstream glob(LOG_DIR + "global.log", ios::trunc);
    glob.close();
    for (int i=0;i<num_threads;i++) {
        ofstream f(LOG_DIR + "miner_" + to_string(i) + ".log", ios::trunc);
    }

    auto txs = read_transactions("../transactions.txt");
    if (txs.empty()) {
        cout << "ERROR: transactions.txt not found or empty. Put a transaction per line.\n";
        return 1;
    }
    const Transaction &tx = txs[0];

    // Print chosen transaction to match MPI workflow
    cout << "Dynamic work-stealing miner\n";
    cout << "Using transaction: " << tx.txid << " (" << tx.data << ")\n";
    cout << "Threads: " << num_threads << " difficulty: " << difficulty << " total_nonces: " << total_nonces << " chunk_size: " << chunk_size << "\n";

    // prepare workers
    vector<WorkerState> workers(num_threads);

    // create initial chunks and distribute roughly round-robin
    vector<WorkRange> initial;
    for (uint64_t s=0; s<total_nonces; s += chunk_size) {
        uint64_t e = min(total_nonces, s + chunk_size);
        initial.emplace_back(s,e);
    }
    for (size_t i=0;i<initial.size();++i) {
        int wid = i % num_threads;
        lock_guard<mutex> lg(workers[wid].mtx);
        workers[wid].q.push_back(initial[i]);
    }

    // start miners
    vector<thread> threads;
    auto tstart = steady_clock::now();
    for (int i=0;i<num_threads;i++) {
        threads.emplace_back(miner_thread, i, ref(workers[i]), ref(workers), ref(tx), difficulty, num_threads);
    }

    for (auto &t : threads) if (t.joinable()) t.join();

    auto tend = steady_clock::now();
    auto dur = chrono::duration_cast<chrono::milliseconds>(tend - tstart).count();

    // summary
    ofstream sum(LOG_DIR + "dynamic_summary.csv", ios::trunc);
    sum << "duration_ms,found,nonce,total_hashes,threads\n";
    sum << dur << "," << (global_found?1:0) << "," << (global_found?to_string(global_nonce.load()):string("NA")) << "," << total_hashes.load() << "," << num_threads << "\n";
    sum.close();

    // Append MPI-like extra fields for easier comparison
    append_log(LOG_DIR + "final_result.txt", "Difficulty: " + to_string(difficulty));
    append_log(LOG_DIR + "final_result.txt", "Total hashes: " + to_string(total_hashes.load()));
    append_log(LOG_DIR + "final_result.txt", "Threads: " + to_string(num_threads));

    cout << "\n--- Dynamic Summary ---\n";
    cout << "Duration (ms): " << dur << "\n";
    cout << "Block found: " << (global_found ? "YES":"NO") << "\n";
    if (global_found) cout << "Winning nonce: " << global_nonce.load() << "\n";
    cout << "Logs: " << LOG_DIR << "\n";
    return 0;
}
