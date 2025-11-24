// static_partition.cpp
// Static partitioned, multithreaded miner that follows mpi_mining I/O/workflow.
// Compile: g++ static_partition.cpp -o static_partition -std=c++17 -pthread -lssl -lcrypto -O2
// Run: ./static_partition [num_threads] [difficulty] [total_nonces]
// Example: ./static_partition 8 4 5000000
//
// Output:
//  logs/miner_<id>.log
//  logs/final_result.txt

#include <openssl/sha.h>
#include <atomic>
#include <chrono>
#include <cstdint>
#include <fstream>
#include <iomanip>
#include <iostream>
#include <mutex>
#include <sstream>
#include <string>
#include <thread>
#include <vector>
#include <filesystem>

using namespace std;
using steady_clock = chrono::steady_clock;

static const string LOG_DIR = "logs_static/";

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
    // MATCH MPI miner input format: data + to_string(nonce)
    string input = data + to_string(nonce);
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
        // Treat whole line as transaction data; txid = "tx<N>"
        Transaction t;
        t.txid = "tx" + to_string(txs.size());
        t.data = line;
        txs.push_back(t);
    }
    return txs;
}

// ---------------- static partition mining ----------------

atomic<bool> global_found(false);
atomic<uint64_t> global_nonce(0);
atomic<uint64_t> total_hashes(0);

struct MinerArgs {
    int id;
    uint64_t start;
    uint64_t end; // exclusive
    const Transaction *tx;
    int difficulty;
};

void miner_thread(const MinerArgs &args) {
    string logf = LOG_DIR + "miner_" + to_string(args.id) + ".log";
    append_log(logf, now_iso() + ",START," + to_string(args.start) + "," + to_string(args.end));
    uint64_t hashes = 0;
    for (uint64_t n = args.start; n < args.end && !global_found.load(); ++n) {
        string h = sha256_hex(args.tx->data, n);
        hashes++;
        total_hashes++;
        if (valid_hash(h, args.difficulty)) {
            bool expected = false;
            if (global_found.compare_exchange_strong(expected, true)) {
                global_nonce = n;

                // ★ Console output: concise and similar to MPI miner
                cout << "\n=== VALID BLOCK FOUND (STATIC) ===\n";
                cout << "Winning nonce: " << n << "\n";
                cout << "Winning hash : " << h << "\n\n";

                append_log(logf, now_iso() + ",FOUND," + to_string(n) + "," + h);
                append_log(LOG_DIR + "final_result.txt", "Nonce: " + to_string(n));
                append_log(LOG_DIR + "final_result.txt", "Hash: " + h);
                append_log(LOG_DIR + "final_result.txt", "Found by: Thread " + to_string(args.id));
                // Additional fields will be appended by main
            }
            break;
        }

        // occasional progress
        if ((hashes % 100000) == 0) {
            append_log(logf, now_iso() + ",PROGRESS," + to_string(n));
        }
    }
    append_log(logf, now_iso() + ",STOP,hashes=" + to_string(hashes));
}

int main(int argc, char **argv) {
    // defaults
    int num_threads = 8;
    int difficulty = 4;
    uint64_t total_nonces = 2000000ULL;

    if (argc >= 2) num_threads = stoi(argv[1]);
    if (argc >= 3) difficulty = stoi(argv[2]);
    if (argc >= 4) total_nonces = stoull(argv[3]);

    filesystem::create_directories(LOG_DIR);

    // clear logs
    ofstream final_clean(LOG_DIR + "final_result.txt", ios::trunc);
    final_clean.close();
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
    cout << "Static partition miner\n";
    cout << "Using transaction: " << tx.txid << " (" << tx.data << ")\n";
    cout << "Threads: " << num_threads << " difficulty: " << difficulty << " total_nonces: " << total_nonces << "\n";
    append_log(LOG_DIR + "static_summary.log", now_iso() + ",START,threads=" + to_string(num_threads));

    vector<thread> threads;
    uint64_t chunk = total_nonces / (uint64_t)num_threads;
    uint64_t start = 0;
    auto tstart = steady_clock::now();
    for (int i=0;i<num_threads;i++) {
        uint64_t s = start;
        uint64_t e = (i==num_threads-1) ? total_nonces : (s + chunk);
        MinerArgs a{i, s, e, &tx, difficulty};
        threads.emplace_back(miner_thread, a);
        start = e;
    }

    for (auto &t : threads) if (t.joinable()) t.join();

    auto tend = steady_clock::now();
    auto dur = chrono::duration_cast<chrono::milliseconds>(tend - tstart).count();

    append_log(LOG_DIR + "static_summary.log", now_iso() + ",END,duration_ms=" + to_string(dur) +
               ",found=" + (global_found? "1":"0") + ",nonce=" + to_string(global_nonce.load()) +
               ",total_hashes=" + to_string(total_hashes.load()));

    // Append MPI-like extra fields for easier comparison
    append_log(LOG_DIR + "final_result.txt", "Difficulty: " + to_string(difficulty));
    append_log(LOG_DIR + "final_result.txt", "Total hashes: " + to_string(total_hashes.load()));
    append_log(LOG_DIR + "final_result.txt", "Threads: " + to_string(num_threads));

    cout << "--- Summary ---\n";
    cout << "Duration (ms): " << dur << "\n";
    cout << "Block found: " << (global_found ? "YES":"NO") << "\n";
    if (global_found) cout << "Winning nonce: " << global_nonce.load() << "\n";
    cout << "Logs in " << LOG_DIR << "\n";
    return 0;
}
