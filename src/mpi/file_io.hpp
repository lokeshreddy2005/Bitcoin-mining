#ifndef FILE_IO_HPP
#define FILE_IO_HPP

#include "raft_state.hpp"
#include "mining_utils.hpp"
#include <fstream>
#include <iostream>
#include <sys/stat.h>
#include <vector>
#include <map>

using namespace std;

// forward declarations
extern int world_rank;
extern RaftState raft_state;
extern WorkStatusReport my_work_status;
extern vector<WorkRange> global_work_ranges;
extern ofstream log_file;
extern uint64_t global_max_nonce_checked;  

inline void ensure_directory(const string& dir) {
    mkdir(dir.c_str(), 0755);
}

inline void log_to_file(const string& msg) {
    if (!log_file.is_open()) {
        ensure_directory(LOG_DIR);
        string filename = LOG_DIR + "rank_" + to_string(world_rank) + ".log";
        log_file.open(filename, ios::out | ios::trunc);
    }
    
    auto now = chrono::system_clock::now();
    auto tt = chrono::system_clock::to_time_t(now);
    log_file << "[" << put_time(localtime(&tt), "%H:%M:%S") << "] "
             << "[Term " << raft_state.current_term << "][";
    
    if (raft_state.state == LEADER) log_file << "LEADER";
    else if (raft_state.state == CANDIDATE) log_file << "CANDIDATE";
    else log_file << "FOLLOWER";
    
    log_file << "] " << msg << endl;
    log_file.flush();
}

inline void save_global_work_state() {
    if (raft_state.state != LEADER) return;
    
    ensure_directory(CHECKPOINT_DIR);
    string filename = CHECKPOINT_DIR + "global_work_state_term_" + 
                      to_string(raft_state.current_term) + ".state";
    
    ofstream state_file(filename);
    if (!state_file) return;
    
    state_file << "# Global Work State - Term " << raft_state.current_term << "\n";
    state_file << "# Max nonce checked: " << global_max_nonce_checked << "\n";
    state_file << "# Format: start_nonce,end_nonce,progress_nonce,worker_id,completed,in_progress\n";
    
    for (const auto& range : global_work_ranges) {
        state_file << range.start_nonce << ","
                   << range.end_nonce << ","
                   << range.progress_nonce << ","
                   << range.worker_id << ","
                   << (range.completed ? 1 : 0) << ","
                   << (range.in_progress ? 1 : 0) << "\n";
    }
    
    state_file.close();
}

inline void load_global_work_state() {
    string pattern = CHECKPOINT_DIR + "global_work_state_term_";
    
    for (int term = raft_state.current_term; term >= 0; term--) {
        string filename = pattern + to_string(term) + ".state";
        ifstream state_file(filename);
        
        if (!state_file) continue;
        
        global_work_ranges.clear();
        string line;
        
        while (getline(state_file, line)) {
            if (line.empty() || line[0] == '#') {
                // Try to extract max nonce from comment
                if (line.find("Max nonce checked:") != string::npos) {
                    size_t pos = line.find(":");
                    if (pos != string::npos) {
                        global_max_nonce_checked = stoull(line.substr(pos + 2));
                    }
                }
                continue;
            }
            
            WorkRange range;
            char comma;
            int completed, in_progress;
            
            istringstream iss(line);
            iss >> range.start_nonce >> comma
                >> range.end_nonce >> comma
                >> range.progress_nonce >> comma
                >> range.worker_id >> comma
                >> completed >> comma
                >> in_progress;
            
            range.completed = (completed == 1);
            range.in_progress = (in_progress == 1);
            range.term_assigned = term;
            
            global_work_ranges.push_back(range);
        }
        
        state_file.close();
        log_to_file("Loaded work state from term " + to_string(term) + 
                   ": " + to_string(global_work_ranges.size()) + " ranges, " +
                   "max checked: " + to_string(global_max_nonce_checked));
        return;
    }
    
    log_to_file("No previous work state, starting fresh");
}

inline vector<Transaction> read_transactions(const string& filename) {
    vector<Transaction> txs;
    
    vector<string> paths = {
        filename,
        "../" + filename,
        "../../" + filename
    };
    
    ifstream infile;
    string found_path;
    
    for (const auto& path : paths) {
        infile.open(path);
        if (infile.is_open()) {
            found_path = path;
            break;
        }
    }
    
    if (!infile.is_open()) {
        if (world_rank == 0) {
            cerr << "Error: Cannot open transaction file" << endl;
        }
        return txs;
    }
    
    string line;
    while (getline(infile, line)) {
        if (line.empty() || line[0] == '#') continue;
        Transaction tx;
        istringstream iss(line);
        getline(iss, tx.txid, ',');
        getline(iss, tx.data, ',');
        getline(iss, tx.prev_hash, ',');
        if (!tx.txid.empty() && !tx.data.empty()) txs.push_back(tx);
    }
    infile.close();
    
    if (world_rank == 0 && !txs.empty()) {
        log_to_file("Loaded " + to_string(txs.size()) + " transactions from: " + found_path);
    }
    
    return txs;
}

#endif
