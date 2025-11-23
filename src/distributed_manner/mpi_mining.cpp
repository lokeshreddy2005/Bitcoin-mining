// mpi_mining.cpp - Complete fault-tolerant Bitcoin mining with Raft
// Compile: mpic++ mpi_mining.cpp -o mpi_mining -std=c++17 -lssl -lcrypto -O2 -pthread
// Run: mpirun -np 9 ./mpi_mining 4

#include <mpi.h>
#include <cstring>
#include "mining_utils.hpp"
#include "raft_state.hpp"
#include "file_io.hpp"
#include "work_manager.hpp"
#include <atomic>
#include <mutex>
#include <thread>
#include <map>

using namespace std;

// ========== GLOBAL STATE ==========
int world_rank, world_size;
RaftState raft_state;
atomic<bool> block_found(false);
atomic<bool> should_terminate(false);
atomic<uint64_t> total_hashes_computed(0);
uint64_t winning_nonce = 0;
string winning_hash;
int difficulty = 4;
vector<Transaction> transactions;
vector<WorkRange> global_work_ranges;
map<int, WorkStatusReport> worker_status;
WorkStatusReport my_work_status;
int global_sequence_num = 0;
mutex state_mutex;
ofstream log_file;

// CRITICAL: Track max nonce checked to prevent re-checking
uint64_t global_max_nonce_checked = 0;

// Set to UINT64_MAX for infinite mining (like real miners)
uint64_t max_nonce_limit = UINT64_MAX;

// ========== RAFT FUNCTIONS ==========

void start_election() {
    lock_guard<mutex> lock(state_mutex);
    
    raft_state.state = CANDIDATE;
    raft_state.current_term++;
    raft_state.voted_for = world_rank;
    raft_state.votes_received = 1;
    raft_state.election_timeout_ms = get_random_election_timeout(world_rank);
    raft_state.last_heartbeat_time = current_time_ms();
    
    log_to_file("Starting election for term " + to_string(raft_state.current_term));
    
    VoteRequest vote_req;
    vote_req.term = raft_state.current_term;
    vote_req.candidate_id = world_rank;
    
    for (int i = 0; i < world_size; ++i) {
        if (i != world_rank) {
            MPI_Send(&vote_req, sizeof(VoteRequest), MPI_BYTE, i, 
                     TAG_REQUEST_VOTE, MPI_COMM_WORLD);
        }
    }
}

void handle_vote_request() {
    VoteRequest vote_req;
    MPI_Status status;
    MPI_Recv(&vote_req, sizeof(VoteRequest), MPI_BYTE, MPI_ANY_SOURCE, 
             TAG_REQUEST_VOTE, MPI_COMM_WORLD, &status);
    
    lock_guard<mutex> lock(state_mutex);
    bool vote_granted = false;
    
    if (vote_req.term > raft_state.current_term) {
        raft_state.current_term = vote_req.term;
        raft_state.state = FOLLOWER;
        raft_state.voted_for = -1;
    }
    
    if (vote_req.term == raft_state.current_term &&
        (raft_state.voted_for == -1 || raft_state.voted_for == vote_req.candidate_id)) {
        vote_granted = true;
        raft_state.voted_for = vote_req.candidate_id;
        raft_state.last_heartbeat_time = current_time_ms();
        log_to_file("Granted vote to " + to_string(vote_req.candidate_id));
    }
    
    VoteReply reply;
    reply.term = raft_state.current_term;
    reply.vote_granted = vote_granted;
    
    MPI_Send(&reply, sizeof(VoteReply), MPI_BYTE, status.MPI_SOURCE, 
             TAG_VOTE_REPLY, MPI_COMM_WORLD);
}

void handle_vote_reply() {
    VoteReply reply;
    MPI_Status status;
    MPI_Recv(&reply, sizeof(VoteReply), MPI_BYTE, MPI_ANY_SOURCE, 
             TAG_VOTE_REPLY, MPI_COMM_WORLD, &status);
    
    lock_guard<mutex> lock(state_mutex);
    
    if (raft_state.state != CANDIDATE) return;
    
    if (reply.term > raft_state.current_term) {
        raft_state.current_term = reply.term;
        raft_state.state = FOLLOWER;
        raft_state.voted_for = -1;
        return;
    }
    
    if (reply.term == raft_state.current_term && reply.vote_granted) {
        raft_state.votes_received++;
        log_to_file("Received vote (total: " + to_string(raft_state.votes_received) + ")");
        
        if (raft_state.votes_received > world_size / 2) {
            raft_state.state = LEADER;
            raft_state.leader_id = world_rank;
            
            log_to_file("*** ELECTED AS LEADER for term " + to_string(raft_state.current_term) + " ***");
            
            load_global_work_state();
            if (global_work_ranges.empty()) {
                initialize_work_ranges();
            }
            
            for (int i = 0; i < world_size; ++i) {
                if (i != world_rank) {
                    int hb = raft_state.current_term;
                    MPI_Send(&hb, 1, MPI_INT, i, TAG_HEARTBEAT, MPI_COMM_WORLD);
                }
            }
        }
    }
}

void send_heartbeats() {
    if (raft_state.state != LEADER) return;
    
    for (int i = 0; i < world_size; ++i) {
        if (i != world_rank) {
            int hb = raft_state.current_term;
            MPI_Send(&hb, 1, MPI_INT, i, TAG_HEARTBEAT, MPI_COMM_WORLD);
        }
    }
}

void handle_heartbeat() {
    int term;
    MPI_Status status;
    MPI_Recv(&term, 1, MPI_INT, MPI_ANY_SOURCE, TAG_HEARTBEAT, 
             MPI_COMM_WORLD, &status);
    
    lock_guard<mutex> lock(state_mutex);
    
    if (term >= raft_state.current_term) {
        raft_state.current_term = term;
        raft_state.state = FOLLOWER;
        raft_state.leader_id = status.MPI_SOURCE;
        raft_state.last_heartbeat_time = current_time_ms();
        raft_state.voted_for = -1;
    }
    
    int ack = raft_state.current_term;
    MPI_Send(&ack, 1, MPI_INT, status.MPI_SOURCE, TAG_HEARTBEAT_ACK, MPI_COMM_WORLD);
}

void check_election_timeout() {
    lock_guard<mutex> lock(state_mutex);
    
    if (raft_state.state == LEADER) return;
    
    long long elapsed = current_time_ms() - raft_state.last_heartbeat_time;
    
    if (elapsed > raft_state.election_timeout_ms) {
        log_to_file("Election timeout! Starting new election");
        state_mutex.unlock();
        start_election();
        state_mutex.lock();
    }
}

// ========== WORK DEDUPLICATION LOGIC ==========

// CRITICAL: Update max nonce to prevent re-checking
void update_max_nonce_checked() {
    for (const auto& range : global_work_ranges) {
        if (range.completed && range.end_nonce > global_max_nonce_checked) {
            global_max_nonce_checked = range.end_nonce;
        }
    }
    log_to_file("Max nonce checked so far: " + to_string(global_max_nonce_checked));
}

// FIXED: Expand work ONLY from max checked nonce (never re-check)
void expand_work_ranges() {
    // First, update what's been completed
    update_max_nonce_checked();
    
    uint64_t max_end = global_max_nonce_checked;
    
    // If nothing completed yet, find current max from existing ranges
    if (max_end == 0) {
        for (const auto& range : global_work_ranges) {
            if (range.end_nonce > max_end) {
                max_end = range.end_nonce;
            }
        }
    }
    
    log_to_file("Expanding work from nonce " + to_string(max_end) + 
               " (already checked: 0-" + to_string(global_max_nonce_checked) + ")");
    
    // Add new ranges AFTER max_end (never re-check old work)
    int new_ranges = (world_size - 1) * 2;
    for (int i = 0; i < new_ranges; ++i) {
        if (max_end >= max_nonce_limit) break;
        
        WorkRange range;
        range.start_nonce = max_end;
        range.end_nonce = min(max_end + NONCE_RANGE_SIZE, max_nonce_limit);
        range.progress_nonce = max_end;
        range.worker_id = -1;
        range.completed = false;
        range.in_progress = false;
        range.term_assigned = raft_state.current_term;
        
        global_work_ranges.push_back(range);
        max_end += NONCE_RANGE_SIZE;
    }
    
    log_to_file("Expanded work: total nonces now " + to_string(max_end));
}

// FIXED: Distribute work only from unchecked ranges
void leader_distribute_work() {
    if (raft_state.state != LEADER || transactions.empty()) return;
    
    this_thread::sleep_for(chrono::milliseconds(200));
    
    // CRITICAL: Update max checked before distributing
    update_max_nonce_checked();
    
    int tx_index = 0;
    Transaction& tx = transactions[tx_index];
    
    log_to_file("Leader distributing work for " + tx.txid + 
               " (completed: 0-" + to_string(global_max_nonce_checked) + ")");
    
    int assigned = 0;
    for (int worker = 0; worker < world_size; ++worker) {
        if (worker == world_rank) continue;
        
        WorkRange* range = get_next_incomplete_range();
        if (!range) {
            expand_work_ranges();
            range = get_next_incomplete_range();
            
            if (!range) {
                log_to_file("No work available");
                break;
            }
        }
        
        // Double-check: Don't assign already-checked nonces
        if (range->start_nonce < global_max_nonce_checked) {
            log_to_file("WARNING: Skipping range [" + to_string(range->start_nonce) + 
                       ", " + to_string(range->end_nonce) + ") - already checked");
            range->completed = true;
            continue;
        }
        
        range->in_progress = true;
        range->worker_id = worker;
        range->term_assigned = raft_state.current_term;
        
        WorkAssignment work;
        work.start_nonce = range->start_nonce;
        work.end_nonce = range->end_nonce;
        work.tx_index = tx_index;
        work.term = raft_state.current_term;
        work.sequence_num = global_sequence_num++;
        strncpy(work.transaction_data, tx.data.c_str(), 255);
        work.transaction_data[255] = '\0';
        
        MPI_Send(&work, sizeof(WorkAssignment), MPI_BYTE, worker, 
                 TAG_WORK_ASSIGN, MPI_COMM_WORLD);
        
        log_to_file("Assigned NEW range [" + to_string(work.start_nonce) + 
                   ", " + to_string(work.end_nonce) + ") to rank " + to_string(worker));
        assigned++;
    }
    
    save_global_work_state();
}

void leader_monitor_workers() {
    if (raft_state.state != LEADER) return;
    
    int flag;
    MPI_Status status;
    
    MPI_Iprobe(MPI_ANY_SOURCE, TAG_HEARTBEAT_ACK, MPI_COMM_WORLD, &flag, &status);
    while (flag) {
        int ack;
        MPI_Recv(&ack, 1, MPI_INT, MPI_ANY_SOURCE, TAG_HEARTBEAT_ACK, 
                 MPI_COMM_WORLD, &status);
        MPI_Iprobe(MPI_ANY_SOURCE, TAG_HEARTBEAT_ACK, MPI_COMM_WORLD, &flag, &status);
    }
    
    MPI_Iprobe(MPI_ANY_SOURCE, TAG_CHECKPOINT_SYNC, MPI_COMM_WORLD, &flag, &status);
    while (flag) {
        WorkStatusReport report;
        MPI_Recv(&report, sizeof(WorkStatusReport), MPI_BYTE, MPI_ANY_SOURCE, 
                 TAG_CHECKPOINT_SYNC, MPI_COMM_WORLD, &status);
        
        for (auto& range : global_work_ranges) {
            if (range.in_progress && range.worker_id == report.worker_id) {
                range.progress_nonce = report.progress_nonce;
            }
        }
        
        MPI_Iprobe(MPI_ANY_SOURCE, TAG_CHECKPOINT_SYNC, MPI_COMM_WORLD, &flag, &status);
    }
    
    MPI_Iprobe(MPI_ANY_SOURCE, TAG_WORK_RESULT, MPI_COMM_WORLD, &flag, &status);
    while (flag) {
        WorkResult result;
        MPI_Recv(&result, sizeof(WorkResult), MPI_BYTE, MPI_ANY_SOURCE, 
                 TAG_WORK_RESULT, MPI_COMM_WORLD, &status);
        
        if (result.found && result.term == raft_state.current_term) {
            block_found = true;
            winning_nonce = result.nonce;
            winning_hash = string(result.hash_hex);
            
            log_to_file("*** NONCE FOUND by rank " + to_string(status.MPI_SOURCE) + 
                       " *** Nonce: " + to_string(winning_nonce) + ", Hash: " + winning_hash);
            
            ofstream result_file(LOG_DIR + "final_result.txt", ios::out | ios::trunc);
            result_file << "Nonce: " << winning_nonce << "\n";
            result_file << "Hash: " << winning_hash << "\n";
            result_file << "Found by: Rank " << status.MPI_SOURCE << "\n";
            result_file << "Term: " << raft_state.current_term << "\n";
            result_file << "Difficulty: " << difficulty << "\n";
            result_file << "Total hashes: " << total_hashes_computed.load() << "\n";
            result_file.close();
            
            for (int i = 0; i < world_size; ++i) {
                if (i != world_rank) {
                    int term = 1;
                    MPI_Send(&term, 1, MPI_INT, i, TAG_TERMINATE, MPI_COMM_WORLD);
                }
            }
            should_terminate = true;
            return;
        } else {
            // Worker completed range
            for (auto& range : global_work_ranges) {
                if (range.worker_id == result.worker_id && range.in_progress) {
                    range.completed = true;
                    range.in_progress = false;
                    log_to_file("Rank " + to_string(result.worker_id) + " completed range [" +
                               to_string(range.start_nonce) + ", " + to_string(range.end_nonce) + ")");
                    
                    // Update global max checked
                    if (range.end_nonce > global_max_nonce_checked) {
                        global_max_nonce_checked = range.end_nonce;
                    }
                    break;
                }
            }
            
            // Assign next NEW work
            WorkRange* next_range = get_next_incomplete_range();
            if (!next_range) {
                expand_work_ranges();
                next_range = get_next_incomplete_range();
            }
            
            if (next_range && !transactions.empty()) {
                // Skip if already checked
                if (next_range->start_nonce < global_max_nonce_checked) {
                    next_range->completed = true;
                    continue;
                }
                
                next_range->in_progress = true;
                next_range->worker_id = result.worker_id;
                next_range->term_assigned = raft_state.current_term;
                
                WorkAssignment work;
                work.start_nonce = next_range->start_nonce;
                work.end_nonce = next_range->end_nonce;
                work.tx_index = 0;
                work.term = raft_state.current_term;
                work.sequence_num = global_sequence_num++;
                strncpy(work.transaction_data, transactions[0].data.c_str(), 255);
                work.transaction_data[255] = '\0';
                
                MPI_Send(&work, sizeof(WorkAssignment), MPI_BYTE, result.worker_id, 
                         TAG_WORK_ASSIGN, MPI_COMM_WORLD);
                
                log_to_file("Assigned NEW range [" + to_string(work.start_nonce) + 
                           ", " + to_string(work.end_nonce) + ") to rank " + to_string(result.worker_id));
            }
        }
        
        save_global_work_state();
        MPI_Iprobe(MPI_ANY_SOURCE, TAG_WORK_RESULT, MPI_COMM_WORLD, &flag, &status);
    }
}

// ========== WORKER MINING ==========

void worker_mine(const WorkAssignment& work) {
    string tx_data(work.transaction_data);
    
    my_work_status.worker_id = world_rank;
    my_work_status.current_start = work.start_nonce;
    my_work_status.current_end = work.end_nonce;
    my_work_status.progress_nonce = work.start_nonce;
    my_work_status.is_mining = true;
    my_work_status.term = work.term;
    
    log_to_file("=== STARTING MINING ===");
    log_to_file("Range: [" + to_string(work.start_nonce) + ", " + to_string(work.end_nonce) + ")");
    log_to_file("Transaction: " + tx_data);
    log_to_file("Difficulty: " + to_string(difficulty) + " leading zeros required");
    
    auto mining_start = steady_clock::now();
    auto last_heartbeat = steady_clock::now();
    auto last_checkpoint = steady_clock::now();
    auto last_log = steady_clock::now();
    
    uint64_t hashes_this_range = 0;
    uint64_t total_checks = work.end_nonce - work.start_nonce;
    
    // ACTUAL MINING LOOP
    for (uint64_t nonce = work.start_nonce; nonce < work.end_nonce; ++nonce) {
        if (should_terminate || block_found) {
            my_work_status.is_mining = false;
            log_to_file("Mining interrupted");
            return;
        }
        
        my_work_status.progress_nonce = nonce;
        
        // Compute SHA-256 hash
        string hash = sha256_hash(tx_data, nonce);
        hashes_this_range++;
        total_hashes_computed++;
        
        // Check if valid
        bool is_valid = valid_hash(hash, difficulty);
        
        // Debug: Log samples
        if (hashes_this_range <= 5 || hashes_this_range % 50000 == 0) {
            log_to_file("Nonce " + to_string(nonce) + ": " + hash + 
                       (is_valid ? " ✓ VALID!" : "  ✗ invalid"));
        }
        
        if (is_valid) {
            auto mining_duration = chrono::duration_cast<chrono::milliseconds>(
                steady_clock::now() - mining_start).count();
            
            WorkResult result;
            result.nonce = nonce;
            result.found = true;
            result.term = work.term;
            result.worker_id = world_rank;
            strncpy(result.hash_hex, hash.c_str(), 64);
            result.hash_hex[64] = '\0';
            
            log_to_file("*** SOLUTION FOUND! ***");
            log_to_file("Nonce: " + to_string(nonce));
            log_to_file("Hash: " + hash);
            log_to_file("Hashes computed: " + to_string(hashes_this_range));
            log_to_file("Time: " + to_string(mining_duration) + " ms");
            
            if (raft_state.leader_id != -1) {
                MPI_Send(&result, sizeof(WorkResult), MPI_BYTE, raft_state.leader_id, 
                         TAG_WORK_RESULT, MPI_COMM_WORLD);
            }
            
            my_work_status.is_mining = false;
            return;
        }
        
        // Periodic tasks
        if (nonce % MESSAGE_CHECK_FREQUENCY == 0 && nonce > work.start_nonce) {
            auto now = steady_clock::now();
            
            if (chrono::duration_cast<chrono::milliseconds>(
                now - last_heartbeat).count() > HEARTBEAT_INTERVAL_MS) {
                
                if (raft_state.leader_id != -1) {
                    int hb = world_rank;
                    MPI_Send(&hb, 1, MPI_INT, raft_state.leader_id, TAG_HEARTBEAT, MPI_COMM_WORLD);
                }
                last_heartbeat = now;
            }
            
            if (chrono::duration_cast<chrono::milliseconds>(
                now - last_checkpoint).count() > CHECKPOINT_INTERVAL_MS) {
                
                if (raft_state.leader_id != -1) {
                    MPI_Send(&my_work_status, sizeof(WorkStatusReport), MPI_BYTE, 
                             raft_state.leader_id, TAG_CHECKPOINT_SYNC, MPI_COMM_WORLD);
                }
                last_checkpoint = now;
            }
            
            if (chrono::duration_cast<chrono::seconds>(
                now - last_log).count() > 10) {
                
                uint64_t done = nonce - work.start_nonce;
                double pct = (100.0 * done) / total_checks;
                auto elapsed_ms = chrono::duration_cast<chrono::milliseconds>(
                    now - mining_start).count();
                uint64_t hash_rate = (hashes_this_range * 1000) / (elapsed_ms + 1);
                
                log_to_file("Progress: " + to_string((int)pct) + "% | " +
                           to_string(hashes_this_range) + " hashes | " +
                           to_string(hash_rate) + " H/s");
                last_log = now;
            }
        }
    }
    
    // Completed range
    auto mining_duration = chrono::duration_cast<chrono::milliseconds>(
        steady_clock::now() - mining_start).count();
    
    log_to_file("=== RANGE COMPLETED ===");
    log_to_file("Total hashes: " + to_string(hashes_this_range));
    log_to_file("Time: " + to_string(mining_duration) + " ms");
    log_to_file("No valid hash found");
    
    WorkResult result;
    result.found = false;
    result.term = work.term;
    result.worker_id = world_rank;
    
    if (raft_state.leader_id != -1) {
        MPI_Send(&result, sizeof(WorkResult), MPI_BYTE, raft_state.leader_id, 
                 TAG_WORK_RESULT, MPI_COMM_WORLD);
    }
    
    my_work_status.is_mining = false;
}

// ========== MESSAGE HANDLING ==========

void handle_messages() {
    int flag;
    MPI_Status status;
    
    MPI_Iprobe(MPI_ANY_SOURCE, TAG_REQUEST_VOTE, MPI_COMM_WORLD, &flag, &status);
    if (flag) handle_vote_request();
    
    MPI_Iprobe(MPI_ANY_SOURCE, TAG_VOTE_REPLY, MPI_COMM_WORLD, &flag, &status);
    if (flag) handle_vote_reply();
    
    MPI_Iprobe(MPI_ANY_SOURCE, TAG_HEARTBEAT, MPI_COMM_WORLD, &flag, &status);
    if (flag) handle_heartbeat();
    
    MPI_Iprobe(MPI_ANY_SOURCE, TAG_WORK_STATUS_REQ, MPI_COMM_WORLD, &flag, &status);
    if (flag) {
        int req;
        MPI_Recv(&req, 1, MPI_INT, MPI_ANY_SOURCE, TAG_WORK_STATUS_REQ, MPI_COMM_WORLD, &status);
        MPI_Send(&my_work_status, sizeof(WorkStatusReport), MPI_BYTE, status.MPI_SOURCE, 
                 TAG_WORK_STATUS_REPLY, MPI_COMM_WORLD);
    }
    
    MPI_Iprobe(MPI_ANY_SOURCE, TAG_WORK_ASSIGN, MPI_COMM_WORLD, &flag, &status);
    if (flag) {
        WorkAssignment work;
        MPI_Recv(&work, sizeof(WorkAssignment), MPI_BYTE, MPI_ANY_SOURCE, 
                 TAG_WORK_ASSIGN, MPI_COMM_WORLD, &status);
        worker_mine(work);
    }
    
    MPI_Iprobe(MPI_ANY_SOURCE, TAG_TERMINATE, MPI_COMM_WORLD, &flag, &status);
    if (flag) {
        int term;
        MPI_Recv(&term, 1, MPI_INT, MPI_ANY_SOURCE, TAG_TERMINATE, MPI_COMM_WORLD, &status);
        should_terminate = true;
        log_to_file("Terminating");
    }
}

// ========== MAIN LOOP ==========

void run_node() {
    raft_state.election_timeout_ms = get_random_election_timeout(world_rank);
    raft_state.last_heartbeat_time = current_time_ms();
    
    transactions = read_transactions("transactions.txt");
    if (world_rank == 0) {
        log_to_file("Loaded " + to_string(transactions.size()) + " transactions");
    }
    
    if (transactions.empty()) {
        log_to_file("ERROR: No transactions loaded!");
        should_terminate = true;
        return;
    }
    
    my_work_status.worker_id = world_rank;
    my_work_status.is_mining = false;
    
    auto last_heartbeat_check = steady_clock::now();
    bool work_distributed = false;
    
    while (!should_terminate && !block_found) {
        handle_messages();
        
        auto now = steady_clock::now();
        
        if (raft_state.state == LEADER) {
            if (chrono::duration_cast<chrono::milliseconds>(
                now - last_heartbeat_check).count() > HEARTBEAT_INTERVAL_MS) {
                
                send_heartbeats();
                leader_monitor_workers();
                last_heartbeat_check = now;
            }
            
            if (!work_distributed && !transactions.empty()) {
                leader_distribute_work();
                work_distributed = true;
            }
        } else {
            check_election_timeout();
            work_distributed = false;
        }
        
        this_thread::sleep_for(chrono::milliseconds(10));
    }
    
    log_to_file("Node shutting down | Total hashes: " + to_string(total_hashes_computed.load()));
}

int main(int argc, char** argv) {
    MPI_Init(&argc, &argv);
    MPI_Comm_rank(MPI_COMM_WORLD, &world_rank);
    MPI_Comm_size(MPI_COMM_WORLD, &world_size);
    
    if (argc >= 2) {
        difficulty = atoi(argv[1]);
    }
    
    // Clean old files
    if (world_rank == 0) {
        system("rm -f logs/*.log checkpoints/*.chk checkpoints/*.state 2>/dev/null");
        cout << "=== Bitcoin Mining Started ===" << endl;
        cout << "Processes: " << world_size << endl;
        cout << "Difficulty: " << difficulty << " leading zeros" << endl;
        cout << "Mining until solution found (no limit)" << endl;
        cout << "Logs: logs/rank_*.log" << endl;
        cout << "==============================" << endl;
    }
    
    MPI_Barrier(MPI_COMM_WORLD);
    
    log_to_file("Starting mining (difficulty=" + to_string(difficulty) + ")");
    
    run_node();
    
    if (log_file.is_open()) log_file.close();
    
    MPI_Barrier(MPI_COMM_WORLD);
    
    // Check results
    if (world_rank == 0) {
        this_thread::sleep_for(chrono::milliseconds(100));
        
        ifstream result_file(LOG_DIR + "final_result.txt");
        if (result_file.is_open()) {
            string nonce_line, hash_line, found_line, term_line, diff_line;
            getline(result_file, nonce_line);
            getline(result_file, hash_line);
            getline(result_file, found_line);
            getline(result_file, term_line);
            getline(result_file, diff_line);
            result_file.close();
            
            if (nonce_line.find("Nonce:") != string::npos) {
                cout << "\n========================================" << endl;
                cout << "*** SUCCESS! Block Mined! ***" << endl;
                cout << "========================================" << endl;
                cout << nonce_line << endl;
                cout << hash_line << endl;
                cout << found_line << endl;
                cout << term_line << endl;
                cout << diff_line << endl;
                cout << "========================================" << endl;
            }
        }
        cout << "\nSee logs/ for detailed logs" << endl;
    }
    
    MPI_Finalize();
    return 0;
}
