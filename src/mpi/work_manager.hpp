#ifndef WORK_MANAGER_HPP
#define WORK_MANAGER_HPP

#include "raft_state.hpp"
#include "file_io.hpp"
#include <vector>

using namespace std;

// forward declarations
extern vector<WorkRange> global_work_ranges;
extern vector<Transaction> transactions;
extern RaftState raft_state;
extern int world_size;

inline void initialize_work_ranges() {
    if (!transactions.empty()) {
        uint64_t total_nonces = NONCE_RANGE_SIZE * (world_size - 1) * 3;
        uint64_t range_size = NONCE_RANGE_SIZE;
        
        for (uint64_t start = 0; start < total_nonces; start += range_size) {
            WorkRange range;
            range.start_nonce = start;
            range.end_nonce = start + range_size;
            range.progress_nonce = start;
            range.worker_id = -1;
            range.completed = false;
            range.in_progress = false;
            range.term_assigned = raft_state.current_term;
            
            global_work_ranges.push_back(range);
        }
        
        log_to_file("Initialized " + to_string(global_work_ranges.size()) + " work ranges");
    }
}

inline WorkRange* get_next_incomplete_range() {
    for (auto& range : global_work_ranges) {
        if (!range.completed && !range.in_progress) {
            return &range;
        }
    }
    
    for (auto& range : global_work_ranges) {
        if (range.in_progress && !range.completed) {
            if (range.progress_nonce > range.start_nonce) {
                range.completed = true;
                
                WorkRange new_range;
                new_range.start_nonce = range.progress_nonce;
                new_range.end_nonce = range.end_nonce;
                new_range.progress_nonce = range.progress_nonce;
                new_range.worker_id = -1;
                new_range.completed = false;
                new_range.in_progress = false;
                new_range.term_assigned = raft_state.current_term;
                
                global_work_ranges.push_back(new_range);
                log_to_file("Created recovery range [" + to_string(new_range.start_nonce) + 
                           ", " + to_string(new_range.end_nonce) + ") from abandoned work");
                
                return &global_work_ranges.back();
            }
        }
    }
    
    return nullptr;
}

#endif
