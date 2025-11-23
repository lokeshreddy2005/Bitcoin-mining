#ifndef RAFT_STATE_HPP
#define RAFT_STATE_HPP

#include <string>
#include <vector>
#include <cstdint>

using namespace std;

#define TAG_REQUEST_VOTE 1
#define TAG_VOTE_REPLY 2
#define TAG_HEARTBEAT 3
#define TAG_HEARTBEAT_ACK 4
#define TAG_WORK_ASSIGN 7
#define TAG_WORK_RESULT 8
#define TAG_CHECKPOINT 9
#define TAG_CHECKPOINT_ACK 10
#define TAG_TERMINATE 13
#define TAG_WORK_STATUS_REQ 14
#define TAG_WORK_STATUS_REPLY 15
#define TAG_CHECKPOINT_SYNC 16

//enums

enum NodeState { FOLLOWER, CANDIDATE, LEADER };


struct RaftState {
    NodeState state = FOLLOWER;
    int current_term = 0;
    int voted_for = -1;
    int leader_id = -1;
    int votes_received = 0;
    long long last_heartbeat_time = 0;
    int election_timeout_ms = 0;
};

struct Transaction {
    string txid;
    string data;
    string prev_hash;
};

struct WorkRange {
    uint64_t start_nonce;
    uint64_t end_nonce;
    uint64_t progress_nonce;
    int worker_id;
    bool completed;
    bool in_progress;
    int term_assigned;
    
    WorkRange() : start_nonce(0), end_nonce(0), progress_nonce(0), 
                  worker_id(-1), completed(false), in_progress(false), 
                  term_assigned(0) {}
};

struct WorkAssignment {
    uint64_t start_nonce;
    uint64_t end_nonce;
    int tx_index;
    char transaction_data[256];
    int term;
    int sequence_num;
    
    WorkAssignment() : start_nonce(0), end_nonce(0), tx_index(0), 
                       term(0), sequence_num(0) {
        transaction_data[0] = '\0';
    }
};

struct WorkResult {
    uint64_t nonce;
    bool found;
    char hash_hex[65];
    int term;
    int worker_id;
    
    WorkResult() : nonce(0), found(false), term(0), worker_id(-1) {
        hash_hex[0] = '\0';
    }
};

struct WorkStatusReport {
    int worker_id;
    uint64_t current_start;
    uint64_t current_end;
    uint64_t progress_nonce;
    bool is_mining;
    int term;
    
    WorkStatusReport() : worker_id(-1), current_start(0), current_end(0),
                         progress_nonce(0), is_mining(false), term(0) {}
};

struct VoteRequest {
    int term;
    int candidate_id;
    
    VoteRequest() : term(0), candidate_id(-1) {}
};

struct VoteReply {
    int term;
    bool vote_granted;
    
    VoteReply() : term(0), vote_granted(false) {}
};

#endif // RAFT_STATE_HPP
