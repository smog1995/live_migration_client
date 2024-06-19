/*
   Copyright 2016 Massachusetts Institute of Technology

   Licensed under the Apache License, Version 2.0 (the "License");
   you may not use this file except in compliance with the License.
   You may obtain a copy of the License at

       http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.
*/

#ifndef _MANAGER_H_
#define _MANAGER_H_

#include "helper.h"
#include "global.h"
#include <mutex>
#include <unordered_map>
class row_t;
class TxnManager;


class LockManager {
public: 
	class LockRequest {
		public:
			LockRequest(txnid_t txn_id, lock_t lock_type) 
				:txn_id_(txn_id), lock_type_(lock_type) {grant_ = false;}
		
			bool grant_;
			txnid_t txn_id_;
			lock_t lock_type_;
	};
	class LockRequestQueue {
		public:
		// 锁请求队列
			LockRequestQueue():latch_(false), granted_count_(0) {}
			list<unique_ptr<LockRequest>> request_queue_;
			bool latch_;
			uint32_t granted_count_;
	};
	LockManager() {

		// 初始化table_name
		// row_lock_map_latch_ = false;
		vector<string> table_list;
		string schema_file = "benchmarks/TPCC_full_schema.txt";
		ifstream fin(schema_file);
		string line;
			if (!fin.is_open()) {
			cerr << "Error: Could not open file " << schema_file << endl;
			cerr << "Reason: " << strerror(errno) << endl;
		}
		while (getline(fin, line)) {
			// cout << line << " line";
			if (line.compare(0, 6, "TABLE=") == 0) {
				table_list.emplace_back(string(&line[6]));
			}
		}
		
		for (string &table_name: table_list) {
			row_lock_map_latch_.insert({table_name,false});
		}
		
	}
	unordered_map<string, unordered_map<uint64_t, std::shared_ptr<LockRequestQueue>>> row_lock_map_;
	unordered_map<string, bool> row_lock_map_latch_;
	RC lockRow(TxnManager* txn_man, lock_t lock_type, uint64_t row_key, string table_name);
	RC unlockRow(TxnManager* txn_man, uint64_t row_key, string table_name);
	//  S，X
	bool compatable_lock_[2][2] = {
		{false, false},
		{false, true}
	};
	// std::unordered_map<txnid_t, std::vector<txnid_t>> wait_for_;
	// bool wait_for_latch_;
	bool Compatibale(lock_t type_a, lock_t type_b) {
		return compatable_lock_[type_a][type_b];
	}
	void deathLockDetection(uint64_t thd_id);
	txnid_t cycleDetection(std::unordered_map<txnid_t, std::vector<txnid_t>>& waits_for);
	void depthFirstSearch(txnid_t vertex, bool& has_cycle, txnid_t& youngest_txn, unordered_map<txnid_t, bool>& onpath,
				 unordered_map<txnid_t, bool>& visited, unordered_map<txnid_t, vector<txnid_t>>& waits_for);
	void lockRequestDump(uint64_t rowkey, string table_name);
	
	
};

struct TxnEntry {
	txnid_t txn_id_;
	std::chrono::system_clock::time_point start_block_time_; //  每次被阻塞，都要重新设置
	bool remote_txn_ = false;
	std::chrono::duration<double> blocked_time_;
	bool blocked = true; // 起初为true
	TxnEntry(txnid_t txn_id, bool remote_txn,std::chrono::system_clock::time_point start_block_time):txn_id_(txn_id),remote_txn_(remote_txn),start_block_time_(start_block_time){}
};
// class Statistics {

// };
class Manager {
public:
	void 			init();
	// returns the next timestamp.
	ts_t			get_ts(uint64_t thread_id);

	// For MVCC. To calculate the min active ts in the system
	ts_t 			get_min_ts(uint64_t tid = 0);

	// HACK! the following mutexes are used to model a centralized
	// lock/timestamp manager. 
 	void 			lock_row(row_t * row);
	void 			release_row(row_t * row);
	
	TxnManager * 		get_txn_man(int thd_id) { return _all_txns[thd_id]; };
	void 			set_txn_man(TxnManager * txn);
	void 		addBlockedTxn(txnid_t txn_id, bool remote_txn,std::chrono::system_clock::time_point start_block_time);
	void 		calculateBlockTime(uint64_t thd_id);
	void 		removeTxn(txnid_t txn_id);
	void 		setUnblockedTxn(txnid_t txn_id);
	void		abortOvertimeTxn(uint64_t thd_id, vector<txnid_t> &overtime_txns);
	LockManager lock_manager;
private:
	pthread_mutex_t ts_mutex;
	uint64_t 		timestamp;
	pthread_mutex_t mutexes[BUCKET_CNT];
	unordered_map<txnid_t, unique_ptr<TxnEntry>> blocked_txns;
	bool blocked_txns_map_latch;
	uint64_t 		hash(row_t * row);
	ts_t * volatile all_ts;
	TxnManager ** 		_all_txns;
	ts_t			last_min_ts_time;
	ts_t			min_ts;
	std::chrono::duration<double>	limit_block_overtime; // <double, std::seconds>应该默认是秒
	vector<int> local_partitions;
	
};

#endif


