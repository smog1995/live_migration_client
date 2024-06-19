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
		row_lock_map_latch_ = false;
	}
	unordered_map<uint64_t, std::shared_ptr<LockRequestQueue>> row_lock_map_;
	bool row_lock_map_latch_;
	RC LockRow(txnid_t xid, lock_t lock_type, uint64_t row_key);
	RC UnlockRow(txnid_t xid, lock_t lock_type, uint64_t row_key);
	//  S，X
	bool compatable_lock_[2][2] = {
		{true, false},
		{false, false}
	};
	bool Compatibale(lock_t type_a, lock_t type_b) {
		return compatable_lock_[type_a][type_b];
	}
};


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
private:
	pthread_mutex_t ts_mutex;
	uint64_t 		timestamp;
	pthread_mutex_t mutexes[BUCKET_CNT];
	uint64_t 		hash(row_t * row);
	ts_t * volatile all_ts;
	TxnManager ** 		_all_txns;
	ts_t			last_min_ts_time;
	ts_t			min_ts;
	vector<int> local_partitions;
	LockManager lock_manager;
};

#endif


