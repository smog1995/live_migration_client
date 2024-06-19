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

#include "manager.h"
#include "row.h"
#include "txn.h"
#include "pthread.h"
//#include <jemallloc.h>

void Manager::init() {
	timestamp = 1;
	last_min_ts_time = 0;
	min_ts = 0; 
	all_ts = (ts_t *) malloc(sizeof(ts_t) * (g_thread_cnt * g_node_cnt));
	_all_txns = new TxnManager * [g_thread_cnt + g_rem_thread_cnt];
	for (UInt32 i = 0; i < g_thread_cnt + g_rem_thread_cnt; i++) {
		//all_ts[i] = 0;
		//all_ts[i] = UINT64_MAX;
		_all_txns[i] = NULL;
	}
	for (UInt32 i = 0; i < BUCKET_CNT; i++)
		pthread_mutex_init( &mutexes[i], NULL );
  for (UInt32 i = 0; i < g_thread_cnt * g_node_cnt; ++i)
      all_ts[i] = 0;
	//  初始化本地分区id，在迁移后需要更新
	for (int i = 0; i < PART_CNT; ++i) {
		if (GET_NODE_ID(i) == g_node_id) {
			local_partitions.push_back(i);
		}
	}
}

uint64_t 
Manager::get_ts(uint64_t thread_id) {
	if (g_ts_batch_alloc)
		assert(g_ts_alloc == TS_CAS);
	uint64_t time;
	uint64_t starttime = get_sys_clock();
	switch(g_ts_alloc) {
	case TS_MUTEX :
		pthread_mutex_lock( &ts_mutex );
		time = ++timestamp;
		pthread_mutex_unlock( &ts_mutex );
		break;
	case TS_CAS :
		if (g_ts_batch_alloc)
			time = ATOM_FETCH_ADD(timestamp, g_ts_batch_num);
		else 
			time = ATOM_FETCH_ADD(timestamp, 1);
		break;
	case TS_HW :
		assert(false);
		break;
	case TS_CLOCK :
		time = get_wall_clock() * (g_node_cnt + g_thread_cnt) + (g_node_id * g_thread_cnt + thread_id);
		break;
	default :
		assert(false);
	}
	INC_STATS(thread_id, ts_alloc_time, get_sys_clock() - starttime);
	return time;
}

ts_t Manager::get_min_ts(uint64_t tid) {
	uint64_t now = get_sys_clock();
	if (now - last_min_ts_time > MIN_TS_INTVL) { 
		last_min_ts_time = now;
    uint64_t min = txn_table.get_min_ts(tid);
    if(min > min_ts)
		  min_ts = min;
	} 
	return min_ts;
}

void Manager::set_txn_man(TxnManager * txn) {
	int thd_id = txn->get_thd_id();
	_all_txns[thd_id] = txn;
}


uint64_t Manager::hash(row_t * row) {
	uint64_t addr = (uint64_t)row / MEM_ALLIGN;
    return (addr * 1103515247 + 12345) % BUCKET_CNT;
}
 
void Manager::lock_row(row_t * row) {
	int bid = hash(row);
  uint64_t mtx_time_start = get_sys_clock();
	pthread_mutex_lock( &mutexes[bid] );	
  INC_STATS(0,mtx[2],get_sys_clock() - mtx_time_start);
}

void Manager::release_row(row_t * row) {
	int bid = hash(row);
	pthread_mutex_unlock( &mutexes[bid] );
}


RC LockManager::LockRow(txnid_t txn_id, lock_t lock_type, uint64_t row_key) {
	RC rc = RCOK;
	while (!ATOM_CAS(row_lock_map_latch_, false, true)) {}
	//  对map临界区访问对应的行锁
	if (row_lock_map_.find(row_key) == row_lock_map_.end()) {
		row_lock_map_.insert(std::make_pair(row_key,std::make_shared<LockRequestQueue>()));
	}
	auto lock_request_queue = row_lock_map_[row_key];
	ATOM_CAS(row_lock_map_latch_, true, false);
	// 再访问行锁等待队列
	while (!ATOM_CAS(lock_request_queue->latch_, false, true)) {}
	
	for (auto ele = lock_request_queue->request_queue_.begin(); ele != lock_request_queue->request_queue_.end(); ele++) {
		// 1.当前面已经有请求时，如果不是当前事务发出的锁，而且不兼容，我们把锁请求加入等待队列后，阻塞当前事务
		if ((*ele)->txn_id_ != txn_id && !Compatibale((*ele)->lock_type_, lock_type)) {
			rc = WAIT;
			break;
		// 2.第一个请求如果不是当前事务发出的请求，而且兼容，那么我们要继续判断，
		//   一旦前面出现一个请求不是兼容的，根据first come first service我们必须等待(也就是会进入分支1处理)
		} else if ((*ele)->txn_id_ != txn_id && Compatibale((*ele)->lock_type_, lock_type)) {
			continue;
		//  3.如果前面的请求是当前事务发出的
		} else if ((*ele)->txn_id_ == txn_id) {
			// 当一开始事务上的是写锁，那么肯定不需要继续加锁；或者加了同样的读锁，也不需要再加
			if ((*ele)->lock_type_ == lock_type || (*ele)->lock_type_ == LOCK_EX) {
				ATOM_CAS(lock_request_queue->latch_, true, false);
				return rc;
			//  只有读锁升级为写锁需要加请求，同时要判断是否只有一个事务，如果有多个事务共享读，那么先放写锁请求到等待队列
			} else {
				if (lock_type == LOCK_EX && lock_request_queue->granted_count_ == 1) {
					(*ele)->lock_type_ = LOCK_EX; // 锁升级
					ATOM_CAS(lock_request_queue->latch_, true, false);
					return rc;
				} else {//  如果有多个请求被授予，那么同样事务会被阻塞，等待被唤醒
					rc = WAIT;
					break;
				}
			}
		}
	}
	LockRequest* lock_request = new LockRequest(txn_id, lock_type);
	
	if (rc == RCOK) {  //  
		lock_request->grant_ = true;
		lock_request->grant_++;
	}
	lock_request_queue->request_queue_.push_back(std::unique_ptr<LockRequest>(lock_request));
	ATOM_CAS(row_lock_map_latch_, false, true);
	return rc;
}

//  首先，明确等待队列中如果授予锁的请求都结束后，如果队列还有请求处于等待，那么刚才释放的最后一个锁若为写锁，那后面为读锁，我们需要唤醒所有后面的事务；
//  若刚才释放掉的最后一个锁为读锁，那么后面必然为写锁，我们只需要唤醒后面的一个事务(因为如果是读锁请求则肯定第一次上锁就成功了，不会被阻塞)
//  同时，一个事务对一个行只可能有一个锁
RC LockManager::UnlockRow(txnid_t txn_id, lock_t lock_type, uint64_t row_key) {
	RC rc = RCOK;
	while (!ATOM_CAS(row_lock_map_latch_, false, true)) {}
	//  对map临界区访问对应的行锁
	if (row_lock_map_.find(row_key) == row_lock_map_.end()) {
		row_lock_map_.insert(std::make_pair(row_key,std::make_shared<LockRequestQueue>()));
	}
	auto lock_request_queue = row_lock_map_[row_key];
	ATOM_CAS(row_lock_map_latch_, true, false);
	// 再访问行锁等待队列
	lock_t txn_lock_type;
	while (!ATOM_CAS(lock_request_queue->latch_, false, true)) {}
	for (auto ele = lock_request_queue->request_queue_.begin(); ele != lock_request_queue->request_queue_.end(); ele++) {
		if ((*ele)->txn_id_ == txn_id) {
			lock_request_queue->granted_count_--;
			txn_lock_type = (*ele)->lock_type_;
			lock_request_queue->request_queue_.erase((ele));
			break;
		}
	}
	if (lock_request_queue->granted_count_ == 0) {
		if (lock_type == LOCK_EX) {
			// 唤醒等待队列的所有事务
			// notify_all_txn();
		} else {
			// 只需唤醒后面的第一个事务（肯定为写锁，否则会被阻塞）
			// notify_one_txn();
		}
	}
	return RCOK;

	
}
