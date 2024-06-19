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
#include "message.h"
#include "msg_queue.h"
//#include <jemallloc.h>
// #include <chrono>
class  MessageQueue;

void Manager::init() {
	timestamp = 1;
	limit_block_overtime = std::chrono::seconds(1);
	cout << "duration为一秒" << limit_block_overtime.count() << endl;
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


RC LockManager::lockRow(TxnManager *txn_man, lock_t lock_type, uint64_t row_key, string table_name) {
	RC rc = RCOK;
	txnid_t txn_id = txn_man->get_txn_id();
	while (!ATOM_CAS(row_lock_map_latch_[table_name], false, true)) {}
	//  对map临界区访问对应的行锁
	if (row_lock_map_[table_name].find(row_key) == row_lock_map_[table_name].end()) {
		row_lock_map_[table_name].insert(std::make_pair(row_key,std::make_shared<LockRequestQueue>()));
	}
	auto lock_request_queue = row_lock_map_[table_name].at(row_key); //使用[row_key]出现访问到空指针，神奇的bug 
	ATOM_CAS(row_lock_map_latch_[table_name], true, false);
	// 再访问行锁等待队列
	assert(lock_request_queue != NULL);
	while (!ATOM_CAS(lock_request_queue->latch_, false, true)) {}
	
	for (auto ele = lock_request_queue->request_queue_.begin(); ele != lock_request_queue->request_queue_.end(); ele++) {
		// 1.当前面已经有请求时，如果不是当前事务发出的锁，而且不兼容，我们把锁请求加入等待队列后，阻塞当前事务
		if ((*ele)->txn_id_ != txn_id && !Compatibale((*ele)->lock_type_, lock_type)) {
			// cout << "锁不兼容,";
			// printf( "事务%ld对table%s的row%ld加锁不兼容或者冲突\n",txn_man->get_txn_id(),table_name.c_str(),row_key);
			rc = WAIT;
			break;
		// 2.第一个请求如果不是当前事务发出的请求，而且兼容，那么我们要继续判断，
		//   一旦前面出现一个请求不是兼容的，根据first come first service我们必须等待(也就是会进入分支1处理)
		} else if ((*ele)->txn_id_ != txn_id && Compatibale((*ele)->lock_type_, lock_type)) {
			continue;
		//  3.如果前面的请求是当前事务发出的
		} else if ((*ele)->txn_id_ == txn_id) {
			// 一种可能是之前事务阻塞，重新运行时重复加锁，此时我们的锁请求已经在队列中，也就是当前请求，我们需要将grant赋值为true；另一种就是事务对同一行做了多次访问
			if ((*ele)->grant_ == false) {
				(*ele)->grant_ = true;
				cout << "唤醒后事务" << txn_man->get_txn_id() << "对row" << row_key << " 加锁" << endl;
				lock_request_queue->granted_count_++;
				ATOM_CAS(lock_request_queue->latch_, true, false);
				return rc;
						// 当一开始事务上的是写锁，那么肯定不需要继续加锁；或者加了同样的读锁，也不需要再加
			} else if ((*ele)->lock_type_ == lock_type || (*ele)->lock_type_ == LOCK_EX) {
				cout << "已经加过锁,无需再加,之前的锁为" <<(*ele)->lock_type_  << ",现在的锁为: " << lock_type << endl;
				ATOM_CAS(lock_request_queue->latch_, true, false);
				return rc;
			//  只有读锁升级为写锁需要加请求，同时要判断是否只有一个事务，如果有多个事务共享读，那么先放写锁请求到等待队列
			} else {
				if (lock_type == LOCK_EX && lock_request_queue->granted_count_ == 1) {
					(*ele)->lock_type_ = LOCK_EX; // 锁升级
					// cout << "目前暂不设置该锁，如果有锁升级说明出错" << endl;
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
		// printf("事务%ld对表%s的row%ld加锁\n",txn_man->get_txn_id(), table_name.c_str(), row_key);
		lock_request_queue->granted_count_++;
	} else {
		// printf("事务%ld对表%s的row%ld加锁但失败,需要等待锁\n",txn_man->get_txn_id(), table_name.c_str(), row_key);
	}
	lock_request_queue->request_queue_.push_back(std::unique_ptr<LockRequest>(lock_request));
	ATOM_CAS(lock_request_queue->latch_, true, false);
	
	return rc;
}

//  首先，明确等待队列中如果授予锁的请求都结束后，如果队列还有请求处于等待，那么刚才释放的最后一个锁若为写锁，那后面为读锁，我们需要唤醒所有后面的事务；
//  若刚才释放掉的最后一个锁为读锁，那么后面必然为写锁，我们只需要唤醒后面的一个事务(因为如果是读锁请求则肯定第一次上锁就成功了，不会被阻塞)
//  同时，一个事务对一个行只可能有一个锁
//  非严格两阶段锁（事务未结束即可释放锁）
RC LockManager::unlockRow(TxnManager* txn_man, uint64_t row_key, string table_name) {
	// printf("事务%ld尝试对table(%s)的row%ld解锁\n",txn_man->get_txn_id(),table_name.c_str(), row_key);
	RC rc = RCOK;
	txnid_t txn_id = txn_man->get_txn_id();
	while (!ATOM_CAS(row_lock_map_latch_[table_name], false, true)) {}
	//  对map临界区访问对应的行锁
	auto lock_request_queue = row_lock_map_[table_name].at(row_key);
	assert(lock_request_queue != NULL);
	ATOM_CAS(row_lock_map_latch_[table_name], true, false);
	// 再访问行锁等待队列
	lock_t txn_lock_type;
	bool first_unlock = false;
	while (!ATOM_CAS(lock_request_queue->latch_, false, true)) {}
	for (auto ele = lock_request_queue->request_queue_.begin(); ele != lock_request_queue->request_queue_.end(); ele++) {
		if ((*ele)->txn_id_ == txn_id) {
			if ((*ele)->grant_ == true) {
				lock_request_queue->granted_count_--;
				// printf("事务%ld对table%s的row%ld解锁成功,此时请求队列剩余锁授予数量:%d\n",txn_man->get_txn_id(),table_name.c_str(),row_key, lock_request_queue->granted_count_);
				txn_lock_type = (*ele)->lock_type_;
				lock_request_queue->request_queue_.erase(ele);
				
				first_unlock = true;
			} else {// 阻塞事务的终止
				// printf("阻塞事务%ld的未授权锁请求移除row%ld\n",txn_man->get_txn_id(),row_key);
				lock_request_queue->request_queue_.erase(ele);
			}
			break;
		}
	}

	// 唤醒操作,同个事务可能有多次解锁（因为重复加锁），只有第一次解锁才可以唤醒操作
	if (lock_request_queue->granted_count_ == 0 && !lock_request_queue->request_queue_.empty() && first_unlock) {
		if (txn_lock_type == LOCK_EX) { //  后面的请求既可能是读锁，也可能是写锁（独占锁）
			// 唤醒等待队列的所有事务
			// notifyAllTxn();
			bool is_first_request = true;
			for (auto &ele : lock_request_queue->request_queue_) {
				if (ele->lock_type_ == LOCK_SH) { //  后面跟着的是读锁，那么就重启这些读锁事务
					// printf("唤醒读事务%ld ",ele->txn_id_);
					is_first_request = false;
					txn_table.restart_txn(txn_man->get_thd_id(), ele->txn_id_, 0);
					// 或者遇到的第一个锁仍是写锁，那么只授予一个写锁然后break
				} else if (ele->lock_type_ == LOCK_EX && is_first_request) {
					// printf("唤醒写事务%ld ",ele->txn_id_);
					txn_table.restart_txn(txn_man->get_thd_id(), ele->txn_id_, 0);
					break;
				}
			}
		} else {  // 如果为之前的为读锁，后面的请求只可能是写锁
			// 只需唤醒的第一个事务（肯定为写锁，否则会被阻塞）
			
			auto first_request = lock_request_queue->request_queue_.begin();
			// printf("唤醒写事务%ld ",(*first_request)->txn_id_);
			txn_table.restart_txn(txn_man->get_thd_id(), (*first_request)->txn_id_, 0);
		}
		// printf("唤醒结束\n");
	} else {
		// printf("无需唤醒,此时请求队列中的数量:%ld\n",lock_request_queue->request_queue_.size());
		// lockRequestDump(row_key, table_name);
	}
	ATOM_CAS(lock_request_queue->latch_, true ,false);
	return RCOK;
}

void LockManager::depthFirstSearch(txnid_t vertex, bool& has_cycle, txnid_t& youngest_txn, unordered_map<txnid_t, bool>& onpath,
				 unordered_map<txnid_t, bool>& visited, unordered_map<txnid_t, vector<txnid_t>>& graph) {
	if (onpath[vertex]) {
		has_cycle = true;
		if (youngest_txn < vertex) {
			youngest_txn = vertex;
		}
		return ;
	}
	if (visited[vertex] || has_cycle) {
		return ;
	}
	visited[vertex] = onpath[vertex] = true;
	for (auto & to_vertex : graph[vertex]) {
		depthFirstSearch(to_vertex, has_cycle, youngest_txn, onpath, visited, graph);
		if (has_cycle) {
			if (youngest_txn < vertex) {
				youngest_txn = vertex;
			}
			break;
		}
	}
	onpath[vertex] = false;
	return ;
}

txnid_t LockManager::cycleDetection(std::unordered_map<txnid_t, std::vector<txnid_t>>& graph) {
	std::unordered_map<txnid_t, bool> visited;
	std::vector<txnid_t> vertexs;
	std::unordered_map<txnid_t, bool> onpath;
	txnid_t youngest_txn = 0;
	bool has_cycle = false;
	for (auto& [vertex, _] : graph) {
		onpath[vertex] = visited[vertex] = false;
		vertexs.push_back(vertex);
	}
	std::sort(vertexs.begin(),vertexs.end(), less<uint64_t>());
	for (auto& _vertex: vertexs) {
		if (!graph[_vertex].empty()) {
			depthFirstSearch(_vertex, has_cycle, youngest_txn, onpath, visited, graph);
		}
	}
	return youngest_txn;

}
void LockManager::deathLockDetection(uint64_t thd_id) {
	std::unordered_map<txnid_t, std::vector<txnid_t>> graph;
	// std::mutex graph_latch_;
	for (auto& [table_name ,table]: row_lock_map_) {
		while (!ATOM_CAS(row_lock_map_latch_[table_name], false, true)) {}
		for (auto& [_, row_request_queue_man]: table) {
			if (row_request_queue_man->granted_count_ > 0) {
				vector<txnid_t> granted_txnid;
				// 检测出所有未授予锁的事务，按我的代码逻辑，前面只可能是授予锁的，后面不可能有未授予锁的
				auto txn_request_iter = row_request_queue_man->request_queue_.begin();
				while (txn_request_iter != row_request_queue_man->request_queue_.end()) {
					if ((*txn_request_iter)->grant_) {
						granted_txnid.push_back((*txn_request_iter)->txn_id_);
						txn_request_iter++;
					} else {
						break;
					}
				}
				while (txn_request_iter != row_request_queue_man->request_queue_.end()) {
					if (!(*txn_request_iter)->grant_) {
						txnid_t waitting_txn = (*txn_request_iter)->txn_id_;
						for (int i = 0; i < granted_txnid.size(); i++) {
							graph[waitting_txn].push_back(granted_txnid[i]);
						}
						txn_request_iter++;
					} else {
						assert((*txn_request_iter)->grant_ == false);// 说明逻辑出错
					}
				}
			}
		}
		while (!ATOM_CAS(row_lock_map_latch_[table_name], true, false)) {}
	}
	txnid_t target_abort_txnid = cycleDetection(graph);
	if (target_abort_txnid != 0) {
		printf("死锁检测:检测到有循环依赖,终止事务%ld\n",target_abort_txnid);
		txn_table.restart_txn_abort(thd_id, target_abort_txnid);
	} else {
		printf("无死锁\n");
	}
}
void LockManager::lockRequestDump(uint64_t rowkey, string table_name) {
	printf("table_name:%s,row_key:%ld,lock_request dump\n",table_name.c_str(),rowkey);
	while (!ATOM_CAS(row_lock_map_latch_[table_name], false, true)) {}
	// assert(row_lock_map_.find(table_name) != row_lock_map_.end());
	// if (row_lock_map_[table_name].find(rowkey) != row_lock_map_[table_name].end()) {
	// 	printf("出现错误,dump 该table下的所有row请求队列\n");
	// 	int n =0;
	// 	for (auto &ele: row_lock_map_[table_name]) {
	// 		printf("%d:(row_key:%ld,grant_count_:%d) ",n++,ele.first,ele.second->granted_count_);
	// 	}
	// 	printf("\n");
	// }
	assert(row_lock_map_[table_name].find(rowkey) != row_lock_map_[table_name].end());
	auto lock_request_queue = row_lock_map_[table_name].at(rowkey);
	while (!ATOM_CAS(row_lock_map_latch_[table_name], true, false)) {}
	printf("request_granted_count:%d\n",lock_request_queue->granted_count_);
	int i = 0;
	for(auto & ele: lock_request_queue->request_queue_) {
		printf("%d:(txn_id: %ld, grant_: %d)  ",i++,ele->txn_id_,ele->grant_);
	}
	printf("dump_finish\n");
	
}


void Manager::addBlockedTxn(txnid_t txn_id, bool remote_txn, std::chrono::system_clock::time_point start_block_time) {
	while (!ATOM_CAS(blocked_txns_map_latch, false, true)) {}
	if (blocked_txns.find(txn_id) == blocked_txns.end()) {
		blocked_txns.insert({txn_id, unique_ptr<TxnEntry>(new TxnEntry(txn_id, remote_txn,start_block_time))});
	} else {
		blocked_txns[txn_id]->start_block_time_ = start_block_time;
	}

	while (!ATOM_CAS(blocked_txns_map_latch, true, false)) {}
}
void Manager::setUnblockedTxn(txnid_t txn_id) {
	while (!ATOM_CAS(blocked_txns_map_latch, false, true)) {}
	if (blocked_txns.find(txn_id) != blocked_txns.end()) {
		blocked_txns[txn_id]->blocked = false;
	}
	while (!ATOM_CAS(blocked_txns_map_latch, true, false)) {}
}
void Manager::removeTxn(txnid_t txn_id) {
	while (!ATOM_CAS(blocked_txns_map_latch, false, true)) {}
	if (blocked_txns.find(txn_id) != blocked_txns.end()) {
		blocked_txns.erase(txn_id);
	}
	while (!ATOM_CAS(blocked_txns_map_latch, true, false)) {}
}

void Manager::calculateBlockTime(uint64_t thd_id) {
	while (!ATOM_CAS(blocked_txns_map_latch, false, true)) {}
	// auto cur_time = 
	std::chrono::system_clock::time_point cur_time = std::chrono::system_clock::now();
	int block_overtime_txn_cnt = 0;
	vector<txnid_t> overtime_txns;
	for (auto &[txn_id, txn_entry] : blocked_txns) {
		if (txn_entry->blocked) {
			txn_entry->blocked_time_ = std::chrono::duration_cast<std::chrono::duration<double>>(cur_time - txn_entry->start_block_time_);
			if (txn_entry->blocked_time_ > limit_block_overtime) {
				++block_overtime_txn_cnt;
				overtime_txns.push_back(txn_id);
			}
		}
	}
	for (auto txnid : overtime_txns) {
		blocked_txns.erase(txnid);
	}
	while (!ATOM_CAS(blocked_txns_map_latch, true, false)) {}
	abortOvertimeTxn(thd_id, overtime_txns);
}
void Manager::abortOvertimeTxn(uint64_t thd_id, vector<txnid_t> &overtime_txns) {
	for (size_t i = 0; i < overtime_txns.size(); i++) {
		txn_table.restart_txn_abort(thd_id, overtime_txns[i]);
	}
}