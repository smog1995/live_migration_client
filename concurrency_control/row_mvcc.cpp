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

//#include "mvcc.h"
#include "txn.h"
#include "row.h"
#include "manager.h"
#include "row_mvcc.h"
#include "mem_alloc.h"

void Row_mvcc::init(row_t * row) {
	_row = row;
	readreq_mvcc = NULL;
	prereq_mvcc = NULL;
	readhis = NULL;
	writehis = NULL;
	readhistail = NULL;
	writehistail = NULL;
	blatch = false;
	latch = (pthread_mutex_t *) 
		mem_allocator.alloc(sizeof(pthread_mutex_t));
	pthread_mutex_init(latch, NULL);
	whis_len = 0;
	rhis_len = 0;
	rreq_len = 0;
	preq_len = 0;
}

row_t * Row_mvcc::clear_history(TsType type, ts_t ts) {
	MVHisEntry ** queue;
	MVHisEntry ** tail;
    switch (type) {
    case R_REQ : queue = &readhis; tail = &readhistail; break;
    case W_REQ : queue = &writehis; tail = &writehistail; break;
	default: assert(false);
    }
	MVHisEntry * his = *tail;
	MVHisEntry * prev = NULL;
	row_t * row = NULL;
	while (his && his->prev && his->prev->ts < ts) {
		prev = his->prev;
		assert(prev->ts >= his->ts);
		if (row != NULL) {
			row->free_row();
			mem_allocator.free(row, sizeof(row_t));
		}
		row = his->row;
		his->row = NULL;
		return_his_entry(his);
		his = prev;
		if (type == R_REQ) rhis_len --;
		else whis_len --;
	}
	*tail = his;
	if (*tail)
		(*tail)->next = NULL;
	if (his == NULL) 
		*queue = NULL;
	return row;
}

MVReqEntry * Row_mvcc::get_req_entry() {
	return (MVReqEntry *) mem_allocator.alloc(sizeof(MVReqEntry));
}

void Row_mvcc::return_req_entry(MVReqEntry * entry) {
	mem_allocator.free(entry, sizeof(MVReqEntry));
}

MVHisEntry * Row_mvcc::get_his_entry() {
	return (MVHisEntry *) mem_allocator.alloc(sizeof(MVHisEntry));
}

void Row_mvcc::return_his_entry(MVHisEntry * entry) {
	if (entry->row != NULL) {
		entry->row->free_row();
		mem_allocator.free(entry->row, sizeof(row_t));
	}
	mem_allocator.free(entry, sizeof(MVHisEntry));
}

void Row_mvcc::buffer_req(TsType type, TxnManager * txn)
{
	MVReqEntry * req_entry = get_req_entry();
	assert(req_entry != NULL);
	req_entry->txn = txn;
	req_entry->ts = txn->get_timestamp();
	req_entry->starttime = get_sys_clock();
	if (type == R_REQ) {
		rreq_len ++;
		STACK_PUSH(readreq_mvcc, req_entry);
	} else if (type == P_REQ) {
		preq_len ++;
		STACK_PUSH(prereq_mvcc, req_entry);
	}
}

// for type == R_REQ 
//	 debuffer all non-conflicting requests
// for type == P_REQ
//   debuffer the request with matching txn.
MVReqEntry * Row_mvcc::debuffer_req( TsType type, TxnManager * txn) {
	MVReqEntry ** queue;
	MVReqEntry * return_queue = NULL;
	switch (type) {
	case R_REQ : queue = &readreq_mvcc; break;
	case P_REQ : queue = &prereq_mvcc; break;
	default: assert(false);
	}
	
	MVReqEntry * req = *queue;
	MVReqEntry * prev_req = NULL;
	if (txn != NULL) {  //  代表prewrite请求
		assert(type == P_REQ);
		//  找到该请求的指针
		while (req != NULL && req->txn != txn) {		
			prev_req = req;
			req = req->next;
		}
		assert(req != NULL);
		//  出队
		if (prev_req != NULL)  //  链表出队情况1：指针不在队头
			prev_req->next = req->next;
		else {   			   //  链表出队情况2：指针在队头
			assert( req == *queue );
			*queue = req->next;
		}
		preq_len --;
		req->next = return_queue;
		return_queue = req;
	} else {	//  代表读请求
		assert(type == R_REQ);
		// should return all non-conflicting read requests
		// The following code makes the assumption that each write op
		// must read the row first. i.e., there is no write-only operation. 
		//  返回所有非冲突的读请求，代码遵循每个写操作都必须先读取对应行，不存在仅写的操作
		uint64_t min_pts = UINT64_MAX;
		//uint64_t min_pts = (1UL << 32);
		//  找到当前prewrite请求队列中的最小时间戳，将在这个时间戳之前的所有读请求都执行
		for (MVReqEntry * preq = prereq_mvcc; preq != NULL; preq = preq->next)
			if (preq->ts < min_pts)
				min_pts = preq->ts;
		while (req != NULL) {
			if (req->ts <= min_pts) {  // 返回从小于min_pts的读请求整条链
				if (prev_req == NULL) {
					assert(req == *queue);
					*queue = (*queue)->next;
				} else 
					prev_req->next = req->next;
				rreq_len --;
				req->next = return_queue;
				return_queue = req;
				req = (prev_req == NULL)? *queue : prev_req->next;
			} else {
				prev_req = req;
				req = req->next;
			}
		}
	}
	return return_queue;
}

void Row_mvcc::insert_history( ts_t ts, row_t * row) 
{
	MVHisEntry * new_entry = get_his_entry(); 
	new_entry->ts = ts;
	new_entry->row = row;
	if (row != NULL)
		whis_len ++;
	else rhis_len ++;
	MVHisEntry ** queue = (row == NULL)? 
		&(readhis) : &(writehis);
	MVHisEntry ** tail = (row == NULL)?
		&(readhistail) : &(writehistail);
	MVHisEntry * his = *queue;
	while (his != NULL && ts < his->ts) {
		his = his->next;
	}

	if (his) {
		LIST_INSERT_BEFORE(his, new_entry,(*queue));					
		//if (his == *queue)
		//	*queue = new_entry;
	} else 
		LIST_PUT_TAIL((*queue), (*tail), new_entry);
}

bool Row_mvcc::conflict(TsType type, ts_t ts) {
	// find the unique prewrite-read couple (prewrite before read)
	// if no such couple found, no conflict. 
	// else 
	// 	 if exists writehis between them, NO conflict!!!!
	// 	 else, CONFLICT!!!
	//  对于写操作我们会用相同时间戳先插入prew再插入read,这里是不会判断为冲突的
	//  如果存在读之前先进行了写，则有冲突，
	ts_t rts;
	ts_t pts;
	if (type == R_REQ) {
		rts = ts;
		pts = 0;
		MVReqEntry * req = prereq_mvcc;
		//  寻找小于ts的时间戳最大的prewrite_request
		while (req != NULL) {
			if (req->ts < ts && req->ts > pts) { 
				pts = req->ts;
			}
			req = req->next;
		}
		//  如果没有找到preWrite请求，说明在该读请求之前不存在写请求，直接返回false
		if (pts == 0) // no such couple exists
			return false;
	} else if (type == P_REQ) {
		rts = 0;
		pts = ts;
		MVHisEntry * his = readhis;
		// 找到大于该时间戳的时间戳最小的read_history，如果没有找到说明不存在，返回false
		// 如果找到了，我们
		while (his != NULL) {
			if (his->ts > ts) {
				rts = his->ts;
			} else 
				break;
			his = his->next;
		}
		if (rts == 0) // no couple exists
			return false;
		assert(rts > pts);
	}
	MVHisEntry * whis = writehis;
	//  存在写版本在prewrite和read之间，那么不会发生冲突
	//  原因：我们的
    while (whis != NULL && whis->ts > pts) {
		if (whis->ts < rts)   //
			return false;
		whis = whis->next;
	}
	// cout << "冲突！";
	return true;
}

RC Row_mvcc::access(TxnManager * txn, TsType type, row_t * row) {
	RC rc = RCOK;
	ts_t ts = txn->get_timestamp();  ///  写入的时间戳为事务的时间戳
	uint64_t starttime = get_sys_clock();

	//  这个锁是保证只能一个线程改动mvcc版本链，实际上就是行锁
	if (g_central_man)
		glob_manager.lock_row(_row);
	else
		pthread_mutex_lock( latch );
  if (type == R_REQ) {
		// figure out if ts is in interval(prewrite(x)) 
		bool conf = conflict(type, ts);
		//  冲突了，就先暂时加入读请求队列，没有冲突则直接加入读版本链
		if ( conf && rreq_len < g_max_read_req) { //  读请求冲突，但请求队列未满，加入请求队列，不需要终止，返回rc等待
			rc = WAIT;
      //txn->wait_starttime = get_sys_clock();
        DEBUG("buf R_REQ %ld %ld\n",txn->get_txn_id(),_row->get_primary_key());
			buffer_req(R_REQ, txn);  
			txn->ts_ready = false;
		} else if (conf) {  //  请求队列已满，需要终止
			rc = Abort;
			printf("\nshould never happen. rreq_len=%ld", rreq_len);
		} else {
			// return results immediately.
			rc = RCOK;
			MVHisEntry * whis = writehis;
			while (whis != NULL && whis->ts > ts) 
				whis = whis->next;
			row_t * ret = (whis == NULL)? 
				_row : whis->row;
			txn->cur_row = ret;
			insert_history(ts, NULL);
			assert(strstr(_row->get_table_name(), ret->get_table_name()));
		}
	} else if (type == P_REQ) {
		if ( conflict(type, ts) ) { //  prewrite请求冲突，需要终止
			// cout <<"conflict";
			rc = Abort;
		} else if (preq_len < g_max_pre_req){
        DEBUG("buf P_REQ %ld %ld\n",txn->get_txn_id(),_row->get_primary_key());
			buffer_req(P_REQ, txn);
			rc = RCOK;
		} else  {
			cout << "else情况";
			rc = Abort;
		}
	} else if (type == W_REQ) { //  这才是写请求的最后一步，把prewrite请求从队列中拿出，再进行真正的写
		rc = RCOK;
		// the corresponding prewrite request is debuffered.
		insert_history(ts, row);
        DEBUG("debuf %ld %ld\n",txn->get_txn_id(),_row->get_primary_key());
		MVReqEntry * req = debuffer_req(P_REQ, txn);
		assert(req != NULL);
		return_req_entry(req); //  释放prewrite请求
		update_buffer(txn); //  这个txn的传参其实只用于运行该事务管理器的线程的时间统计
	} else if (type == XP_REQ) {
        DEBUG("debuf %ld %ld\n",txn->get_txn_id(),_row->get_primary_key());
		MVReqEntry * req = debuffer_req(P_REQ, txn);
		assert (req != NULL);
		return_req_entry(req);
		update_buffer(txn);
	} else 
		assert(false);
	
	if (rc == RCOK) {//  先做清理版本链中过期数据(活跃事务中的最小时间戳)
		if (whis_len > g_his_recycle_len || rhis_len > g_his_recycle_len) {
			ts_t t_th = glob_manager.get_min_ts(txn->get_thd_id());
			if (readhistail && readhistail->ts < t_th)
				clear_history(R_REQ, t_th);
			// Here is a tricky bug. The oldest transaction might be 
			// reading an even older version whose timestamp < t_th.
			// But we cannot recycle that version because it is still being used.
			// So the HACK here is to make sure that the first version older than
			// t_th not be recycled.
			if (whis_len > 1 && 
				writehistail->prev->ts < t_th) {
				row_t * latest_row = clear_history(W_REQ, t_th);
				if (latest_row != NULL) {
					assert(_row != latest_row);
					_row->copy(latest_row);
				}
			}
		}
	}
	
	uint64_t timespan = get_sys_clock() - starttime;
	txn->txn_stats.cc_time += timespan;
	txn->txn_stats.cc_time_short += timespan;

	if (g_central_man)
		glob_manager.release_row(_row);
	else
		pthread_mutex_unlock( latch );	
		
	return rc;
}

void Row_mvcc::update_buffer(TxnManager * txn) {
	// 处理读请求，将其执行
	MVReqEntry * ready_read = debuffer_req(R_REQ, NULL);
	MVReqEntry * req = ready_read;
	MVReqEntry * tofree = NULL;
	// 处理每一个读请求:找到这个读请求能读取到的对应版本（赋值给请求中的事务管理器cur_row)，然后使用txn_table来通知工作线程重启该事务（阻塞->活跃），然后将读请求释放
	while (req != NULL) {
		// find the version for the request
		MVHisEntry * whis = writehis;
		while (whis != NULL && whis->ts > req->ts) 
			whis = whis->next;
		row_t * row = (whis == NULL)?  //  如果能找到不为空且时间戳小于读请求的写版本，那么该读请求会读到这个写版本；如果找不到，那么读取的是原始行
			_row : whis->row;
		req->txn->cur_row = row;  //  事务读取到该版本，赋值到cur_row中
		insert_history(req->ts, NULL);//  然后加入读版本链
		assert(row->get_data() != NULL);
		assert(row->get_table() != NULL);
		assert(row->get_schema() == _row->get_schema());

		req->txn->ts_ready = true;
		uint64_t timespan = get_sys_clock() - req->starttime;
		req->txn->txn_stats.cc_block_time += timespan;
		req->txn->txn_stats.cc_block_time_short += timespan;
    	txn_table.restart_txn(txn->get_thd_id(),req->txn->get_txn_id(), 0);// 读取后就可以重启事务，开始执行了
		tofree = req;
		req = req->next;
		// free ready_read
		return_req_entry(tofree);
	}
}
