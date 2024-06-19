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

#ifndef _MESSAGE_H_
#define _MESSAGE_H_

#include "global.h"
#include "helper.h"
#include "logger.h"
#include "array.h"

class ycsb_request;
class LogRecord;
struct Item_no;

class Message {
public:
  virtual ~Message(){}
  static Message * create_message(char * buf); 
  static Message * create_message(BaseQuery * query, RemReqType rtype); 
  static Message * create_message(TxnManager * txn, RemReqType rtype); 
  static Message * create_message(uint64_t txn_id, RemReqType rtype); 
  static Message * create_message(uint64_t txn_id,uint64_t batch_id, RemReqType rtype); 
  static Message * create_message(LogRecord * record, RemReqType rtype); 
  static Message * create_message(RemReqType rtype); 
  static std::vector<Message*> * create_messages(char * buf); 
  static void release_message(Message * msg); 
  RemReqType rtype;
  uint64_t txn_id;
  uint64_t batch_id;
  uint64_t return_node_id;

  uint64_t wq_time;
  uint64_t mq_time;
  uint64_t ntwk_time;

  // Collect other stats
  double lat_work_queue_time;
  double lat_msg_queue_time;
  double lat_cc_block_time;
  double lat_cc_time;
  double lat_process_time;
  double lat_network_time;
  double lat_other_time;

  uint64_t mget_size();
  uint64_t get_txn_id() {return txn_id;}
  uint64_t get_batch_id() {return batch_id;}
  uint64_t get_return_id() {return return_node_id;}
  void mcopy_from_buf(char * buf);
  void mcopy_to_buf(char * buf);
  void mcopy_from_txn(TxnManager * txn);
  void mcopy_to_txn(TxnManager * txn);
  RemReqType get_rtype() {return rtype;}

  virtual uint64_t get_size() = 0;
  virtual void copy_from_buf(char * buf) = 0;
  virtual void copy_to_buf(char * buf) = 0;
  virtual void copy_to_txn(TxnManager * txn) = 0;
  virtual void copy_from_txn(TxnManager * txn) = 0;
  virtual void init() = 0;
  virtual void release() = 0;
};

// Message types
class InitDoneMessage : public Message {
public:
  void copy_from_buf(char * buf);
  void copy_to_buf(char * buf);
  void copy_from_txn(TxnManager * txn);
  void copy_to_txn(TxnManager * txn);
  uint64_t get_size();
  void init() {}
  void release() {}
};

class LiveMigrationMessage : public Message {
public:
  void init() {}
  void copy_from_buf(char * buf);
  void copy_to_buf(char* buf);
  void copy_from_txn(TxnManager * txn){}
  void copy_to_txn(TxnManager * txn){}
  uint64_t get_size() {
    uint64_t size = Message::mget_size();
    size += sizeof(finish) + sizeof(live_migration_stage) + sizeof(migration_dest_id) + sizeof(part_id) 
            + sizeof(table_index_name);
    return size;
  }
  void release();
  // 1代表第一阶段快照传输，2代表第二阶段异步传输，3代表同步传输
  int finish;
  int live_migration_stage;
  int migration_dest_id; //  要迁移到哪个目标节点,源节点在入消息队列时已指定
  int part_id;
  char table_index_name[TABLE_NAME_SIZE];
};

class LiveMigrationAckMessage : public Message {
public:
  uint8_t live_migration_stage;
  int finish;
  void init() {}
  void copy_from_buf(char * buf) {
    mcopy_from_buf(buf);
    size_t ptr = Message::mget_size();
    COPY_VAL(live_migration_stage, buf, ptr);
    COPY_VAL(finish, buf, ptr);
  }
  void copy_to_buf(char* buf) {
    mcopy_to_buf(buf);
    size_t ptr = Message::mget_size();
    COPY_BUF(buf, live_migration_stage, ptr);
    COPY_BUF(buf, finish, ptr);
  }
  void copy_from_txn(TxnManager * txn) {}
  void copy_to_txn(TxnManager * txn) {}
  uint64_t get_size() {
    uint64_t size = Message::mget_size();
    size += sizeof(live_migration_stage) + sizeof(finish);
    return size;
  }
  void release() {}
};

//  myt add：live migration step1:transport table data
class SnapshotMessage : public Message {
public:
  int finish;
  int part_id;
  // int table_name_size;
  // int buffer_size;
  int tuple_count;
  char table_index_name[TABLE_NAME_SIZE];   //  table的index_name,后来改的,比较方便
  char snapshot_buffer[MIGRATION_BUFFER_SIZE];
  void init() {
  }
  void copy_from_buf(char * buf);
  void copy_to_buf(char * buf);
    void copy_from_txn(TxnManager * txn) {}
  void copy_to_txn(TxnManager * txn) {}
  uint64_t get_size() {
    uint64_t size = Message::mget_size();
    size += sizeof(finish) + sizeof(part_id) 
            + sizeof(tuple_count) + sizeof(table_index_name) + sizeof(snapshot_buffer);
    return size;
  }
  void release();
  
};

class SnapshotAckMessage : public Message {
public:
  int copy_success;
  void init() {}
  void copy_from_buf(char * buf) {
    mcopy_from_buf(buf);
    size_t ptr = Message::mget_size();
    COPY_VAL(copy_success, buf, ptr);
  }
  void copy_to_buf(char * buf) {
    mcopy_to_buf(buf);
    size_t ptr = Message::mget_size();
    COPY_BUF(buf, copy_success, ptr);
  }
  void copy_from_txn(TxnManager * txn) {}
  void copy_to_txn(TxnManager * txn) {}
  uint64_t get_size() {
    uint64_t size = Message::mget_size();
    size += sizeof(copy_success);
    return size;
  }
  void release(){}
};

class TxnAbortMessage : public Message {
public:
  void init() {}
  void copy_from_buf(char* buf) {
    mcopy_from_buf(buf);
  }
  void copy_to_buf(char* buf) {
    mcopy_to_buf(buf);
  }
  void copy_from_txn(TxnManager* txn) {
    mcopy_from_txn(txn);
  }
  void copy_to_txn(TxnManager* txn) {
    mcopy_to_txn(txn);
  }
  uint64_t get_size() {
    return Message::mget_size();
  }
  void release() {}
};

class FinishMessage : public Message {
public:
  void copy_from_buf(char * buf);
  void copy_to_buf(char * buf);
  void copy_from_txn(TxnManager * txn);
  void copy_to_txn(TxnManager * txn);
  uint64_t get_size();
  void init() {}
  void release() {}
  bool is_abort() { return rc == Abort;}

  uint64_t pid;
  RC rc;
  //uint64_t txn_id;
  //uint64_t batch_id;
  bool readonly;
#if CC_ALG == MAAT
  uint64_t commit_timestamp;
#endif
};

class LogMessage : public Message {
public:
  void copy_from_buf(char * buf);
  void copy_to_buf(char * buf);
  void copy_from_txn(TxnManager * txn);
  void copy_to_txn(TxnManager * txn);
  uint64_t get_size();
  void init() {}
  void release(); 
  void copy_from_record(LogRecord * record);

  //Array<LogRecord*> log_records;
  LogRecord record;
};

class LogRspMessage : public Message {
public:
  void copy_from_buf(char * buf);
  void copy_to_buf(char * buf);
  void copy_from_txn(TxnManager * txn);
  void copy_to_txn(TxnManager * txn);
  uint64_t get_size();
  void init() {}
  void release() {}
};

class LogFlushedMessage : public Message {
public:
  void copy_from_buf(char * buf) {}
  void copy_to_buf(char * buf) {}
  void copy_from_txn(TxnManager * txn) {}
  void copy_to_txn(TxnManager * txn) {}
  uint64_t get_size() {return sizeof(LogFlushedMessage);}
  void init() {}
  void release() {}

};


class QueryResponseMessage : public Message {
public:
  void copy_from_buf(char * buf);
  void copy_to_buf(char * buf);
  void copy_from_txn(TxnManager * txn);
  void copy_to_txn(TxnManager * txn);
  uint64_t get_size();
  void init() {}
  void release() {}

  RC rc;
  uint64_t pid;

};

class AckMessage : public Message {
public:
  void copy_from_buf(char * buf);
  void copy_to_buf(char * buf);
  void copy_from_txn(TxnManager * txn);
  void copy_to_txn(TxnManager * txn);
  uint64_t get_size();
  void init() {}
  void release() {}

  RC rc;
#if CC_ALG == MAAT
  uint64_t lower;
  uint64_t upper;
#endif

  // For Calvin PPS: part keys from secondary lookup for sequencer response
  Array<uint64_t> part_keys;
};

class PrepareMessage : public Message {
public:
  void copy_from_buf(char * buf);
  void copy_to_buf(char * buf);
  void copy_from_txn(TxnManager * txn);
  void copy_to_txn(TxnManager * txn);
  uint64_t get_size();
  void init() {}
  void release() {}

  uint64_t pid;
  RC rc;
  uint64_t txn_id;
};

class ForwardMessage : public Message {
public:
  void copy_from_buf(char * buf);
  void copy_to_buf(char * buf);
  void copy_from_txn(TxnManager * txn);
  void copy_to_txn(TxnManager * txn);
  uint64_t get_size();
  void init() {}
  void release() {}

  RC rc;
#if WORKLOAD == TPCC
	uint64_t o_id;
#endif
};


class DoneMessage : public Message {
public:
  void copy_from_buf(char * buf);
  void copy_to_buf(char * buf);
  void copy_from_txn(TxnManager * txn);
  void copy_to_txn(TxnManager * txn);
  uint64_t get_size();
  void init() {}
  void release() {}
  uint64_t batch_id;
};

class ClientResponseMessage : public Message {
public:
  void copy_from_buf(char * buf);
  void copy_to_buf(char * buf);
  void copy_from_txn(TxnManager * txn);
  void copy_to_txn(TxnManager * txn);
  uint64_t get_size();
  void init() {}
  void release() {}

  RC rc;
  uint64_t client_startts;
};

class ClientQueryMessage : public Message {
public:
  void copy_from_buf(char * buf);
  void copy_to_buf(char * buf);
  void copy_from_query(BaseQuery * query); 
  void copy_from_txn(TxnManager * txn);
  void copy_to_txn(TxnManager * txn);
  uint64_t get_size();
  void init();
  void release();

  uint64_t pid;
  uint64_t ts;
#if CC_ALG == CALVIN
  uint64_t batch_id;
  uint64_t txn_id;
#endif
  uint64_t client_startts;
  uint64_t first_startts;
  Array<uint64_t> partitions;
};

class YCSBClientQueryMessage : public ClientQueryMessage {
public:
  void copy_from_buf(char * buf);
  void copy_to_buf(char * buf);
  void copy_from_query(BaseQuery * query);
  void copy_from_txn(TxnManager * txn);
  void copy_to_txn(TxnManager * txn);
  uint64_t get_size();
  void init(); 
  void release(); 

  Array<ycsb_request*> requests;

};

class TPCCClientQueryMessage : public ClientQueryMessage {
public:
  void copy_from_buf(char * buf);
  void copy_to_buf(char * buf);
  void copy_from_query(BaseQuery * query);
  void copy_from_txn(TxnManager * txn);
  void copy_to_txn(TxnManager * txn);
  uint64_t get_size();
  void init(); 
  void release(); 

  uint64_t txn_type;
	// common txn input for both payment & new-order
  uint64_t w_id;
  uint64_t d_id;
  uint64_t c_id;

  // payment
  uint64_t d_w_id;
  uint64_t c_w_id;
  uint64_t c_d_id;
	char c_last[LASTNAME_LEN];
  uint64_t h_amount;
  bool by_last_name;

  // new order
  Array<Item_no*> items;
  bool rbk;
  bool remote;
  uint64_t ol_cnt;
  uint64_t o_entry_d;

};

class PPSClientQueryMessage : public ClientQueryMessage {
public:
  void copy_from_buf(char * buf);
  void copy_to_buf(char * buf);
  void copy_from_query(BaseQuery * query);
  void copy_from_txn(TxnManager * txn);
  void copy_to_txn(TxnManager * txn);
  uint64_t get_size();
  void init(); 
  void release(); 

  uint64_t txn_type;

  // getparts 
  uint64_t part_key;
  // getproducts / getpartbyproduct
  uint64_t product_key;
  // getsuppliers / getpartbysupplier
  uint64_t supplier_key;

  // part keys from secondary lookup
  Array<uint64_t> part_keys;

  bool recon;

};

class QueryMessage : public Message {
public:
  void copy_from_buf(char * buf);
  void copy_to_buf(char * buf);
  void copy_from_txn(TxnManager * txn);
  void copy_to_txn(TxnManager * txn);
  uint64_t get_size();
  void init() {}
  void release() {}

  uint64_t pid;
#if CC_ALG == WAIT_DIE || CC_ALG == TIMESTAMP || CC_ALG == MVCC
  uint64_t ts;
#endif
#if CC_ALG == MVCC || CC_ALG == MVCC2PL
  uint64_t thd_id;
#elif CC_ALG == OCC 
  uint64_t start_ts;
#endif
#if MODE==QRY_ONLY_MODE
  uint64_t max_access;
#endif
};

class YCSBQueryMessage : public QueryMessage {
public:
  void copy_from_buf(char * buf);
  void copy_to_buf(char * buf);
  void copy_from_txn(TxnManager * txn);
  void copy_to_txn(TxnManager * txn);
  uint64_t get_size();
  void init();
  void release(); 

 Array<ycsb_request*> requests;

};

class TPCCQueryMessage : public QueryMessage {
public:
  void copy_from_buf(char * buf);
  void copy_to_buf(char * buf);
  void copy_from_txn(TxnManager * txn);
  void copy_to_txn(TxnManager * txn);
  uint64_t get_size();
  void init();
  void release(); 

  uint64_t txn_type;
  uint64_t state; 

	// common txn input for both payment & new-order
  uint64_t w_id;
  uint64_t d_id;
  uint64_t c_id;

  // payment
  uint64_t d_w_id;
  uint64_t c_w_id;
  uint64_t c_d_id;
	char c_last[LASTNAME_LEN];
  uint64_t h_amount;
  bool by_last_name;

  // new order
  Array<Item_no*> items;
	bool rbk;
  bool remote;
  uint64_t ol_cnt;
  uint64_t o_entry_d;

};

class PPSQueryMessage : public QueryMessage {
public:
  void copy_from_buf(char * buf);
  void copy_to_buf(char * buf);
  void copy_from_txn(TxnManager * txn);
  void copy_to_txn(TxnManager * txn);
  uint64_t get_size();
  void init();
  void release(); 

  uint64_t txn_type;
  uint64_t state; 

  // getparts 
  uint64_t part_key;
  // getproducts / getpartbyproduct
  uint64_t product_key;
  // getsuppliers / getpartbysupplier
  uint64_t supplier_key;

  // part keys from secondary lookup
  Array<uint64_t> part_keys;
};




#endif
