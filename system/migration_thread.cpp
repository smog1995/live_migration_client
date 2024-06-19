#include "global.h"
#include "thread.h"
#include "migration_thread.h"
#include "client_thread.h"
#include "ycsb_query.h"
#include "tpcc_query.h"
#include "client_query.h"
#include "transport.h"
#include "client_txn.h"
#include "msg_thread.h"
#include "msg_queue.h"
#include "work_queue.h"
#include "message.h"
#include "wl.h"
class Workload;
void MigrationThread::setup() {
    cout << "migrationThread setup" << endl;
    schema_file = "benchmarks/TPCC_full_schema.txt";
    finished = false;
    cout << schema_file << "路径";
    init_migration_table_list();
}
void MigrationThread::init_migration_table_list() {
    ifstream fin(schema_file);
    string line;
    // assert(fin.is_open());
     if (!fin.is_open()) {
        cerr << "Error: Could not open file " << schema_file << endl;
        cerr << "Reason: " << strerror(errno) << endl;
    }
    while (getline(fin, line)) {
      // cout << line << " line";
		if (line.compare(0, 6, "TABLE=") == 0) {
            table_name.emplace_back(string(&line[6]));
            ++table_count;
            cout << table_name[table_count - 1] << "table_name" << endl;
        }
    }
    cout << "init_migration_table_list finish" << endl;
}

RC MigrationThread::run() {
  tsetup();
  printf("Running LiveMigrationThread \n");
  int index = 0;
  LiveMigrationMessage* start_msg = (LiveMigrationMessage*)Message::create_message(MIGRATION_MSG);
//   start_msg->src_id = 0;
  start_msg->migration_dest_id = 1;
  start_msg->part_id = 0;
  // start_msg->table_name_size = table_name[index].size();
  start_msg->finish = false;
  start_msg->live_migration_stage = 1;
  // start_msg->table_name = (char*) mem_allocator.alloc(20);
  // start_msg->table_name = new char[start_msg->table_name_size];
  memcpy(start_msg->table_name, table_name[index].c_str(), table_name[index].size());
  cout << start_msg->table_name << "发送迁移卡其消息";
  msg_queue.enqueue(0, start_msg, 0); // 发送到服务器0
  while(!finished) {
    Message* msg = work_queue.migration_dequeue();
    if(!msg) {
      continue;
    }
    if (msg->get_rtype() == MIGRATION_ACK) {
        LiveMigrationAckMessage *migration_ack = (LiveMigrationAckMessage*) msg;
        if (migration_ack->finish && migration_ack->live_migration_stage == 1) {
            cout << "阶段" << migration_ack->live_migration_stage << "完成" << endl;
            finished = true;
        }
    } else {
        cout << "msg类型为" << msg->get_rtype() << " ";
    }
   
  }

  fflush(stdout);
	return FINISH;
}
