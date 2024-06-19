
#include "global.h"

class Workload;
class Message;
class MigrationThread : public Thread {
public:
	RC 	run();
  void setup();
  void init_migration_table_list();
private:
  uint64_t last_send_time;
  uint64_t send_interval;
  int src_id;
  int dest_id;
  int part_id;
    // 1代表第一阶段快照传输，2代表第二阶段异步传输，3代表同步传输
  bool finished;
  vector<string> table_name;
  int table_count;
  string schema_file;
};