
#include "global.h"
#include <chrono>
class Workload;
class Message;

class MigrationThread : public Thread {
public:
	RC 	run();
  void setup();
  void init_migration_table_list();
  void change_stage();
private:
  uint64_t last_send_time;
  uint64_t send_interval;
  int src_id;
  int dest_id;
  int part_id;
  int live_migration_stage;
    // 1代表第一阶段快照传输，2代表第二阶段异步传输，3代表同步传输
  bool finished;
  RC  thread_state;   // RCOK为收到消息状态，WAITREM为等待消息状态，开始计时的判断条件
  vector<string> table_indexs_name;
  int cur_table_index;
  int table_count;
  string schema_file;

  std::chrono::system_clock::time_point start_time;
  std::chrono::system_clock::time_point receive_time;
  std::chrono::duration<double> stage_time_cost[3];
};