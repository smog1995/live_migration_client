#include "global.h"
#include "tpcc.h"
#include "ycsb.h"
#include "pps.h"
// #include "wl.h"
#include "message.h"
class MigrationManager {
public:
    MigrationManager() {
       
    }
    void init(TPCCWorkload* wl) {
        _wl = wl;
    }
    void run_live_migration_stage_1(uint64_t thd_id, Message * msg);
    void run_live_migration_stage_2() {}
    void run_live_migration_stage_3() {}
    void process_migration_message(uint64_t thd_id, LiveMigrationMessage * msg);
    
    // void SnapshotRowCopy(uint64_t thd_id, string& table_name, int dest_id, int part_id);
private:
    TPCCWorkload* _wl;
    int client_id;
};