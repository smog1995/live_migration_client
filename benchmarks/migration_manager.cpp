
#include "migration_manager.h"
#include "msg_queue.h"

class MessageQueue;
void MigrationManager::run_live_migration_stage_1(uint64_t thd_id, Message * msg) {
    assert(ISSERVER);
    cout << "run_live_migration_stage_1" <<endl;
    //  客户端发送给源节点，源节点收到后开启迁移，发送给目标节点
    if (msg->get_rtype() == MIGRATION_MSG) {
        // cout << "(暂时只实现了快照传输)处理migration_msg" << endl;
        auto migration_msg =  dynamic_cast<LiveMigrationMessage*>(msg);
        
        if (migration_msg->live_migration_stage == SNAPSHOT_TRANS) {
            printf("migration_manager: 收到客户端传输table(%s)的请求,发送目的节点(%d),分区(%d)\n",migration_msg->table_index_name, migration_msg->migration_dest_id, migration_msg->part_id);
            _wl->transportSnapshot(thd_id, migration_msg->table_index_name, migration_msg->migration_dest_id, migration_msg->part_id);
        } else if (migration_msg->live_migration_stage == ASYNC_LOGS) {
            //  TODO:  异步日志传输阶段处理
            printf("异步日志传输阶段，目前未实现\n");
        } else if (migration_msg->live_migration_stage == SYNC_EXEC) {
            //  TODO : 同步执行阶段处理
            printf("同步执行阶段，目前未实现\n");
        }
        
    //  目标节点确认快照接收完毕，发送给源节点确认消息
    } else if (msg->get_rtype() == SNAPSHOT_MSG) {
        cout << "收到snapshotMessage" << endl;
        SnapshotMessage* snapshot_msg = (SnapshotMessage*) msg;
        _wl->copyRowData(snapshot_msg->table_index_name, snapshot_msg->part_id, 
                         snapshot_msg->tuple_count, snapshot_msg->snapshot_buffer);
        if (snapshot_msg->finish) {
            _wl->printTable(snapshot_msg->table_index_name);
            SnapshotAckMessage * msg = (SnapshotAckMessage*) Message::create_message(SNAPSHOT_ACK);
            msg->return_node_id = g_node_id;
            msg->copy_success = true;
            msg_queue.enqueue(thd_id, msg, snapshot_msg->return_node_id);
        }
    //  源节点收到目标节点的ack，通知客户端一阶段顺利执行（可以让客户端进行迁移统计）
    } else if (msg->get_rtype() == SNAPSHOT_ACK) {
        // cout << "收到snapshot_ack" << endl;
        printf("收到snapshot_ack");
        SnapshotAckMessage* snapshot_ack = dynamic_cast<SnapshotAckMessage*>(msg);
        if (snapshot_ack->copy_success) {
            LiveMigrationAckMessage * msg = (LiveMigrationAckMessage*) Message::create_message(MIGRATION_ACK);
            msg->finish = true;
            msg->live_migration_stage = SNAPSHOT_TRANS;
            msg->return_node_id = g_node_id;
            int client_node_id = 2;// 暂时这样写
            msg_queue.enqueue(thd_id, msg, client_node_id);
        } else {
            cout <<"迁移一阶段失败，需要重新启动" << endl;
        }
    }
    // return RCOK;
}

void MigrationManager::process_migration_message(uint64_t thd_id, LiveMigrationMessage * msg) {
    assert(ISSERVER);
        // LiveMigrationMessage * migration_msg =  dynamic_cast<LiveMigrationMessage*>(msg);
        // cout << &msg->finish << "finish";
        cout << msg->get_size() << "名字";
        // _wl->transportSnapshot(thd_id, msg->table_name, msg->migration_dest_id, msg->part_id);
    //  目标节点确认快照接收完毕，发送给源节点确认消息
    

}