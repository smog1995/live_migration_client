#include "txn.h"
#include "row.h"
#include "manager.h"
#include "row_mvcc.h"
#include "mem_alloc.h"

class table_t;
class Catalog;
class TxnManager;

// struct MVReqEntry {
// 	TxnManager * txn;
// 	ts_t ts;
// 	ts_t starttime;
// 	MVReqEntry * next;
// };
struct MVCCTransNode {	
	ts_t ts;
	uint64_t lsn;  // 数据所在的日志
	uint64_t seq_no_;  // 事务中的sql number,可能一个存储过程有多个相同sql no 的行版本
	row_t * row;
	uint8_t  flag_;  //事务状态（未提交、已提交、终止） 
	MVHisEntry * next;
	MVHisEntry * prev;
};