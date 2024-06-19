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

#include "global.h"	
#include "migration_index_hash.h"
#include "mem_alloc.h"
#include "row.h"
#include "table.h"
#include "catalog.h"
#include "message.h"
#include "msg_queue.h"
RC MigrationIndexHash::init(uint64_t bucket_cnt) {
	// _bucket_cnt = bucket_cnt;
	// _bucket_cnt_per_part = bucket_cnt;
	_bucket_cnt_per_part = (bucket_cnt / g_part_cnt) + (bucket_cnt % g_part_cnt ? 1 : 0 );//  有余数时需要多加1
	_buckets = new MigrationBucketHeader * [g_part_cnt];
//   _buckets = new MigrationBucketHeader * [1];
	for (UInt32 i = 0; i < g_part_cnt; ++i) {
		_buckets[i] = (MigrationBucketHeader*) mem_allocator.alloc(sizeof(MigrationBucketHeader) * _bucket_cnt_per_part);
		uint64_t buckets_init_cnt = 0;
		for (UInt32 j = 0; j < _bucket_cnt_per_part; ++j) {
			_buckets[i][j].init();
			++buckets_init_cnt;
		}
		printf("part %u Index init with %ld buckets\n",i, buckets_init_cnt);
	}
	return RCOK;
}

// 这个函数的part_cnt没什么意义
RC 
MigrationIndexHash::init(int part_cnt, table_t * table, uint64_t bucket_cnt) {
	init(bucket_cnt);
	this->table = table;
	// cout << (table == NULL) ;
	// cout << "表初始化bucket_cnt为" << bucket_cnt << endl;
	// cout << this->table->get_schema()->get_tuple_size();
	// printf("%s表初始化bucket_cnt为%lu",this->table->get_table_name(),bucket_cnt);
	return RCOK;
}

void MigrationIndexHash::index_delete() {
  for (UInt32 i = 0; i < g_part_cnt; ++i) {
	for (UInt32 j = 0; j < _bucket_cnt_per_part; ++j) {
		(*(_buckets + i)+j)->delete_bucket();
 	}
  }
  mem_allocator.free(_buckets,sizeof(MigrationBucketHeader) * _bucket_cnt_per_part);
  delete _buckets;
}

bool MigrationIndexHash::index_exist(idx_key_t key) {
	assert(false);
}

void 
MigrationIndexHash::get_latch(MigrationBucketHeader * bucket) {
	while (!ATOM_CAS(bucket->locked, false, true)) {}
}

void 
MigrationIndexHash::release_latch(MigrationBucketHeader * bucket) {
	bool ok = ATOM_CAS(bucket->locked, true, false);
	assert(ok);
}

	//  part_id作为接口实现类，并不起作用
RC MigrationIndexHash::index_insert(idx_key_t key, itemid_t * item, int part_id) {
	RC rc = RCOK;
	uint64_t bkt_idx = hash(key);
	assert(bkt_idx < _bucket_cnt_per_part);
	MigrationBucketHeader * cur_bkt = *(_buckets+part_id)+bkt_idx;
	// 1. get the ex latch
	get_latch(cur_bkt);
	
	// 2. update the latch list
	cur_bkt->insert_item(key, item);
	
	// 3. release the latch
	release_latch(cur_bkt);
	return rc;
}
RC MigrationIndexHash::index_insert_nonunique(idx_key_t key, itemid_t * item, int part_id) {
	RC rc = RCOK;
	uint64_t bkt_idx = hash(key);
	assert(bkt_idx < _bucket_cnt_per_part);
	//MigrationBucketHeader * cur_bkt = &_buckets[part_id][bkt_idx];
	MigrationBucketHeader * cur_bkt = (*_buckets + part_id )+bkt_idx;
	// 1. get the ex latch
	get_latch(cur_bkt);
	
	// 2. update the latch list
	cur_bkt->insert_item_nonunique(key, item);
	
	// 3. release the latch
	release_latch(cur_bkt);
	return rc;
}

RC MigrationIndexHash::index_read(idx_key_t key, itemid_t * &item, int part_id) {
	uint64_t bkt_idx = hash(key);
	assert(bkt_idx < _bucket_cnt_per_part);
	MigrationBucketHeader * cur_bkt = *(_buckets + part_id) + bkt_idx;
	RC rc = RCOK;
	// 1. get the sh latch
	get_latch(cur_bkt);

	cur_bkt->read_item(key, item);
	
	// 3. release the latch
	release_latch(cur_bkt);
	return rc;

}


//  实际上不使用
RC MigrationIndexHash::index_read(idx_key_t key, int count, itemid_t * &item, int part_id) {
	uint64_t bkt_idx = hash(key);
	assert(bkt_idx < _bucket_cnt_per_part);
	MigrationBucketHeader * cur_bkt = (*_buckets + part_id) + bkt_idx;
	RC rc = RCOK;
	// 1. get the sh latch
	get_latch(cur_bkt);

	cur_bkt->read_item(key, count, item);
	
	// 3. release the latch
	release_latch(cur_bkt);
	return rc;

}


RC MigrationIndexHash::index_read(idx_key_t key, itemid_t * &item, 
						int part_id, int thd_id) {
	uint64_t bkt_idx = hash(key);
	assert(bkt_idx < _bucket_cnt_per_part);
	MigrationBucketHeader * cur_bkt = *(_buckets + part_id) + bkt_idx;
	RC rc = RCOK;
	// 1. get the sh latch
	get_latch(cur_bkt);
	cur_bkt->read_item(key, item);
	// 3. release the latch
	release_latch(cur_bkt);
	return rc;
}




/************** MigrationBucketHeader Operations ******************/

void MigrationBucketHeader::init() {
	node_cnt = 0;
	first_node = NULL;
	locked = false;
}

void MigrationBucketHeader::delete_bucket() {
	MigrationBucketNode * cur_node = first_node;
	while (cur_node != NULL) {
    ((row_t *)cur_node->items->location)->free_row();
		cur_node = cur_node->next;
	}
}


void MigrationBucketHeader::insert_item(idx_key_t key, 
		itemid_t * item) {
	MigrationBucketNode * cur_node = first_node;
	MigrationBucketNode * prev_node = NULL;
	while (cur_node != NULL) {
		if (cur_node->key == key)
			break;
		prev_node = cur_node;
		cur_node = cur_node->next;
	}
	if (cur_node == NULL) {	
		//这个桶(bucketheader)中的所有节点（bucket_node)的key均与当前key不等，因此需要创建新节点	
		MigrationBucketNode * new_node = (MigrationBucketNode *) 
			mem_allocator.alloc(sizeof(MigrationBucketNode));		
		new_node->init(key);
		new_node->items = item;
		if (prev_node != NULL) {
			new_node->next = prev_node->next;
			prev_node->next = new_node;
		} else {
			new_node->next = first_node;
			first_node = new_node;
		}
	} else {
		// cout << "哈希冲突";
		//  使用头插法将新item插入该node的item列的头部
		item->next = cur_node->items;
		cur_node->items = item;
	}
}


void MigrationBucketHeader::insert_item_nonunique(idx_key_t key, 
		itemid_t * item) 
{
  MigrationBucketNode * new_node = (MigrationBucketNode *) 
    mem_allocator.alloc(sizeof(MigrationBucketNode));		
  new_node->init(key);
  new_node->items = item;
  new_node->next = first_node;
  first_node = new_node;
}

void MigrationBucketHeader::read_item(idx_key_t key, itemid_t * &item) 
{
	MigrationBucketNode * cur_node = first_node;
	while (cur_node != NULL) {
		if (cur_node->key == key)
			break;
		cur_node = cur_node->next;
	}
	M_ASSERT_V(cur_node != NULL, "Key does not exist! %ld\n",key);
	//M_ASSERT(cur_node != NULL, "Key does not exist!");
	//M_ASSERT(cur_node->key == key, "Key does not exist!");
  //assert(cur_node != NULL);
  assert(cur_node->key == key);
	item = cur_node->items;
}

void MigrationBucketHeader::read_item(idx_key_t key, uint32_t count, itemid_t * &item) 
{
    MigrationBucketNode * cur_node = first_node;
    uint32_t ctr = 0;
    while (cur_node != NULL) {
        if (cur_node->key == key) {
            if (ctr == count) {
                break;
            }
            ++ctr;
        }
		cur_node = cur_node->next;
    }
    if (cur_node == NULL) {
        item = NULL;
        return;
    }
    M_ASSERT_V(cur_node != NULL, "Key does not exist! %ld\n",key);
    assert(cur_node->key == key);
	item = cur_node->items;
}


void MigrationIndexHash::print_index_structure() {
	IndexIterator iter;
	cout << "打印索引结构" << endl;
	auto schema = table->get_schema();
	// cout << schem
	for (int i = 0; i < g_part_cnt; ++i) {
		int row_count = 0;
		cout << "part " << i << " begin: " << endl;
		iter = getBeginIterator(i);
		
		while(!iter.IsEnd()) {
			row_count++;
			pair<idx_key_t, itemid_t* > pair = *iter;
			cout << "{" << pair.first << "} ";
			++iter;
			row_t* row = (row_t*) pair.second->location;
			// table_t* table = row->get_table();
			// cout << table->get_schema()->tuple_size << " ";
		}
		cout << endl;
		cout << "part " << i << " end,行总数为:" << row_count << endl;
	}
	
	
}

auto MigrationIndexHash::getBeginIterator(int part_id) -> IndexIterator {
	// IndexIterator iter(_buckets, _bucket_cnt);
	// return iter;
	return {_buckets, _bucket_cnt_per_part, part_id};
}



//--------------------IndexIterator-------------------------------------------------
IndexIterator::IndexIterator(MigrationBucketHeader** bucket, uint64_t bucket_cnt, int part_id)
	: _bucket(bucket), _bucket_count(bucket_cnt), _part_id(part_id) {
	_cur_bucket_index = 0;
	_cur_bucket_node = (*(_bucket + _part_id) + 0)->first_node;
	if (_cur_bucket_node == NULL) {
		++_cur_bucket_index;
		//  有些bucket里面还没有元素插入
		while (_cur_bucket_index < _bucket_count && ((*_bucket + _part_id) + _cur_bucket_index)->first_node == NULL) {
			++_cur_bucket_index;
		}
		_cur_bucket_node = _cur_bucket_index < _bucket_count ? ((*_bucket + _part_id) + _cur_bucket_index)->first_node : NULL;
	}
	_cur_item = _cur_bucket_node ? _cur_bucket_node->items : NULL;
	if (_cur_item) {
		cout  << "bucket "<< _cur_bucket_index  << ": ";
	}

	// cout << (_cur_bucket_node ? _cur_bucket_index : ) << endl;
			// -1表示遍历结束
	// cout << "容器是否为空？" <<( _cur_item == NULL )  << " "<< _cur_bucket_index;
}


auto IndexIterator::operator*() -> pair<idx_key_t, itemid_t*> {
	// cout << "*操作" <<endl;
	return {_cur_bucket_node->key, _cur_item};
}



		//  ++iter;
auto IndexIterator::operator++() -> IndexIterator& {
	// cout << "++操作" << endl;
	_cur_item = _cur_item->next;
	if (_cur_item == NULL) {
		_cur_bucket_node = _cur_bucket_node->next;
		if (_cur_bucket_node == NULL) {
			++_cur_bucket_index;
			//  有些bucket里面还没有元素插入
			while (_cur_bucket_index < _bucket_count && ((*_bucket + _part_id) + _cur_bucket_index)->first_node == NULL) {
				++_cur_bucket_index;
			}
			_cur_bucket_node = _cur_bucket_index < _bucket_count ? ((*_bucket + _part_id) + _cur_bucket_index)->first_node : NULL;
			
			// -1表示遍历结束
		}
		_cur_item = _cur_bucket_node ? _cur_bucket_node->items : NULL;
		if (_cur_item) {
		cout  << "bucket "<< _cur_bucket_index  << ": ";
	}

	}

	// 调试

	return *this;
}
auto IndexIterator::IsEnd() -> bool {
	// cout << "isend?" <<  (_cur_bucket_index == _bucket_count && _cur_bucket_node == NULL )<< endl;
	return _cur_bucket_index == _bucket_count && _cur_bucket_node == NULL;
}