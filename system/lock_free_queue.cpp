#include <assert.h>
#include <stdio.h>
#include <stdint.h>
#include <mm_malloc.h>
#include <iostream>
#include <pthread.h>
#include <unistd.h>
#include "helper.h"
#include "lock_free_queue.h"

using namespace std;

    /*  链表的实现跟我们的不太一样，一般链表顺序是node1  --> node2  --> node3
                                             |head|                 |tail|
                                   但是这里面是node1  <--  node2  <-- node3
    */
LockfreeQueue::LockfreeQueue()
{
  _head = NULL; 
  _tail = NULL; 
}


bool
LockfreeQueue::enqueue(uintptr_t value)
{
    bool success = false;
    QueueEntry * new_head = (QueueEntry *) malloc(sizeof(QueueEntry));
    new_head->value = value;
    new_head->next = NULL;
    QueueEntry * old_head;
    while (!success) {
        old_head = _head;
        success = ATOM_CAS(_head, old_head, new_head);
        if (!success)  //  宏定义，usleep,挂起1微秒
            PAUSE
    }
    if (old_head == NULL)
        _tail = new_head;
    else
        old_head->next = new_head;
    return true;
}


bool
LockfreeQueue::dequeue(uintptr_t & value)
{
    bool success = false;
    QueueEntry * old_tail;
    QueueEntry * old_head;
    while (!success) {
        old_tail = _tail;
        old_head = _head;
        //  空队列
        if (old_tail == NULL) {
            PAUSE
            return false;
        }
        //  如果队列中不止一个节点
        if (old_tail->next != NULL)
            success = ATOM_CAS(_tail, old_tail, old_tail->next);
        else if (old_tail == old_head) { //  队列只剩一个节点
            // 我们必须先交换头部，再交换尾部
            success = ATOM_CAS(_head, old_tail, NULL);
            if (success)
                ATOM_CAS(_tail, old_tail, NULL);
        }
        if (!success)
            PAUSE
    }
    value = old_tail->value;
    free(old_tail);
    return true;
}


