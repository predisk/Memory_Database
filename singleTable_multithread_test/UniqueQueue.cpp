#include"UniqueQueue.h"

bool UniqueQueue::insert(Node* node) {
    long key = node->key;
    void* addr = node->addr;

    if(!head_) {
        head_ = node;
        tail_ = node;
        return true;
    } else {
        Node* cur = head_;
        while(cur) {
            if(cur->key == key)
                return false;
            cur = cur->next;
        }
        node->pre = tail_;
        tail_->next = node;
        tail_ = node;
        return true;
    }
}

Node* UniqueQueue::remove_node(long key) {
    Node* tmp = 0;
    if(!head_) {
        cout << "UniqueQueue remove error : no such key." << endl;
        return 0;
    } else if(head_->key == key) {
        tmp = head_;
        if(head_->next) {
            head_ = head_->next;
            if(head_)
                head_->pre = 0;
        } else {
            head_ = 0;
            tail_ = 0;
        }
    } else if(tail_->key == key) {
        tmp = tail_;
        tail_ = tmp->pre;
        tail_->next = 0;
    } else {
        tmp = head_->next;
        while(tmp!=tail_) {
            if(tmp->key == key) {
                tmp->pre->next = tmp->next;
                tmp->next->pre = tmp->pre;
                break;
            }
            tmp = tmp->next;
        }
        if(tmp == tail_) {
            cout << "UniqueQueue remove error : no such key." << endl;
            return 0;
        }
    }
    return tmp;
}

bool UniqueQueue::delete_node(long key)
{
    Node* target = remove_node(key);
    if(target) {
        delete target;
        return true;
    }
    return false;
}

void* UniqueQueue::get_addr(long key) {
    Node* cur = head_;
    while(cur) {
        if(cur->key == key) {
            return cur->addr;
        }
        cur = cur->next;
    }
    cout << "UniqueQueue get_addr error: no such key." << endl;
    return 0;
}

vector<KVpair> UniqueQueue::traverse() {
   Node* cur = head_;
   vector<KVpair> ret;
   while(cur) {
        KVpair tmp_pair = make_pair(cur->key,cur->addr);
        ret.push_back(tmp_pair);
        cur = cur->next;
   }
   return ret;
}

void UniqueQueue::close() {
    Node* cur = head_;
    while(cur) {
        Node* tmp = cur;
        cur = cur->next;
        delete tmp;
    }
}

void UniqueQueue::rdlock() {
    pthread_rwlock_rdlock(&queue_lock_);
}

void UniqueQueue::wrlock() {
    pthread_rwlock_wrlock(&queue_lock_);
}

void UniqueQueue::unlock() {
    pthread_rwlock_unlock(&queue_lock_);
}
