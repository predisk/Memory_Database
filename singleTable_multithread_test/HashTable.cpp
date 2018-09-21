#include"HashTable.h"
#include<utility>
bool HashTable::insert(long key,void* addr) {
    Node* tmp = new Node;
    tmp->key = key;
    tmp->addr = addr;
    tmp->pre = 0;
    tmp->next = 0;
    if(insert(tmp))
        return true;
    cout << "HashTable insert error : The key is repeated!" << endl;
    return false;
}

bool HashTable::insert(Node* node) {
    unsigned int slot = hash_func(node->key);
    if(!qp_[slot]) {
        qp_[slot] = new UniqueQueue;
    }
    return qp_[slot]->insert(node);
}

bool HashTable::delete_node(long key) {
    unsigned int slot = hash_func(key);
    if(!qp_[slot])
        return false;
    return qp_[slot]->delete_node(key);
}

bool HashTable::update_key(long old_key,long new_key) {
    unsigned int old_pos = hash_func(old_key);
    unsigned int new_pos = hash_func(new_key);
    if(!qp_[old_pos]) {
        cout << "HashTable error:old_key is nonexsitent." << endl;
        return false;
    }
    Node* target = qp_[old_pos]->remove_node(old_key);
    if(target) {
        target->key = new_key;
        if(qp_[new_pos]->insert(target)) {
            return true;
        } else { //the new key is repeated
            cout << "HashTable update_key error:the new key is repeated." << endl;
            target->key = old_key;
            qp_[old_pos]->insert(target);
            return false;
        }
    } else { //no such key
        cout << "HashTable update error:no such key" <<endl;
        return false;
    }
}

void* HashTable::get_addr(long key)
{
    unsigned int slot = hash_func(key);
    if(!qp_[slot]){
        cout << "HashTable get_addr error:no such key" << endl;
        return 0;
    }
    return qp_[slot]->get_addr(key);
}

unsigned int HashTable::hash_func(long key) {
    return (unsigned int)key%TABLE_SIZE;
}

vector<KVpair> HashTable::traverse() {
    std::vector<KVpair> ret;
    for(unsigned int i=0; i< TABLE_SIZE;i++) {
        if(qp_[i]) {
            vector<KVpair>tmp = qp_[i]->traverse();
            ret.insert(ret.end(),tmp.begin(),tmp.end());
        }
    }
    return ret;
}

void HashTable::close() {
    for(unsigned int i=0; i<TABLE_SIZE; i++) {
        if(qp_[i]){
            qp_[i]->close();
            qp_[i] = 0;
        }
    }
}

unsigned int HashTable::slot_wrlock(long key) {
    unsigned int slot= hash_func(key);
    if(!qp_[slot]) {
        qp_[slot] = new UniqueQueue;
    }
    qp_[slot]->wrlock();
    return slot;
}

unsigned int HashTable::slot_rdlock(long key) {
    unsigned int slot= hash_func(key);
    if(!qp_[slot]) {
        qp_[slot] = new UniqueQueue;
    }
    qp_[slot]->rdlock();
    return slot;
}

void HashTable::slot_unlock(long key) {
    unsigned int slot = hash_func(key);
    if(!qp_[slot]) {
        qp_[slot] = new UniqueQueue;
    }
    qp_[slot]->unlock();
}



void HashTable::table_rdlock() {
    pthread_rwlock_rdlock(&table_lock_);
}

void HashTable::table_wrlock() {
    pthread_rwlock_wrlock(&table_lock_);
}

void HashTable::table_unlock() {
    pthread_rwlock_unlock(&table_lock_);
}



