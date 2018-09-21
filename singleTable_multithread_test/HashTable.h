#include<iostream>
#include"UniqueQueue.h"
#include"global.h"


class HashTable
{
protected:
    static const unsigned int TABLE_SIZE = 499;
    UniqueQueue* qp_[TABLE_SIZE];
    pthread_rwlock_t table_lock_;
public:
    HashTable()
    {
        for(unsigned int i=0; i<TABLE_SIZE; i++)
            qp_[i] = 0;
        pthread_rwlock_init(&table_lock_,NULL);
    }
    ~HashTable()
    {
        pthread_rwlock_destroy(&table_lock_);
    }
    
    unsigned int slot_wrlock(long key);
    
    unsigned int slot_rdlock(long key);
    
    void slot_unlock(long key);

    void table_rdlock();
    
    void table_wrlock();
    
    void table_unlock();
    //------------test ok
    unsigned int hash_func(long key);

    bool insert(long key,void* addr);

    bool insert(Node* node);

    void* get_addr(long key);

    bool delete_node(long key);

    vector<KVpair> traverse();

    bool update_key(long oldKey,long newKey);

    void close();

};
