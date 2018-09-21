#include<pthread.h>
#include "global.h"
struct Node
{
    long key;
    void* addr;
    Node* pre;
    Node* next;
};

class UniqueQueue
{
typedef std::pair<long,void*>KVpair;
protected:
    Node* head_;
    Node* tail_;
    pthread_rwlock_t queue_lock_;
public:
    UniqueQueue()
    {
        head_ = 0;
        tail_ = 0;
        pthread_rwlock_init(&queue_lock_,NULL);
    }

    ~UniqueQueue()
    {
        Node* cur = head_;
        while(cur)
        {
            Node* tmp = cur;
            cur = cur->next;
            delete tmp;
        }
        pthread_rwlock_destroy(&queue_lock_);
    }

    bool insert(Node* node);
    bool delete_node(long key);
    Node* remove_node(long key);
    void* get_addr(long key);
    std::vector<KVpair> traverse();
    void close();

    void rdlock();
    void wrlock();
    void unlock();
};
