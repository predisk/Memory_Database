#include<vector>
#include <iostream>
#include<sstream>
#include<fstream>
#include<stdlib.h>
#include<assert.h>

/*待改进：
 把某个元素放进vector中的合适位置：直接创建一个新的vector进行复制
*/

using namespace std;

typedef pair<int,void*> KVpair;

class Node
{
protected:
    Node* father_;
    vector<KVpair>entry_; //descend
    vector<Node*>pChildren_;
    int minDegree_;
public:

    friend class Btree;

    Node(Node* father,int minDegree):father_(father),minDegree_(minDegree){}
    Node(int minDegree):father_(NULL),minDegree_(minDegree){}

    ~Node() {}

    bool isLeaf()
    {
        if(!pChildren_.size())
            return true;
        else
            return false;
    }

    bool isFull()
    {
        return (keyCount()==minDegree_*2-1);
    }

    int keyCount()
    {
        return entry_.size();
    }
};

class Btree
{
protected:
    Node* pRoot_;
    int minDegree_;

    void deleteTree(Node* x);
    void create();

    Node* new_pNode(Node* father);
    Node* new_pNode();

    void split(Node* x,int pos);

    void insertNotFull(Node* x,int key,void* addr);

    void* searchAddr(Node* nptr,int key);

    void deleteNode(Node* x,int key);

    int getPos(Node* cur,int key);

    bool keyIsIn(Node* x,int key);

    void merge(Node* parent,int pos);

    KVpair successor(Node* x);

    KVpair predecessor(Node* x);

    Node* whichNode(int key);
    Node* searchNode(Node* nptr,int key);
    /*-------------for show -----------------*/
    void printBrotherKey(int key);

    int printChildKey(int key);

public:
    Btree(int minDegree):pRoot_(NULL),minDegree_(minDegree) {}
    void close();

    int minKeyCount(){return minDegree_-1;}

    int maxKeyCount(){return 2*minDegree_-1;}

    int height();

    void insert(KVpair kp);
    void insert(int key,void* ptr);

    void* search(int key);

    void remove(int key);

    void show();
};
