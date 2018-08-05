#include"Btree.h"
#include<vector>
#include <iostream>
#include<sstream>
#include<fstream>
#include<stdlib.h>
#include<assert.h>
void Btree::close()
{
    deleteTree(pRoot_);
    pRoot_ = 0;
}
void Btree::deleteTree(Node* x)
{
    if(!x->isLeaf())
    {
        for(int i=0;i<x->pChildren_.size();i++)
        {
            deleteTree(x->pChildren_[i]);
        }
    }
    delete x;
}
int Btree::height()
{
    int ret =1;
    Node* tmp = pRoot_;
    while(1)
    {
        if(!tmp->isLeaf())
        {
            ret++;
            tmp = tmp->pChildren_[0];
        }
        else
            break;
    }
    return ret;
}

Node* Btree::new_pNode(Node* father)
{
    Node* ret = new Node(father,minDegree_);
    return ret;
}

Node* Btree::new_pNode()
{
    Node* ret = new Node(minDegree_);
    return ret;
}

void* Btree::search(int key)
{
    return searchAddr(pRoot_,key);
}

void* Btree::searchAddr(Node* ptr,int key)
{
    if(!ptr)return 0;
    int i =0;
    for(; i<ptr->keyCount(); i++)
    {
        if(key < ptr->entry_[i].first)
            break;
    }

    if(ptr->isLeaf())
        return 0;
    else
        return searchAddr(ptr->pChildren_[i],key);
}

void Btree::split(Node* x,int pos)
{
    Node* newChild = new Node(x,minDegree_);
    Node* oldChild = x->pChildren_[pos];

    KVpair middle = oldChild->entry_[minDegree_-1];

    newChild->entry_.insert(newChild->entry_.end(),oldChild->entry_.begin()+minDegree_,oldChild->entry_.end());
    oldChild->entry_.erase(oldChild->entry_.begin()+minKeyCount(),oldChild->entry_.end());


    if(!oldChild->isLeaf())
    {
        newChild->pChildren_.insert(newChild->pChildren_.begin(),oldChild->pChildren_.begin()+minDegree_,oldChild->pChildren_.end());
        oldChild->pChildren_.erase(oldChild->pChildren_.begin()+minDegree_,oldChild->pChildren_.end());
    }

    x->entry_.insert(x->entry_.begin()+pos,middle);
    x->pChildren_.insert(x->pChildren_.begin()+pos+1,newChild);
}

void Btree::create()
{
    pRoot_ = new_pNode();
}

void Btree::insert(KVpair kp)
{
    insert(kp.first,kp.second);
}

void Btree::insert(int key,void* addr)
{
    // insert to leaf
    if(!pRoot_)
        create();
    Node* r = pRoot_;
    if(r->isFull())
    {
        Node* s = new_pNode();
        pRoot_ = s;
        s->pChildren_.push_back(r);
        split(s,0);
        insertNotFull(s,key,addr);
    }
    else
        insertNotFull(r,key,addr);
}

void Btree::insertNotFull(Node* x,int key,void* addr)
{
    // suppose that x is not full
    if(x->isLeaf())
    {
        KVpair tmp = make_pair(key,addr);

        int i=0;
        for(; i<x->keyCount(); i++)
        {
            if(tmp.first < x->entry_[i].first)
                break;
        }
        x->entry_.insert(x->entry_.begin()+i,tmp);
    }
    else
    {
        //find the proper leaf of the subtree of x
        int i = 0;
        for(; i<x->keyCount(); i++)
        {
            if(key < x->entry_[i].first)
                break;
        }

        int pos = i;
        if(x->pChildren_[pos]->isFull())
        {
            split(x,pos);
            if(key > x->entry_[pos].first)
                pos++;
        }
        insertNotFull(x->pChildren_[pos],key,addr);
    }
}

int Btree::getPos(Node* cur,int key)
{
    for(int i=0; i<cur->keyCount(); i++)
    {
        if(cur->entry_[i].first == key)
            return i;
    }
    return -1;
}
bool Btree::keyIsIn(Node* x,int key)
{
    for(int i=0; i<x->keyCount(); i++)
    {
        if(x->entry_[i].first == key)
            return true;
    }
    return false;
}

KVpair Btree::predecessor(Node* x)
{
    if(x->isLeaf())
        return x->entry_[0];
    else
        return predecessor(x->pChildren_[0]);
}

KVpair Btree::successor(Node* x)
{
    int maxPos = x->keyCount()-1;
    if(x->isLeaf())
        return x->entry_[maxPos];
    else
        return successor(x->pChildren_[maxPos]);
}


void Btree::merge(Node* x,int pos)
{
    Node* left = x->pChildren_[pos];
    Node* right = x->pChildren_[pos+1];

    KVpair tmp = x->entry_[pos];

    left->entry_.push_back(tmp);

    left->entry_.insert(left->entry_.end(),right->entry_.begin(),right->entry_.end());
    left->pChildren_.insert(left->pChildren_.end(),right->pChildren_.begin(),right->pChildren_.end());

    x->entry_.erase(x->entry_.begin()+pos);
    x->pChildren_.erase(x->pChildren_.begin()+pos+1);
    delete right;
}

void Btree::remove(int key)
{
    deleteNode(pRoot_,key);
}

void Btree::deleteNode(Node* x,int key)
{
    if(keyIsIn(x,key))
    {
        int pos = getPos(x,key);
        if(x->isLeaf())
            x->entry_.erase(x->entry_.begin()+pos);
        else
        {
            Node* left = x->pChildren_[pos];
            Node* right = x->pChildren_[pos+1];
            if(left->keyCount()> minKeyCount())
            {
                KVpair leftMin = successor(left);
                Node* tmp = whichNode(leftMin.first);
                x->entry_[pos] = leftMin;
                deleteNode(tmp,leftMin.first);
            }
            else if(right->keyCount()> minKeyCount())
            {
                KVpair rightMax = predecessor(right);
                Node* tmp = whichNode(rightMax.first);
                x->entry_[pos] = rightMax;
                deleteNode(tmp,rightMax.first);
            }
            else
            {
                merge(x,pos);
                deleteNode(x->pChildren_[pos],key);
            }
        }
    }
    else
        // key is not in x
    {
        if(x->isLeaf())
        {
            cout << "the key is not in this tree." << endl;
            return;
        }
        int pos=0;
        for(; pos<x->keyCount(); pos++)
        {
            if(key < x->entry_[pos].first)
                break;
        }
        Node* cur = x->pChildren_[pos];
        if(cur->keyCount()> minKeyCount())
            deleteNode(cur,key);
        else
        {
            Node* left =0;
            Node* right = 0;
            if(pos!=0)
                left = x->pChildren_[pos-1];
            if(pos!=maxKeyCount())
                right = x->pChildren_[pos+1];
            if(left && left->keyCount()> minKeyCount())
            {
                KVpair leftMax = successor(left);
                Node* tmp = whichNode(leftMax.first);
                cur->entry_.insert(cur->entry_.begin(), x->entry_[pos-1]);
                x->entry_[pos-1] = leftMax;
                deleteNode(tmp,leftMax.first);
                deleteNode(cur,key);
            }
            else if(right && right->keyCount()> minKeyCount())
            {
                KVpair rightMin = predecessor(right);
                Node* tmp = whichNode(rightMin.first);
                cur->entry_.push_back(x->entry_[pos]);
                x->entry_[pos] = rightMin;
                deleteNode(tmp,rightMin.first);
                deleteNode(x->pChildren_[pos],key);
            }
            else if(left)
            {
                merge(x,pos-1);
                deleteNode(x->pChildren_[pos-1],key);
            }
            else
            {
                merge(x,pos);
                deleteNode(x->pChildren_[pos],key);
            }
        }
    }
}

Node* Btree::whichNode(int key)
{
    return searchNode(pRoot_,key);
}

Node* Btree::searchNode(Node* nptr,int key)
{
    if((!nptr)||(!nptr->keyCount()))
        return 0;
    int maxPos = nptr->keyCount()-1;

    if(key < nptr->entry_[0].first)
        searchNode(nptr->pChildren_[0],key);
    else if(key > nptr->entry_[maxPos].first)
        searchNode(nptr->pChildren_[maxPos+1],key);
    else
    {
        for(int i=0; i<nptr->keyCount(); i++)
        {
            if(nptr->entry_[i].first == key)
                return nptr;
        }
        return 0;
    }
}
/*------------------for show-------------------------*/

void Btree::printBrotherKey(int key)
{
    Node* cur = whichNode(key);
    if(!cur)
        cout << "the key is not in the tree" << endl;
    else
    {
        cout << "The node containing ";
        for(int i=0; i<cur->keyCount(); i++)
            cout << cur->entry_[i].first << " ";
        cout << endl;
    }
}
int Btree::printChildKey(int key)
{
    Node* cur = whichNode(key);
    if(cur)
    {
        if(!cur->isLeaf())
        {
            for(unsigned int i=0; i<cur->pChildren_.size(); i++)
            {
                Node* child = cur->pChildren_[i];

                cout << "The " << i << " child including ";
                for(int i=0; i<child->keyCount(); i++)
                    cout << child->entry_[i].first << " ";

                cout << endl;
            }
            return 1;
        }
        return 0;
    }
}
void Btree::show()
{
    if(!pRoot_)
        cout << "the tree is empty!" << endl;
    else
    {
        cout << "The h is " << height() << endl;
        int rootKey = pRoot_->entry_[0].first;
        cout << "root :" << endl;
        printBrotherKey(rootKey);
        printChildKey(rootKey);

    }

}
