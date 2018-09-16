#include "KDtree.h"
#include <iostream>
#include <vector>
#include <cmath>

using namespace std;


struct data* KDnode:: get_val()
{
    return &(this->coord_.first);
}

int KDnode::get_dis()
{
    return discrim_;
}


int KDnode::compare(struct data *cmp_coor)
{

    if(discrim_)
    {
        if(cmp_coor->x_>=coord_.first.x_)
        {
            if(cmp_coor->x_>coord_.first.x_)
            {
                return 1;
            }
            return 0;
        }
        return -1;
    }
    
    else
    {
        if(cmp_coor->y_>=coord_.first.y_)
        {
            if(cmp_coor->y_>coord_.first.y_)
            {
                return 1;
            }
            return 0;
        }
        return -1;
    }
    
}

double KDnode::distance(struct data* point)
{
    double res;
    res = sqrt(pow(coord_.first.x_ - point->x_, 2) + pow(coord_.first.y_ - point->y_, 2));
    return res;
}
KDnode* KDtree::get_root()
{
    return root_;
}

KDnode* KDtree::insert(KDpair* ins_coor)
{
    KDnode *elem = NULL;
    KDnode *par = root_;
    int ins_dis = 0;
    if((elem = find_node(root_, ins_coor, root_, ins_dis)))
    {
        cout << "this node has exist" << endl;
        return NULL;
    }
    elem = new KDnode(*ins_coor, ins_dis);
    if(!get_root())
    {
        root_ = elem;
    }
    if(par->compare(&(ins_coor->first))>0)
    {
        par->p_left = elem;
    }
    else
        par->p_right = elem;
    return elem;
}

KDnode* KDtree::insert(double x, double y, void* target)
{
    KDnode *elem = NULL;
    KDnode *par = root_;
    int ins_dis = 0;
    struct data tmp_coo(x, y);
    KDpair tmp = make_pair(tmp_coo, target);
    if((elem = find_node(root_, &tmp, root_, ins_dis)))
    {
        cout << "this node has exist" << endl;
        return NULL;
    }
    elem = new KDnode(x, y, target, ins_dis);
    if(!get_root())
    {
        root_ = elem;
    }
    if(par->compare(&(tmp.first))>0)
    {
        par->p_left = elem;
    }
    else
        par->p_right = elem;
    return elem;

}

KDnode* KDtree::find_node(KDnode *&subroot, KDpair* find_coor, KDnode*& par, int& dis)
{
    if(subroot)
    {    
        int res;
        if((res = subroot->compare(&(find_coor->first)))==0&&subroot->get_coord()->second==find_coor->second)//equal
        {
            return subroot;
        }
        par = subroot;
        dis = (dis + 1) % 2;
        if(res == 1)//subroot->coor>find_coor
        {
            return find_node(subroot->p_left, find_coor, par, dis);
        }
    
        else
        {
            return find_node(subroot->p_right, find_coor, par, dis);
        }
    }
    return NULL;
}

KDnode* KDtree::find_min(KDnode *&subroot, int dis)
{
    KDnode *temp1, *temp2;
    struct data *coord, *t1coord, *t2coord;
    if(subroot==NULL)
        return NULL;
    coord = (subroot->get_val());
    temp1 = find_min(subroot->p_left, dis);
    if(temp1 != NULL)
        t1coord = temp1->get_val();
    if(dis != subroot->get_dis())
    {
        temp2 = find_min(subroot->p_right, dis);
        if(temp2 != NULL)
            t2coord = temp2->get_val();
        if((temp1==NULL) || ((temp2 != NULL)&&temp2->compare(t1coord)<0))
            temp1 = temp2;
    }
    if(temp1 == NULL)
        return subroot;
    else
        return temp1;
}

KDnode* KDtree::find_max(KDnode *&subroot, int dis)
{
    KDnode *temp1, *temp2;
    struct data*t1coord, *t2coord, *coord;
    if(subroot==NULL)
        return NULL;
    coord = subroot->get_val();
    temp1 = find_max(subroot->p_right, dis);
    if(temp1!=NULL)
        t1coord = temp1->get_val();
    if(dis!=subroot->get_dis())
    {
        temp2 = find_max(subroot->p_left, dis);
        if(temp2!=NULL)
            t2coord = temp2->get_val();
        if((temp1==NULL) || ((temp2!=NULL)&&temp2->compare(t1coord)>0))
        {
            temp1 = temp2;
        } 
    }
    if(temp1 == NULL)
        return subroot;
    else
        return temp1;
}

bool KDtree::delete_node(KDpair* delete_coor)
{
    KDnode *del_node;
    KDnode *par_node = root_;
    int dis = 0;
    if((del_node = find_node(root_, delete_coor, par_node, dis))==NULL)
    {
        cout<<"the deleted node isn't exist"<<endl;
        return false;
    }
    KDnode *child = NULL;
    if(!del_node->p_left)
        child = find_min(del_node->p_right, (del_node->get_dis()+1)%2);
    else
        child = find_max(del_node, (del_node->get_dis()+1)%2);
    if(par_node->compare(del_node->get_val())>0)
        par_node->p_left = child;
    else
        par_node->p_right = child;
    delete del_node;
    return true;
}
bool KDtree::update_coord(KDpair* update_coor)
{
    int dis = 0;
    KDnode *update_node;
    if((update_node = find_node(root_, update_coor, root_, dis)))
    {
        update_node->get_coord()->first.x_ = update_coor->first.x_;
        update_node->get_coord()->first.x_ = update_coor->first.x_;
        return true;
    }
    cout << "Don't find the target node in KDtree" << endl;
    return false;
}

vector<KDpair*> KDtree::RangeQuery(struct data* center, double r)
{
    vector<KDpair*> point_res;
    RangeQuery_recur(root_, center, r, point_res);
    return point_res;
}
void KDtree:: RangeQuery_recur(KDnode*& subroot, struct data* center, double r, vector<KDpair*>& point_res)
{
    if(subroot == NULL)
        return;
    if(subroot->distance(center)<=r)
    {
        point_res.push_back(subroot->get_coord());
    }
    if(subroot->get_dis())//y
    {
        if(subroot->get_val()->y_+r>center->y_)
        {
            RangeQuery_recur(subroot->p_left, center, r, point_res);
        }
        if(subroot->get_val()->y_-r<center->y_)
        {
            RangeQuery_recur(subroot->p_right, center, r, point_res);
        }
 
    }
    
    else
    {
        if(subroot->get_val()->x_+r>center->x_)
        {
            RangeQuery_recur(subroot->p_left, center, r, point_res);
        }
        if(subroot->get_val()->x_-r<center->x_)
        {
            RangeQuery_recur(subroot->p_right, center, r, point_res);
        }
    }  
}



