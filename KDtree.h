#include<vector>
#include<iostream>

using namespace std;
struct data
{
    double x_;
    double y_;
    data(double x, double y) : x_(x), y_(y){}
    data();
};

typedef pair<struct data,void*> KDpair;


class KDnode
{
    public:
      KDnode *p_left;
      KDnode *p_right;

      KDnode (KDpair coord, int dis):p_left(NULL), p_right(NULL), coord_(coord), discrim_(dis) {}
      KDnode (double x, double y, void*p, int dis):p_left(NULL), p_right(NULL), discrim_(dis){
            struct data tmp(x, y);
            coord_ = make_pair(tmp, p);
      }
      ~KDnode();

      struct data *get_val();

      KDpair *get_coord() { return &coord_; }

      int get_dis();

      int compare(struct data*cmp_coor);

      double distance(struct data *point);

    private:
      KDpair coord_;
      int discrim_;

};

class KDtree
{
    public:
      KDtree():root_(NULL) {}
      KDtree(KDnode *root):root_(root) {}

      KDnode *get_root();

      KDnode* insert(KDpair* ins_coor);

      KDnode *insert(double x, double y, void *target);

      KDnode* find_node(KDnode *&subroot, KDpair *find_coor, KDnode *&par, int& dis);

      KDnode *find_min(KDnode *&subroot, int dis);

      KDnode *find_max(KDnode *&subroot, int dis);

      bool delete_node(KDpair* delete_coor);

      bool update_coord(KDpair *update_coor);

      //bool delete_node(double x, double y, void *target);

      vector<KDpair*> RangeQuery(struct data *cent_coor, double r);

      void RangeQuery_recur(KDnode *&subroot, struct data *center, double r, vector<KDpair*> &point_res);


    private:
        KDnode *root_;
};