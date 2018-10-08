

#ifndef TABLE_H
#define TABLE_H

#include<string>
#include<vector>
#include<math.h>
#include<sstream>
#include<pthread.h>
#include "Buffer.h"
#include "schema.h"
#include "loader.h"
//#include "parser.h"
#include "Btree.h"
#include "KDtree.h"

typedef pair<string,string> CVpair; //colName+value;
class Table
{
    public:
      Table() : schema_(NULL), data_head_(NULL), cur_(NULL){}
      virtual ~Table(){}

      enum LoadErrorT
      {
          LOAD_OK = 0
      };

      virtual LoadErrorT load(const string &filepattern, const string &separators)=0;

      virtual void init(Schema *s, unsigned int size);

      LinkedTupleBuffer *get_root();

      LinkedTupleBuffer *read_next();

      void reset();

      virtual void close();

      Schema *schema()
      {
          return schema_;
      }

      void print_table();


    protected:
      Schema *schema_;
      LinkedTupleBuffer *data_head_;
      LinkedTupleBuffer *cur_;
};

class WriteTable : public Table
{
    public:
        WriteTable() : last_(NULL), size_(0), isModify_(false), index_btree_(NULL), index_kdtree_(NULL)
        {
          pthread_rwlock_init(&lock_, NULL);
        }

        virtual ~WriteTable(){}

        void init(Schema *s, unsigned int size);
        /**
         * Load a txt file, where each line is a tuple and each field
         * is separated by any character in the \a sepatators string.
         */
        LoadErrorT load(const string &filepattern, const string &separators);

        /**
         * Append the input to this table,
         * creat new buckets as nucessary
         */
        /*append the vector input*/
        virtual void* append(const vector<string> &input);
        /*append the @count num input*/
        virtual void append(const char **data, unsigned int count);
        /*update the existed tuple*/
        virtual void append(const void *const src);

        /**
         * append the table to this object
         * caller must check that schemas are same/
         */
        void concatenate(const WriteTable &table);

        void query(vector<CVpair> &clause,vector <void*>& tuple_res);
        void query_nonindex(vector<CVpair> &clause, vector<void *> &tuple_res);
        void *query_index(int id_res, vector<CVpair> &clause);

        void printTuples();
        void printTuples(vector<void*>&input);

        bool insert(vector<CVpair>& entry);

        void parse_updated_data(void* dest,char* data,int col);

        bool update(vector<CVpair>& clause,vector<CVpair>& newCV);

        bool deleteTuple(string& arg);

        bool deleteTuple(vector<CVpair>& clause);

        void* search_tuple(string id);

        unsigned int tupleTotal();

        double distance(double x,double y,double centerX,double centerY);

        vector<void*> RangeQuery(double centerX,double centerY,double r);

        void setModify();
        void setUnmodify();
        bool isModify();

        void Try_rdlock();
        void Try_wrlock();
        void unlock();

    protected:
      LinkedTupleBuffer *last_;
      unsigned int size_;
      bool isModify_;
      pthread_rwlock_t lock_;
      Btree *index_btree_;
      KDtree *index_kdtree_;
};

#endif //TABLE_H
