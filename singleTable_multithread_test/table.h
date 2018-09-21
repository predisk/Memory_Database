#ifndef TABLE_H
#define TABLE_H
#include<math.h>
#include<sstream>
#include<fstream>
#include "schema.h"
//#include "loader.h"
//#include "parser.h"
#include "HashTable.h"
#include "global.h"

class Table {
public:
    Table() : schema_(NULL),ht_(NULL) {
    	pthread_rwlock_init(&append_lock_,NULL);
    }
    ~Table() {
    	pthread_rwlock_destroy(&append_lock_);
    }

    enum LoadErrorT {
        LOAD_OK = 0
    };

    void init(Schema *s);

    Schema *schema() {
        return schema_;
    }

    vector<KVpair> query(vector<CVpair> clause);

    void printTuples();

    void printTuples(vector<void*>input);

    void append(const char **data, unsigned int count);

    void* search_tuple(long id_long);

    double distance(double x,double y,double centerX,double centerY);

    void parse_updated_data(void* dest,char* data,int col);
    //-------------lock--------------------------
    bool insert(vector<CVpair> entry);

    LoadErrorT load(const string &filepattern, const string &separators);

    bool update(vector<CVpair> clause,vector<CVpair> newCV);

    bool delete_tuple(vector<CVpair> clause);

    void RangeQuery(double centerX,double centerY,double r);

    bool save(string path ,string file_name,string file_type);

    bool close(string path ,string file_name,string file_type);

protected:
    Schema *schema_;
    HashTable* ht_;
    bool is_modify_;
    bool can_append_;
    vector<void*>append_list_;
    pthread_rwlock_t append_lock_;
};

#endif //TABLE_H
