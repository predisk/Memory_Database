#include"table.h"
#include"schema.h"
#include "global.h"
#include<iostream>
#include<fstream>
#include<pthread.h>
class Manager
{
private:
    vector<Table*>openTables_;
    vector<string>existTables_;
    string path_;
    string fileType_;
    pthread_rwlock_t openLock_;
    pthread_rwlock_t existLock_;
protected:
    //load the table from disk to memory
    Table* loadTable(string tableName);

    Schema* loadSchema(string tableName);

    //return the ptr if table is in the memory(openTables)
    //or return 0
    Table* isOpen(string tableName);

    //return true if table is exited(in the existTables)
    //or false
    bool isExist(string tableName);
public:

    Manager():path_(""),fileType_(".txt") 
    {
        pthread_rwlock_init(&openLock_, NULL);
        pthread_rwlock_init(&existLock_,NULL);
    }
    ~Manager() {}

    //init the existTables,that is load all tables in the db
    bool init(string path="");

    //if table is existed, return the ptr to the table
    Table* getTable(string tableName);

    bool preserve(Table* ptr);

    int auto_preserve();

    bool saveSchema(string schema);

    bool updateExistTable();

    Schema* creatSchema(string input);

    Table* creatTable(Schema* s);
    
    //-------------------operator for table ------------
    bool create(string input);

    bool loadTuple(string tableName,string fileName,string sep=" ");

    bool insertTuple(string tableName,vector<CVpair>& data);

    bool updateTuple(string tableName,vector<CVpair>& clause,vector<CVpair>& data);

    bool deleteTuple(string tableName,vector<CVpair>& clause);

    bool deleteTable(string tableName);

    bool rangeQuery(string tableName,double x,double y,double r);

    bool close(string tableName);

    bool recovery(string line);
};
