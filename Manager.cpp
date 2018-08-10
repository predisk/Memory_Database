#include"table.h"
#include"Buffer.h"
#include"schema.h"
#include<iostream>
#include<vector>
#include<fstream>
class Manager
{
private:
    vector<WriteTable*>openTables_;
    vector<bool>isModify_;
    vector<string>existTables_;
    int bufferSize_;
    string path_;
    string fileType_;
protected:
    //load the table from disk to memory
    WriteTable* loadTable(string tableName); //ok

    Schema* loadSchema(string tableName); //ok

    //return the ptr if table is in the memory(openTables)
    //or return 0
    WriteTable* isOpen(string tableName); // ok

    //return true if table is exited(in the existTables)
    //or false
    bool isExist(string tableName);  //ok
public:
    Manager():bufferSize_(0),path_(""),fileType_(".txt") {}  //ok
    ~Manager() {}

    //init the existTables
    bool init(int bufferSize,string path=""); //ok

    //if table is existed, return the ptr to the table
    WriteTable* getTable(string tableName); //ok

    bool preserve(WriteTable* ptr);

    int auto_preserve();

    bool saveSchema(string schema);

    void modify(WriteTable* w);

    bool updateExistTable();

    Schema* creatSchema(string input);

    WriteTable* creatTable(Schema* s);

    //-------------------operator for table ------------
    bool loadTuple(string tableName,string fileName,string sep=" ");

    bool insertTuple(string tableName,string values);

    bool updateTuple(string tableName,string values,string clue);

    bool deleteTuple(string tableName,string clue);

    bool deleteTable(string tableName);

    bool rangeQuery(string tableName,string arg);

    bool close(string tableName);
};
