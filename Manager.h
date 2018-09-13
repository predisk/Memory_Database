#include"table.h"
#include"Buffer.h"
#include"schema.h"
#include<iostream>
#include<vector>
#include<fstream>
class Manager
{
private:
    vector<WriteTable*>openTables_; //the tables opened in the memory
    vector<bool>isModify_;  // the flags meaning the table is modified or not
    vector<string>existTables_;  // all table names the db including
    int bufferSize_;
    string path_; // the path of all table file/metadata.txt
    string fileType_; // the file type of all the table file/metadata.txt
protected:
    //load the table from disk to memory
    WriteTable* loadTable(string tableName); //ok
    
    //load schema from metadata file
    //if nonexisted return 0
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

    //save one table into disk but not delete it from memory
    bool preserve(WriteTable* ptr);

    //save all the tables in the openTables_
    int auto_preserve();

    //when created
    bool saveSchema(string schema);

    void modify(WriteTable* w);

    bool updateExistTable();

    Schema* creatSchema(string input);

    WriteTable* creatTable(Schema* s);

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
    //-----------test-------------------
    vector<bool> testModify()
    {
        return isModify_;
    }

    vector<WriteTable*> testOpen()
    {
        return openTables_;
    }

    vector<string> testExist()
    {
        return existTables_;
    }
};
