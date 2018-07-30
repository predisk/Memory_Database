#include <iostream>
#include<fstream>
#include<assert.h>
#include"schema.h"
//#include"table.h"

using namespace std;

//vector<WriteTable*>openTables;
vector<string>existTables;

void init();
//WriteTable* isOpen(string tableName);
bool isExist(string tableName);
//WriteTable loadTable(string tableName);

int main()
{
    init();
    while(1)
    {

        string input;
        cout << "please input: ";

        getline(cin,input);

        //cout << input.compare("CREATE TABLE Taxi (id CHAR(10),x INT,y INT);");

        string head = input.substr(0,input.find(' '));
        string remain = input.substr(input.find(' ')+1,input.length());

        if(!head.compare("CREATE"))
        {
            //"CREATE TABLE Taxi (id CHAR(10),x INT,y INT);"
            string second = remain.substr(0,remain.find(' ')); //TABLE
            remain = remain.substr(remain.find(' ')+1,remain.length());

            Schema s;
            s.create(remain);
            // create a table using s
            //openTables.push_back(table)`

        }
        else if(!head.compare("LOAD"))
        {
            cout << "IS LOAD" << endl;
        }
        else if(!head.compare("INSERT"))
        {
            //log
            cout << "IS INSERT" << endl;
        }
        else if(!head.compare("UPDATE"))
        {
            //log
            cout << "IS UPDATE" << endl;
        }
        else if(!head.compare("DELETE"))
        {
            //log
            cout << "IS DELETE" << endl;
        }
        else if(!head.compare("RangeQuery"))
        {
            cout << "IS RangeQuery" <<endl;
        }
        else if(!head.compare("CLOSE"))
        {
            cout << "IS CLOSE" << endl;
        }
        else
        {
            cout << "Invalid command" <<endl;
        }
   }
}

WriteTable* isOpen(string tableName)
//test
{
    for(int i=0;i<openTables.size();i++)
    {
        if(!((openTables[i]->schema_).tableName_).compare(tableName))
            return openTables[i];
    }
    return NULL;
}

bool isExist(string tableName)
//test
{
    string tmp;
    for(int i=0;i<existTables.size();i++)
    {
        tmp = existTables[i].tableName_;
        if(!tmp.compare(tableName))
            return true;
    }
    return false;
}

void init()
{
    ifstream metadata;
    metadata.open("medata.txt");
    assert(metadata.is_open());

    string line;
    while(getline(metadata,line))
    {
        existTables.push_back(line);
    }
}
