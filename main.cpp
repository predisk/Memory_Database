#include <iostream>
#include<fstream>
#include<assert.h>
#include"schema.h"
//#include"table.h"

using namespace std;

//WriteTable* isOpen(string tableName);
bool isExist(string tableName);
//WriteTable loadTable(string tableName);
//vector<WriteTable*>openTables;
vector<string>existTables; // all exist tableName
int main()
{


    void init();

    init();
    while(1)
    {

        string input;
        bool error = false;
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

            string tableName = remain.substr(0,remain.find(' '));
            for(int i=0;i<existTables.size();i++)
            {
                if(!existTables[i].compare(tableName))
                {
                    cout << "ERROR: the table name is repeated!" <<endl;
                    error = true;
                    break;
                }
            }
            if(error)
                continue;

            Schema s;
            if(!s.create(remain))
            {
                error = true;
                continue;
            }
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

//WriteTable* isOpen(string tableName)
////test
//{
//    for(int i=0;i<openTables.size();i++)
//    {
//        if(!((openTables[i]->schema_).tableName_).compare(tableName))
//            return openTables[i];
//    }
//    return NULL;
//}

bool isExist(string tableName)
//test
{
    for(int i=0; i<existTables.size(); i++)
    {
        if(!existTables[i].compare(tableName))
            return true;
    }
    return false;
}

void init() //OK
{
    ifstream metadata;
    metadata.open("medata.txt");
    assert(metadata.is_open());

    string line;
    string tableName;
    while(getline(metadata,line))
    {
        tableName = line.substr(0,line.find(' '));
        existTables.push_back(tableName);
    }
}






















