#include <iostream>
#include<fstream>
#include<assert.h>
#include "schema.h"
#include "table.h"
#include "Manager.h"
#include "command.h"

using namespace std;

int main()
{
    Manager m;
    Command cmd;
    m.init(40);//bufferSize

    while(1)
    {
        string input;
        cout << "-> ";
        getline(cin,input);

        transform(input.begin(),input.end(),input.begin(),::tolower);
        string head = input.substr(0,input.find(' '));
        string remain = input.substr(input.find(' ')+1,input.length());

        //"CREATE TABLE Taxi (id INT,x INT,y INT)"
        if(!head.compare("create"))
        {
            remain = remain.substr(remain.find(' ')+1,remain.length());
            m.create(remain);
        }

        //LOAD tableName path=C:\\Document\\Projects\\CPPtest sep=' '
        else if(!head.compare("load"))
        {

            string tableName = remain.substr(0,remain.find(' '));
            string path = remain.substr(remain.find("path")+5,remain.find("sep")-remain.find("path")-6);
            string sep = remain.substr(remain.find("sep")+5,remain.length()-remain.find("sep")-6);
            m.loadTuple(tableName,path,sep);
        }

        //INSERT into tablename (field1,field2,fieldn) VALUES (value1,value2,valuen)
        else if(!head.compare("insert"))
        {
            string tableName;
            vector<CVpair> data;
            cmd.insertTuple(input,tableName,data);
            m.insertTuple(tableName,data);
        }

        //UPDATE tablename SET field1=new_value1,field2=new_value2  <where id=value,x=3>
        else if(!head.compare("update"))
        {
            string tableName;
            vector<CVpair>clause;
            vector<CVpair>data;
            cmd.update(input,tableName,clause,data);
            m.updateTuple(tableName,clause,data);
        }

        //DELETE from tablename <where id=value,x=4>
        else if(!head.compare("delete"))
        {
            if(remain.find("where") >=0)
            {
                string tableName;
                vector<CVpair>clause;
                cmd.deleteTuple(input,tableName,clause);
                m.deleteTuple(tableName,clause);
            }
            else
            {
                cout << "If you decide to delete the whole table please input YES" << endl;
                string sure;
                getline(cin,sure);
                if(!sure.compare("YES"))
                {
                    string tableName = remain.substr(remain.find("from")+5,remain.find("<")-remain.find("from")-6);
                    m.deleteTable(tableName);
                }
                else
                    continue;
            }
        }

        //RANGEQUERY tableName (x,y,r)
        else if(!head.compare("rangequery"))
        {
            string tableName = remain.substr(0,remain.find(' '));
            string arg = remain.substr(remain.find('(')+1,remain.find(')')-remain.find('(')-1);
            double x,y,r;
            cmd.RangeQuery(arg,x,y,r);
            m.rangeQuery(tableName,x,y,r);
        }

        //CLOSE tableName
        else if(!head.compare("close"))
        {
            string tableName = remain;
            m.close(tableName);
        }
        else if(!head.compare("exit"))
        {
            exit(0);
        }
        else
        {
            cout << "Invalid command" <<endl;
        }
    }
}




















