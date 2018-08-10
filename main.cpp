#include <iostream>
#include<fstream>
#include<assert.h>
#include "schema.h"
#include "table.h"
#include "Manager.h"

using namespace std;

int main()
{
    Manager m;
    m.init(40);//bufferSize

    while(1)
    {
        string input;
        cout << "->";
        getline(cin,input);

        string head = input.substr(0,input.find(' '));
        string remain = input.substr(input.find(' ')+1,input.length());

        //"CREATE TABLE Taxi (id CHAR(10),x INT,y INT)"
        if(!head.compare("CREATE"))
        {
            remain = remain.substr(remain.find(' ')+1,remain.length());
            Schema* s = m.creatSchema(remain);
            if(!s)
                continue;
            else
                WriteTable* w = m.creatTable(s);
        }

        //LOAD tableName path=C:\\Document\\Projects\\CPPtest sep=' '
        else if(!head.compare("LOAD"))
        {

            string tableName = remain.substr(0,remain.find(' '));
            string path = remain.substr(remain.find("path")+5,remain.find("sep")-remain.find("path")-6);
            string sep = remain.substr(remain.find("sep")+5,remain.length())-remain.find("sep")-6);
            m.loadTuple(tableName,fileName,sep);
        }

        //INSERT into tablename (value1, value2, ..., valuen)
        else if(!head.compare("INSERT"))
        {
            remain = remain.substr(remain.find(' ')+1,remain.length());
            string tableName = remain.substr(0,remain.find(' '));
            string values = remain.substr(remain.find('(')+1,remain.find(')')-remain.find('(')-1);
            m.insertTuple(tableName,values);
        }

        //UPDATE tablename SET field1=new_value1,field2=new_value2  <where id= value>
        else if(!head.compare("UPDATE"))
        {
            string tableName = remain.substr(0,remain.find(' '));
            string values = remain.substr(remain.find("SET")+4,remain.find('<')-remain.find("SET")-5);
            string query = remain.substr(remain.find("where")+6,remain.find('>')-remain.find("where")-6);
            m.updateTuple(tableName,values,query);
        }

        //DELETE from tablename <where id = value>
        else if(!head.compare("DELETE"))
        {
            remain = remain.substr(0,remain.find(' ')+1);
            string tableName = remain.substr(0,remain.find(' '));
            if(remain.find("where") >=0)
            {
                string query = remain.substr(remain.find("where")+6,remain.find('>')-remain.find("where")-6);
                m.deleteTuple(tableName,query);
            }
            else
            {
                cout << "If you decide to delete the whole table please input YES" << endl;
                string sure;
                getline(cin,sure);
                if(!sure.compare("YES"))
                    m.deleteTable(tableName);
                else
                    continue;
            }
        }

        //RANGEQUERY tableName (posX,poY)
        else if(!head.compare("RangeQuery"))
        {

            string tableName = remain.substr(0,remain.find(' '));
            string arg = remain.substr(remain.find('(')+1,remain.find(')')-remain.find('(')-1);
            m.rangeQuery(tableName,arg);
        }

        //CLOSE tableName
        else if(!head.compare("CLOSE"))
        {
            string tableName = remain;
            m.close(tableName);
        }
        else
        {
            cout << "Invalid command" <<endl;
        }
    }
}
