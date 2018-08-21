#include <iostream>
#include<fstream>
#include<assert.h>
#include<thread>
#include<time.h>
#include<windows.h>
#include "schema.h"
#include "table.h"
#include "Manager.h"
#include "command.h"

using namespace std;

void workFunc();
void inputFunc();

vector<string>cmdList;
Manager m;
Command cmd;

int main()
{
    m.init(40);
    thread input(inputFunc);
    thread work(workFunc);
    input.join();
    work.join();
}

void inputFunc()
{
    ifstream in;
    in.open("test.txt");
    if(!in.is_open())
    {
        cout << "read error:the file open fail." << endl;
        return;
    }
    string line;
    while(getline(in,line))
    {
        cmdList.push_back(line);
    }
}

void workFunc()
{
    int tsCount = 0 ;
    DWORD timeTotal = 0;
    while(1)
    {
        if(!cmdList.size())
            continue;

        tsCount++;

        DWORD start,ending;
        start = ::GetTickCount();
        string input = cmdList[0];
        cmdList.erase(cmdList.begin());

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
            if((int)remain.find("where") >=0)
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
                transform(sure.begin(),sure.end(),sure.begin(),::tolower);
                if(!sure.compare("yes"))
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
            string tableName;
            double x,y,r;
            cmd.rangeQuery(remain,tableName,x,y,r);
            m.rangeQuery(tableName,x,y,r);
        }

        //CLOSE tableName
        else if(!head.compare("close"))  //ok
        {
            string tableName = remain;
            m.close(tableName);
        }
        else if(!head.compare("exit"))  //ok
        {
            m.auto_preserve();
            exit(0);
        }
        else
        {
            cout << "Invalid command" <<endl;
        }
        ending = GetTickCount();
        timeTotal += ending - start;
        cout << "/ms: " << timeTotal/tsCount << endl;
    }
}
