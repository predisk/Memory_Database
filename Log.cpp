#include "Log.h"
#include <iostream>
#include<sstream>
#include <windows.h>
#include <string>
#include <sys/stat.h>
#include <assert.h>
#include <stdio.h>
#include <process.h>
#include <fstream>//二进制


#define BUFFERSIZE 1024
#define TIMER 1000//定时
using namespace std;
const char* LogPath="D:\\sample.dat";
const char* LogPath1="D:\\sample1.txt";
//初始化
Log::Log(){
    flag = 0;
    get_buffer();
    _beginthread(Timer, 0, this);
    //NULL, 0, &SecondThreadFunc, NULL, 0, &threadID
    //hThread = (HANDLE)_beginthreadex(NULL, 0, &Timer,this,0,&threadID);
 }
Log::~Log(){
    //while(ifInputFile.peek() != EOF)
    //CloseHandle( hThread );
    if(use_lenth != 0){
        write_in_disk();
    }
    delete[] bufferdata_;
}

void Log::Timer(void *p) {
    int counter = 0;
    Log *handle = (Log *)p;

    /**自动刷新*/
    while(1){
        Sleep(TIMER);
        if(flag!=0){
            handle->flush_log();
        }
    }
}


Log::get_buffer() {
    //char *bufferdata_;
    bufferdata_= new char[BUFFERSIZE];
    end_ = bufferdata_;
    start_ = bufferdata_;
    //cout << "success" << endl;
    use_lenth = 0;
    return 0;
}

Log::write(char *command) {
    int data_lenth;
    if((data_lenth = strlen(command)) != 0){
        write_in_buffer(command,data_lenth);
        cout << data_lenth <<endl;
        }
    return 0;
}

Log::write_in_buffer(char *command, int data_lenth){

    //use_lenth = reinterpret_cast<int>(end_) - reinterpret_cast<int>(start_) ;
    char endl_ = '\n';
    if((data_lenth + use_lenth + 25) > BUFFERSIZE){
        write_in_disk();
    }
    get_time();
    if(use_lenth == 0){
        strcpy(bufferdata_,time);
    }
    else{
        strncat(bufferdata_,time,26);
    }
        strncat(bufferdata_,command,data_lenth);
        strcat(bufferdata_,"\n");

    //cout << "content: " << bufferdata_ << endl;
    //cout << "test" <<endl;
    end_ += data_lenth + 25;
    //cout << end_ << endl;
    use_lenth = reinterpret_cast<int>(end_) - reinterpret_cast<int>(start_) ;
    flag = 1;
    cout << "use lenth :" << use_lenth << endl;
    return 0;

}

Log::write_in_disk() {
    //cout << "content: " << bufferdata_ << endl;
    std::ofstream ofs(LogPath, ofstream::binary|ofstream::app);
    ofs.write(bufferdata_,use_lenth);
    ofs.close();
    memset(bufferdata_,0,use_lenth);
    end_ = bufferdata_;
    use_lenth = 0;
}



Log::flush_log() {
    if(use_lenth!=0){
        write_in_disk();
        checkpoint();
        flag = 0;
    }
    /**已落盘但是有更新*/
    else if(flag != 0){
        /**更新检查点*/
        checkpoint();
        flag = 0;
    }
}

Log::checkpoint() {
    int pos;
    std::ifstream in(LogPath, ifstream::binary);
    //in.seekg (0, ifstream::beg);
    //a = in.tellg();
    in.seekg (0, ifstream::end);
    pos = in.tellg();
    in.close();
    //cout << "n = " << n << endl;
    std::ofstream out(LogPath1, ofstream::trunc|ofstream::out);
    if(out.is_open()){
        out << pos;
        out.close();
    }
    /**database*/
    //auto_preserve();

}

Log::get_time() {
    SYSTEMTIME st;
    ::GetLocalTime(&st);
    sprintf(time, "%04d-%02d-%02d %02d:%02d:%02d.%d ", st.wYear, st.wMonth, st.wDay, st.wHour,st.wMinute, st.wSecond, st.wMilliseconds);
    return 0;
}

Log::recover(){
/**判断是否成功打开 没有成功打开则pos = 0*/
    int pos;
    std::ifstream ifs(LogPath1,ifstream::out);
    if(ifs.is_open()){
        ifs>>pos;
    }
    else{
        pos = 0;
    }
    cout << pos << endl;

    ifs.close();



    std::ifstream ifs1(LogPath,ifstream::binary);
    ifs1.seekg(pos,ifstream::beg);
    string input;
    string original;
    while(getline(ifs1,original)){
        //cout<<"content = "<< s <<endl;
        input = original.substr(24);
        //cout <<"temp = "

        string head = input.substr(0,input.find(' '));
        string remain = input.substr(input.find(' ')+1,input.length());

        //"CREATE TABLE Taxi (id CHAR(10),x INT,y INT)"
        if(!head.compare("CREATE")){
            remain = remain.substr(remain.find(' ')+1,remain.length());
            Schema* s = m.creatSchema(remain);
            if(!s)
                continue;
            else
                WriteTable* w = m.creatTable(s);
        }
        
        //LOAD tableName path=C:\\Document\\Projects\\CPPtest sep=' '

        else if(!head.compare("LOAD")){
            string tableName = remain.substr(0,remain.find(' '));
            string path = remain.substr(remain.find("path")+5,remain.find("sep")-remain.find("path")-6);
            string sep = remain.substr(remain.find("sep")+5,remain.length())-remain.find("sep")-6);
            m.loadTuple(tableName,fileName,sep);
        }
        
        //INSERT into tablename (value1, value2, ..., valuen)

        else if(!head.compare("INSERT")){
            remain = remain.substr(remain.find(' ')+1,remain.length());
            string tableName = remain.substr(0,remain.find(' '));
            string values = remain.substr(remain.find('(')+1,remain.find(')')-remain.find('(')-1);
            m.insertTuple(tableName,values);
        }

        //UPDATE tablename SET field1=new_value1,field2=new_value2  <where id= value>

        else if(!head.compare("UPDATE")){
            string tableName = remain.substr(0,remain.find(' '));
            string values = remain.substr(remain.find("SET")+4,remain.find('<')-remain.find("SET")-5);
            string query = remain.substr(remain.find("where")+6,remain.find('>')-remain.find("where")-6);
            m.updateTuple(tableName,values,query);
        }



        //DELETE from tablename <where id = value>

        else if(!head.compare("DELETE")){
            remain = remain.substr(0,remain.find(' ')+1);
            string tableName = remain.substr(0,remain.find(' '));
            if(remain.find("where") >=0){
                string query = remain.substr(remain.find("where")+6,remain.find('>')-remain.find("where")-6);
                m.deleteTuple(tableName,query);
            }
            else{
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

        else if(!head.compare("RangeQuery")){
            continue;
            /*
            string tableName = remain.substr(0,remain.find(' '));
            string arg = remain.substr(remain.find('(')+1,remain.find(')')-remain.find('(')-1);
            m.rangeQuery(tableName,arg);
            */
        }


        //CLOSE tableName

        else if(!head.compare("CLOSE")){
            continue;
            /*
            string tableName = remain;
            m.close(tableName);
            */

        }
        else if(!head.compare("RECOVER"))
        {
            continue;
        }

        else{
            cout << "Invalid command" <<endl;
        }
    }
        

    ifs1.close();


}
