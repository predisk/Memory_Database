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
    //_beginthread(Timer, 0, this);
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
        handle->auto_write();
    }
}

Log::auto_write() {
    //cout << "in time" << endl;
    if(use_lenth != 0){
        write_in_disk();
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
    delete[] bufferdata_;
    get_buffer();
}



Log::flush_log() {
    if(use_lenth!=0){
        write_in_disk();
        checkpoint();
        flag = 0;
    }
    else if(flag != 0){
        /**更新检查点*/
        checkpoint();
    }
}

Log::checkpoint() {
    int n;
    std::ifstream in(LogPath, ifstream::binary);
    //in.seekg (0, ifstream::beg);
    //a = in.tellg();
    in.seekg (0, ifstream::end);
    n = in.tellg();
    in.close();
    //cout << "n = " << n << endl;
    std::ofstream out(LogPath1, ofstream::trunc|ofstream::out);
    if(out.is_open()){
        out << n;
        out.close();
    }


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

    int len = 0;
    while(getline(ifs1,s)){
        cout<<"content = "<< s<<endl;
        char *data;
        s1 = s.substr(24);
        
        len = s1.length();
        data = (char *)malloc((len+1)*sizeof(char));
        s1.copy(data,len,0);
        cout<<"data = " << data<<endl;;
        delete[] data;
    }


    ifs1.close();

}

