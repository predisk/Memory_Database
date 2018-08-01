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
    //std::ofstream ofs(LogPath, ofstream::binary|ofstream::app);
    //ofs.close();
    //cout << "Success open files" << endl;
    //Sleep(TIMER);
    flag = 1;
    get_buffer();
    _beginthread(Timer, 0, this);
    //NULL, 0, &SecondThreadFunc, NULL, 0, &threadID
    //hThread = (HANDLE)_beginthreadex(NULL, 0, &Timer,this,0,&threadID);


 }
Log::~Log(){
    flag = 0;
    //CloseHandle( hThread );
    write_in_disk();
     //cout << "content: " <<endl;
     //cout<< bufferdata_ << endl;
    delete[] bufferdata_;
    cout << "1" <<endl;

}

void Log::Timer(void *p)
{
    int counter = 0;
    Log *handle = (Log *)p;

    /**自动刷新*/
    while(1){
        handle->write_in_time();
        Sleep(TIMER);
    }

}
Log::write_in_time(){
    cout << "in time" << endl;
    write_in_disk();
}

Log::get_buffer() {
    //char *bufferdata_;
    //char *end_;
    bufferdata_= new char[BUFFERSIZE];
    end_ = bufferdata_;
    start_ = bufferdata_;
    //cout << "success" << endl;
    use_lenth = 0;
    //use_lenth = reinterpret_cast<int>(end_ - start_);
    //cout<< use_lenth << endl;
    return 0;
}

Log::write(char *command){
    int data_lenth;
    if((data_lenth = strlen(command)) != 0){
        write_in_buffer(command,data_lenth);
        cout << data_lenth <<endl;
        }
    return 0;
}

Log::write_in_buffer(char *command, int data_lenth){

    use_lenth = reinterpret_cast<int>(end_) - reinterpret_cast<int>(start_) ;
    char endl_ = '\n';
    if((data_lenth + use_lenth) > BUFFERSIZE){
        write_in_disk();
    }
    get_time();
    if(use_lenth == 0){
        strcpy(bufferdata_,time);
        strncat(bufferdata_,command,data_lenth);
        strcat(bufferdata_,"\n");

    }
    else{
        //memset(bufferdata_, '0' , 1);
        strncat(bufferdata_,time,26);
        strncat(bufferdata_,command,data_lenth);
        strcat(bufferdata_,"\n");
    }
    //cout << "content: " << bufferdata_ << endl;
    //cout << "test" <<endl;
    end_ += data_lenth + 25;
    //cout << end_ << endl;
    use_lenth = reinterpret_cast<int>(end_) - reinterpret_cast<int>(start_) ;
    cout << "use lenth :" << use_lenth << endl;
    return 0;

}

Log::write_in_disk(){
    //cout << "content: " << bufferdata_ << endl;
    //cout << use_lenth << endl;
    std::ofstream ofs(LogPath, ofstream::binary|ofstream::app);
    ofs.write(bufferdata_,use_lenth);
    ofs.close();
    //memset(bufferdata_, '0' , lenth);
    //end_ = start_;
    FILE* fp=NULL;
    fp = fopen(LogPath1, "a+");
    fwrite(bufferdata_ , 1, use_lenth , fp );
    fclose (fp);
    delete[] bufferdata_;
    get_buffer();


}
Log::flush_log(){
    write_in_disk();
}

Log::get_time() {
  SYSTEMTIME st;
  ::GetLocalTime(&st);
  sprintf(time, "%04d-%02d-%02d %02d:%02d:%02d.%d ", st.wYear, st.wMonth, st.wDay, st.wHour,st.wMinute, st.wSecond, st.wMilliseconds);
  return 0;
}
