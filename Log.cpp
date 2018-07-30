#include "Log.h"
#include <iostream>
#include<sstream>
#include <windows.h>
#include <string>
#include <sys/stat.h>
#include <assert.h>
#include <stdio.h>
#include <fstream>//二进制
#define BUFFERSIZE 1024
using namespace std;
const char* LogPath="D:\\sample.dat";

//初始化
Log::Log(){
    std::ofstream ofs(LogPath, ofstream::binary|ofstream::app);
    ofs.close();
    //cout << "Success open files" << endl;
 }
Log::~Log(){
}

Log::write(char *command){
    int data_lenth;
    //cout << "Your input is : " << command << endl;
    data_lenth = strlen(command);
    //cout << "data_lenth is : " << data_lenth << endl;//12
    std::ofstream ofs(LogPath, ofstream::binary|ofstream::app);
    ofs.write(command,data_lenth);
    get_time();
    ofs.write(time,26);
    ofs.put('\n');
    ofs.close();
    //cout << time << endl;
}

Log::get_time() {
  SYSTEMTIME st;
  ::GetLocalTime(&st);
  sprintf(time, "%04d-%02d-%02d %02d:%02d:%02d.%d  ", st.wYear, st.wMonth, st.wDay, st.wHour,st.wMinute, st.wSecond, st.wMilliseconds);
}
