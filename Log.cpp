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
const char* LogPath="D:\\sample.dat";

Log::Log()
{
  get_buffer();
 }

Log::write_Log(char *command)
{
  assert(*command);
  write_in_buffer(*command);
 }


/**
 * 将每一条command转换成 时间 + commmand内容 + '\n'
 * 获取这一条command大小
 * 调用check_buffer计算是否超过范围
 * 如果超过，调用write_in_disk，再重新写入，更新end_指针
 * 没有超过，直接写入buffer，更新end_指针
*/
Log::write_in_buffer(char *command)
{
  int use_lenth,data_lenth;
  use_lenth = reinterpret_cast<int>(end_ - start_);
  data_lenth = strlen(*command);
  char endl = '\n';
  if((data_lenth + use_lenth) > BUFFERSIZE){
    write_in_buffer();
    strncat(bufferdata_,get_time(),26);
    strncat(bufferdata_,*command,data_lenth);
    strncat(bufferdata_,1,data_lenth);
  }
  else{
    strncat(bufferdata_,get_time(),26);
    strncat(bufferdata_,*command,data_lenth);
  }
  
  

}


/**
 * 
 * 当buffer满了的时候或close的时候
 * 将buffer的内容转换成二进制流并写入磁盘
 * 清空buffer，重置end_指针
*/
Log::write_in_disk()
{
  //FILE* fp=NULL;
  //fopen_s(&fp, LogPath, "a+");
  int lenth;
  char *last_time = "last store time is:";//待修改
  lenth = reinterpret_cast<int>(end_ - start_);
  std::ofstream ofs("D:\\simple2.dat", ofstream::binary|ofstream::app);
  ofs.write(bufferdata_,lenth);
  ofs.write(get_last_time(),26);//待修改
  ofs.close();
  //memset(bufferdata_, '0' , lenth);
  //end_ = start_;
  delete[] bufferdata_;
  get_buffer();
}


/**
 * 获取缓存
 * 设置end_和start_的值
*/
Log::get_buffer() 
{
  //char *bufferdata_;
  //char *end_;
  bufferdata_= new char[BUFFERSIZE];
  end_ = bufferdata_;
  start_ = bufferdata_;
}

/**
 * 如果缓存不为空,把缓存中的内容刷入磁盘中
 * 删除缓存
*/
Log::~Log() {
  delete[] bufferdata_;
  write_in_disk();
}




/**
 * 获取当前时间
 * 返回一个格式为20xx-xx-xx xx:xx:xx.xx格式的char数组
*/
Log::get_time() 
{
  SYSTEMTIME st;
  ::GetLocalTime(&st);
  char stTime[26];
  sprintf(stTime, "%04d-%02d-%02d %02d:%02d:%02d.%d  ", st.wYear, st.wMonth, st.wDay, st.wHour,st.wMinute, st.wSecond, st.wMilliseconds);
  return stTime;
}

Log::get_last_time() 
{
  SYSTEMTIME st;
  ::GetLocalTime(&st);
  char stTime[26];
  sprintf(stTime, "%04d-%02d-%02d %02d:%02d:%02d.%d\n", st.wYear, st.wMonth, st.wDay, st.wHour,st.wMinute, st.wSecond, st.wMilliseconds);
  return stTime;
}