#ifndef _YLOG_H_
#define _YLOG_H_

class LOG 
{
	public:
		/**
		 * 获取当前时间
		 * 返回一个格式为20xx-xx-xx xx:xx:xx.xx格式的string
		*/
		get_time();
		get_buffer(data,end_);
		
		/**
		 * 将每一条command转换成 时间 + commmand内容 + '\n'
		 * 获取这一条command大小
		 * 调用check_buffer计算是否超过范围
		 * 如果超过，调用write_in_disk，再重新写入，更新end_指针
		 * 没有超过，直接写入buffer，更新end_指针
		*/
		write_in_buffer(char *command);

		/**
		 * 当buffer满了的时候或close的时候
		 * 将buffer的内容转换成二进制流并写入磁盘
		 * 清空buffer，重置end_指针
		*/
		write_in_disk();

		/**
		 * 初始化日志系统，调用getbuffer
		*/
    		Log(char *command);

    	
    		~Log();//如果缓存不为空把缓存中的内容刷入磁盘中


    		/**
		 * 检查是否放的进去buffer
		 * 数据大小+end_指针原来大小是否超过buffer的size
		 * 超过返回1，没有超过返回0
		*/
    		check_buffer();

    protected:
        	char *bufferdata_;
		char *end_;
		char *start_;
};
#endif /* _YLOG_H_ */
