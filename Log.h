#ifndef LOG_H_
#define LOG_H_

class Log {
	public:
        /**
         * 初始化
        */
    	Log();


        /**
         * 调用write_in_disk,确保buffer内容都已落盘
         * 删除缓存
        */


    	~Log();
    	
        /**
         * 获取缓存
         * 设置各个指针以及use_lenth的值
        */
    	get_time();


        /**
         * 调用write_in_disk,确保buffer内容都已落盘
         * 删除缓存
        */
    	get_buffer();

        /**
         * 获取这一条command大小data_lenth
         * command不为空，则调用write_in_buffer
         * 将data_lenth和command传递
        */
        write(char *command);

        /**
         * 
         * 计算这次提交是否会超过buffer范围
         * 如果超过，调用write_in_disk
         * 写入buffer，更新end_指针
        */
    	write_in_buffer(char *command, int data_lenth);

        /**
         * 当buffer满了/整个系统close/数据库数据写入磁盘的时候调用
         * 将buffer的内容转换成二进制流并写入磁盘
         * 直接删除buffer，重新get_buffer()
         * end_和start_指针会重置
        */
    	write_in_disk();

        /**
         * 其他部分的数据写入磁盘的时候
         * 需要执行日志也落盘的操作的接口//暂定
         * 直接调用write_in_disk()
        */
        flush_log()；
    protected:
        char time[25];
        char *bufferdata_;
		void *end_;
		void *start_;
		int use_lenth;


};
#endif /* LOG_H_ */
