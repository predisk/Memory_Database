class LOG {
	public:
		get_time();//返回当前时间
		get_buffer(data,free);//获取一块内存作为缓存
		
		/**
		 * 将每一条command转换成 时间 + commmand内容 + '\n'
		 * 获取这一条command大小
		 * 调用check_buffer计算是否超过范围
		 * 如果超过，调用write_in_disk，再重新写入，更新free指针
		 * 没有超过，直接写入buffer，更新free指针
		*/
		write_in_buffer();

		/**
		 * 当buffer满了的时候或close的时候
		 * 将buffer的内容转换成二进制流并写入磁盘
		 * 清空buffer，重置free指针
		*/
		write_in_disk();


    	Log(char *command);
    	~Log();//如果缓存不为空把缓存中的内容刷入磁盘中


    	/**
		 * 检查是否放的进去buffer
		 * 数据大小+free指针原来大小是否超过buffer的size
		 * 超过返回1，没有超过返回0
		*/
    	check_buffer();

    protected:
        /** Data segment of page. */
        char *bufferdata_;

        unsigned long maxsize_;

        /**
         * True if class owns the memory at \a *data and is responsible for its
         * deallocation, false if not.
         */
        bool owner_;

        /**
         * Marks the free section of data segment, therefore data <= free <
         * data+maxsize.
         */
        /* volatile */ char *free_;
};
