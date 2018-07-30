#ifndef LOG_H_
#define LOG_H_

class Log {
	public:
    	Log();
    	~Log();
    	write(char *command);
    	get_time();
    protected:
        char time[26]={0};


};
#endif /* LOG_H_ */
