#include "global.h"
#include "table.h"
#include "schema.h"
#include "command.h"
#include "exceptions.h" 
#include "TaskQueue.h"
#include<fstream>
#include <sstream>
#include <sys/time.h>



Schema s;
Table t;
Command cmd;
TaskQueue q;

void* handle_func(void* arg);
int main() {
	s.create("taxi (id long,x int,y int)");
    t.init(&s);
    unsigned int total = 80000;
    srand(time(NULL));
	for(unsigned int i=1; i<=total; i++) {
		//insert into taxi (id,x,y) values (long,int,int)
		string cmd = "insert into taxi (id,x,y) values (";
		ostringstream os;
		if(i%10==0)
			q.job_append("save");
		long id = i;
    	//long id = rand()%100000;
    	os << id;
    	cmd = cmd + os.str() +"," + os.str() + "," + os.str() + ")";
    	q.job_append(cmd);
	}
	q.job_append("save");

	pthread_t t1_id,t2_id,t3_id,t4_id,t5_id;
    struct timeval start,end;
    gettimeofday(&start,NULL);

    pthread_create(&t1_id, NULL, handle_func, NULL);
    pthread_create(&t2_id, NULL, handle_func, NULL);
    pthread_create(&t3_id, NULL, handle_func, NULL);
    pthread_create(&t4_id, NULL, handle_func, NULL);
    pthread_create(&t5_id, NULL, handle_func, NULL);
    pthread_join(t1_id, NULL);
    pthread_join(t2_id, NULL);
    pthread_join(t3_id, NULL);
    pthread_join(t4_id, NULL);
    pthread_join(t5_id, NULL);
    
    t.save("","taxi",".txt");
    gettimeofday(&end, NULL);

    double diff =  (end.tv_sec - start.tv_sec) * 1000000 + (end.tv_usec - start.tv_usec);
    cout << "throughout " << (total/diff)*1000000 << endl;
	return 0;
}

void* handle_func(void* arg) {
    //从任务队列中取，zhixing
	Job* jp = q.job_remove();
	while(jp) {
		string input = jp->cmd;
		delete jp;
		cout << "handle: " << input.c_str() << endl;
    
    	transform(input.begin(),input.end(),input.begin(),::tolower);
    	string head = input.substr(0,input.find(' '));
    	string remain = input.substr(input.find(' ')+1,input.length());
    	string table_name;
    
    	if(!head.compare("save")){
        	t.save("","taxi",".txt");
    	} else if(!head.compare("insert")) {
    	//INSERT into tablename (field1,field2,fieldn) VALUES (value1,value2,valuen)
        	vector<CVpair> data;
        	cmd.insertTuple(input,table_name,data);
        	t.insert(data);
    	} else if(!head.compare("update")) {
    	//UPDATE tablename SET field1=new_value1,field2=new_value2  <where id=value,x=3>
        	vector<CVpair>clause;
        	vector<CVpair>data;
        	cmd.update(input,table_name,clause,data);
        	t.update(clause,data);
    	} else if(!head.compare("delete")) {
    	//DELETE from tablename <where id=value,x=4>
        	vector<CVpair>clause;
        	cmd.deleteTuple(input,table_name,clause);
        	t.delete_tuple(clause);
    	} else if(!head.compare("rangequery")) {
    	//RANGEQUERY tableName (x,y,r)
        	string table_name = remain.substr(0,remain.find(' '));
        	string arg = remain.substr(remain.find('(')+1,remain.find(')')-remain.find('(')-1);
        	double x,y,r;

        	string strX = arg.substr(0,arg.find(","));
        	arg = arg.substr(arg.find(",")+1,arg.length());
        	string strY = arg.substr(0,arg.find(","));
        	arg = arg.substr(arg.find(",")+1,arg.length());
        	string strR = arg;
        	istringstream istX(strX),istY(strY),istR(strR);
        	istX >> x;
        	istY >> y;
        	istR >> r;
        	t.RangeQuery(x,y,r);
    	} else if(!head.compare("close")) {
    	//CLOSE tableName
        	string tableName = remain;
        	t.close("","taxi",".txt");
    	} else {
        	cout << "Invalid command" <<endl;
    	}
        jp = q.job_remove();
	}

}
