#include<pthread.h>
#include<string>
using namespace std;
struct Job
{
	string cmd;
	Job* j_next;
};

class TaskQueue
{
private:
	Job* q_head;
	Job* q_tail;
	pthread_rwlock_t q_lock;

public:
	TaskQueue() {
		q_head = NULL;
		q_tail = NULL;
		pthread_rwlock_init(&q_lock, NULL);
	}

	Job* make_job(string cmd);
	void job_append(string cmd);
	void job_append(Job* jp);
	Job* job_remove();
};
