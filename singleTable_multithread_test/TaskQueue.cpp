#include"TaskQueue.h"

Job* TaskQueue::make_job(string cmd)
{
	Job* jp = new Job;
	jp->j_next = NULL;
	jp->cmd = cmd;
	return jp;
}

void TaskQueue::job_append(string cmd) {
	Job* jp = make_job(cmd);
	job_append(jp);
}

void TaskQueue::job_append(Job* jp) {
	pthread_rwlock_wrlock(&q_lock);
	if (q_head == NULL) {
		q_head = jp;
		q_tail = jp;
	} else {
		q_tail->j_next = jp;
		q_tail = jp;
	}
	pthread_rwlock_unlock(&q_lock);
}

Job* TaskQueue::job_remove()
{
	pthread_rwlock_wrlock(&q_lock);
	if (q_head == NULL)
	{
		pthread_rwlock_unlock(&q_lock);
		return NULL;
	}
	Job* ret = q_head;
	q_head = q_head->j_next;
	ret->j_next = NULL;
	pthread_rwlock_unlock(&q_lock);
	return ret;
}