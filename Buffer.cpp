#include "Buffer.h"
#include <iostream>
#include <cstring>

using namespace std;

/**********************************************************

			Buffer

***********************************************************/

Buffer::Buffer(unsigned long size)
{
	capacity_ = size;
	head_ = new char[size];
	free_=head_;
}

Buffer::Buffer(void* head,unsigned int size,void*free)
{
	head_=head;
	capacity_=size;
	free_=free;
	if(free==NULL)
		this->free_=reinterpret_cast<char*>(head)+size;
}


Buffer::~Buffer()
{
	delete[] reinterpret_cast<char*>(head_);
	head_=0;
	free_=NULL;
	capacity_=0;
}


bool Buffer::is_valid_add(void* loc ,unsigned int length)
{
	return (head_ <= loc) && (loc <= ((char*)head_+capacity_ -length));
}

bool Buffer::can_store(unsigned int size)
{
	return is_valid_add(free_,size);
}

void* Buffer::allocate(unsigned int len)
{
	if(!can_store(len)) return NULL;
	void* ret=free_;
	free_ = reinterpret_cast<char*>(free_)+len;
	return ret;
}

const unsigned int Buffer::used_space()
{
	return reinterpret_cast<char*>(free_)-reinterpret_cast<char*>(head_);
}

const unsigned int Buffer::cur_capacity()
{
	return capacity_;
}


/*************************************************************

					TupleBuffer

*************************************************************/

TupleBuffer::TupleBuffer(unsigned long size , unsigned int tuplesize)
	:Buffer(size),tuplesize_(tuplesize)
{
	if(size < tuplesize){
		cout<<"size must be greater than tuplesize!"<<endl;
	}
}

TupleBuffer::TupleBuffer(void* data,unsigned int size, void* free,unsigned int tuplesize)
	:Buffer(data,size,free),tuplesize_(tuplesize)
{
	if(size < tuplesize)
		cout<<"size must be greater than tuplesize!"<<endl;
}

void* TupleBuffer::get_tuple_offset(unsigned int pos)
{
	char* f = reinterpret_cast<char*>(free_);
	char* d = reinterpret_cast<char*>(head_);
	char* ret = d + static_cast<unsigned long long>(pos)*tuplesize_;
	return ret < f ? ret : 0;
}


bool TupleBuffer::is_valid_tuple_add(void* loc)
{
	return Buffer::is_valid_add(loc,tuplesize_);
}


void* TupleBuffer::allocate_tuple()
{
	return Buffer::allocate(tuplesize_);
}

void TupleBuffer::tuple_add(void *&data, int len)
{
    char *p_med = reinterpret_cast<char *>(data);
    p_med += len;
    data = reinterpret_cast<void *>(p_med); 
}

bool LinkedTupleBuffer:: delete_record(void* data)
{
	if(capacity_<=tuplesize_)
		return false;
	capacity_ -= tuplesize_;

	if((char*)data+tuplesize_==(char*)free_)
	{
		char *tmp = reinterpret_cast<char *>(free_);
		tmp -= tuplesize_;
		free_ = reinterpret_cast<void *>(tmp);
	}
	else
	{
		memset(data, -1, tuplesize_);
	}
	return true;
}
bool LinkedTupleBuffer::empty_tuple(void* data)
{
		return *(int*)data == -1;
}
