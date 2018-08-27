#ifndef BUFFER_H_INCLUDED
#define BUFFER_H_INCLUDED

class Buffer
{
protected:
	void* head_; //the begin address of buffer
	unsigned long capacity_;  //the size of buffer
	void* free_;   //the first free block

public:
	/*
	 *Allocate an empty buffer of specified size
	*/
	Buffer(unsigned long size);
	/*
	 *create a buffer
	 *class does not own the block!
	 *parameter:
	 *	head:the start of block
	 *  size:the buffer size
	 *  free:free section of the block
	 */
	Buffer(void* head,unsigned int size,void* free);

	~Buffer();

	/*
	 *return true if the buffer can store size bytes data
	 */
	bool can_store(unsigned int size);

  	/*
	 *return a memory location and move the free pointer forward
  	 *return memory location for writing length bytes
  	 */
	void* allocate(unsigned int length);

	/*
	 *Return whether this address is valid for reading length bytes data
	 *check if loc points anywhere between head and head + capacity_ -length
	 */
	bool is_valid_add(void* loc,unsigned int length);

	/*clear the contests*/
	void clear();

	/*return the capacity of the buffer*/
	const unsigned int cur_capacity();

	/*return the used space of this buffer*/
	const unsigned int used_space();


};

class TupleBuffer : public Buffer
{
protected:
	unsigned int tuplesize_;
	unsigned int tupleCount_;

public:
	/*
	 *Create a buffer of size ,the tuple which are tuplesize bytes each
	 *tuplesize : Size of tuples in bytes.
	*/
	TupleBuffer(unsigned long size , unsigned int tuplesize);
	TupleBuffer(void* head, unsigned int size , void* free , unsigned int tuplesize);

	~TupleBuffer() {}
	
	
    	unsigned int getTupleCount();//ÐÂÔö

    	bool isEmptyBuffer();//ÐÂÔö

	/*
	 * return true if a tuple can be stored in this buffer
	 *call the function can_store() in Buffer class.
	*/
	bool can_store_tuple(){
		return can_store(tuplesize_);
	}

	/**
	 *return the pointer to the start of \a pos -th tuple,
	 * or NULL if the pos -th tuple not exist in this buffer
	 */
	void* get_tuple_offset(unsigned int pos);

	/*
	 *return whether this address is valid
	 *points is anywhere between head_ and head_ + capacity_ - tuplesize
	 * call the function is_valid_add in Buffer class
	 */
	bool is_valid_tuple_add(void* loc);

	/*
	 *Return a memory location and moves the free_ pointer forward
	 *return the memory location is good for writing a tuple, else return null
	 */
	void* allocate_tuple();

	/*each time the pointer add the number of the tuplesize*/
	void tuple_add(void *&data, int len);

	class Iterator
	{
		friend class TupleBuffer;

	private:
		int tupleid_;
		TupleBuffer *page_;

	protected:
		Iterator(TupleBuffer *p)
		{
			tupleid_ = 0;
			page_ = p;
		}

	public:
		Iterator &operator=(Iterator &rhs)
		{
			page_ = rhs.page_;
			tupleid_ = rhs.tupleid_;
			return *this;
		}

		void place(TupleBuffer *p)
		{
			page_ = p;
			reset();
		}

		void *next()
		{
			return page_->get_tuple_offset(tupleid_++);
		}

		void reset()
		{
			tupleid_ = 0;
		}
	};

	Iterator createIterator()
	{
		return Iterator(this);
	}


};

class LinkedTupleBuffer : public TupleBuffer{
public:
	LinkedTupleBuffer(void* head , unsigned int size , void* free ,
		unsigned int tuplesize)
		:TupleBuffer(head,size,free,tuplesize),next_(0)
	{ }

	/*
	 * create a buffer of size \a size ,
	 *holding tuples \a tuplesize bytes each.
	 *parameter:
	 *	size : buffer size in bytes.
	 *  tuplesize : size of one tuple
	 */
	LinkedTupleBuffer(unsigned int size, unsigned int tuplesize)
            : TupleBuffer(size, tuplesize), next_(0) { }
	/*return the head of the tuplebuffer*/
	void* get_head()
	{
		return head_;
	}
	/*delete one tuple*/
	bool delete_record(void *data);
	/*return true if the tuplesize of the data is empty,which means this tuple is deleted */
	bool empty_tuple(void *data);

	/*
     * return a pointer to next tuplebuffer.
    */
    LinkedTupleBuffer* get_next(){
    	return next_;
    }

    /*
     *set the pointer to the next LinkedTupleBuffer.
    */
    void set_next(LinkedTupleBuffer* const bucket){
    	next_=bucket;
    }

private:
	LinkedTupleBuffer* next_;

};



#endif // BUFFER_H_INCLUDED
