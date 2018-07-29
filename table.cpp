


#include "table.h"
#include "loader.h"
//#include <glob.h>
//#include <sys/mman.h>
//#include <sys/stat.h>
//#include <fcntl.h>
#include <iostream>
#include "stdlib.h"
//#include <assert.h>


void Table::init(Schema *s, unsigned int size)
{
    schema_ = s;
    data_head_ = 0;
    cur_ = 0;
}

 LinkedTupleBuffer* Table:: get_root()
{
    return data_head_;
}

LinkedTupleBuffer* Table:: read_next()
{
    LinkedTupleBuffer *ret = cur_;
    if(cur_)
        cur_ = cur_->get_next();
    return ret;
}
void Table:: reset()
{
    cur_ = data_head_;
}

void Table:: close()
{
    LinkedTupleBuffer *t = data_head_;
    while(t)
    {
        t = data_head_->get_next();
        delete data_head_;
        data_head_ = t;
    }
    reset();
}

void WriteTable::init(Schema* s, unsigned int size)
{
    Table::init(s, size);
    this->size_ = size;
    data_head_ = new LinkedTupleBuffer(size, s->get_tuple_size());
    last_ = data_head_;
    cur_ = data_head_;
}

Table::LoadErrorT WriteTable::load(const string& filepattern,
        const string& separators)
{
    Loader loader(separators[0]);
    loader.load(filepattern, *this);
    return LOAD_OK;
}

void WriteTable::append(const vector<string> &input)
{
    unsigned int s = schema_->get_tuple_size();
    if(! last_->can_store(s))
    {
        LinkedTupleBuffer *tmp = new LinkedTupleBuffer(size_, s);
        last_->set_next(tmp);
        last_ = tmp;
    }
    void *target = last_->allocate_tuple();
    schema_->parse_tuple(target, input);
}
void WriteTable::append(const char **data, unsigned int count)
{
    unsigned int s = schema_->get_tuple_size();
    if(!last_->can_store(s))
    {
        LinkedTupleBuffer *tmp = new LinkedTupleBuffer(size_, s);
        last_->set_next(tmp);
        last_ = tmp;
    }
    if(schema_->columnCounts() == count)
    {
        void *target = last_->allocate_tuple();
        schema_->parse_tuple(target, data);
    }
    
    else
    {
        std::cout << "ERROR COUNT!" << endl;
        //throw NotYetImplemented();
    }
}

void WriteTable::append(const void * const src)
{
    unsigned int s = schema_->get_tuple_size();
    
    if(!last_->can_store(s))
    {
        LinkedTupleBuffer *tmp = new LinkedTupleBuffer(size_, s);
        last_->set_next(tmp);
        last_ = tmp;
    }

    void *target = last_->allocate_tuple();
    if(target != NULL)
    {
        schema_->copy_tuple(target, src);
    }
}

void WriteTable::concatenate(const WriteTable &table)
{
    if(schema_->get_tuple_size() == table.schema_->get_tuple_size())
    {
        if(data_head_ == 0)
        {
            data_head_ = table.data_head_;
        }
        last_->set_next(table.data_head_);
        last_ = table.last_;
    }
}

