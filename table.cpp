


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

int mystrtok(char** argv,char* string)
{
    char* ptr;
    char* split = " ";
    int arg_num=0;
    ptr = strtok(string , split);
    //cout<<"ptr1 "<<ptr<<endl;
    while(ptr != NULL){
        argv[arg_num]=ptr;
        arg_num++;
        cout<<ptr<<endl;
        ptr = strtok(NULL,split);
    }
    return arg_num;
}

void WriteTable::insert(const char* input){
    char* argv[100];
    int col = schema_->getColPos("ID");
    int arg_num = mystrtok(argv,input);
    int id = atoi(argv[col]);
    //printf("arg_num = %d\n",arg_num);
    unsigned int s = schema_ -> columnCounts();
    if(s != arg_num){
        printf("the table size and input size is different!\n");
    }  
    void* new_data = search(id);
    if(new_data == NULL){
        printf("the id is exit! can't insert again\n");
    }
    schema_ -> parse_tuple(new_data,argv);
}



int Distance(int x , int y ,int X ,int Y)
{
    return sqrt((x-X)^2 + (y-Y)^2);
}

vector<void*> WriteTable::RangeQuery(int x,int y,int r)
{
    int x_col = schema_ ->getColPos("X");
    int y_col = schema_ -> getColPos("Y");
    int X,Y;
    vector<void*> ret;
    vector<string> tuple_data; 
    LinkedTupleBuffer *cur;
    cur = data_head_;
    int tuple_num = cur->cur_capacity() / schema_->get_tuple_size();
    while(cur != NULL)
    {
        for(unsigned int i=0; i<tuple_num;i++)
	{
            void* data = get_tuple_offset(i);
            bool flag=empty_tuple(data);
            if(!flag)
	    {
                tuple_data = schema_ -> output_tuple(data);
                stringstream stream(tuple_data[x_col]);
                stream >> X;
                stringstream stream(tuple_data[y_col]);
                stream >> Y;
                int dis = Distance(x,y,X,Y);
                if(dis <= r^2)
                    ret.push_back(data);
            } 
        }
        cur = cur -> set_next();
    }
    return ret; 
}


void WriteTable::insert(const char* input){
    char* argv[100];
	int arg_num = mystrtok(argv,input);
	//printf("arg_num = %d\n",arg_num);
	unsigned int s = schema_ -> get_tuple_size();
	if(s != arg_num){
        printf("the table size and input size is different!\n");
	}
	append(argv);
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

