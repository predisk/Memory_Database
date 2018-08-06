
#include "table.h"
#include "loader.h"
#include "parser.h"
#include "Buffer.h"
#include "schema.h"
#include <cstring>
#include <vector>
#include <iterator>
#include <iostream>
#include "stdlib.h"
//#include <glob.h>
//#include <sys/mman.h>
//#include <sys/stat.h>
//#include <fcntl.h>

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

bool WriteTable::delete_solve(string& arg)
{


    char p_qr[100];
    vector<char*> id_array;
    size_t p_find;
    p_find = arg.find("where");
    arg.copy(p_qr,arg.size()-(p_find+6),p_find + 6);
    query(p_qr, id_array);
    if(id_array.empty())
    {
        cout << "Clause no result" << endl;
        return false;
    }
    /*Now delete*/
    vector<char *>::iterator it_;

    for (it_ = id_array.begin(); it_ != id_array.end();it_++)
    {
        LinkedTupleBuffer *p_tmp = data_head_;
        LinkedTupleBuffer *p_par = NULL;
        while(p_tmp)
        {
            int s = schema_->get_tuple_size();
            void *tuple_buf = p_tmp->get_head();
            while((char*)tuple_buf - (char*)(p_tmp->get_head()) <= p_tmp->cur_capacity() - s)
            {
                if(p_tmp->empty_tuple(tuple_buf))
                {
                    p_tmp->tuple_add(tuple_buf,s);
                    continue;
                }
                void* offset;
                int colindex = schema_->getColPos("id");
                offset = schema_->calc_offset(tuple_buf, colindex);
                if(strcmp((char*)offset,*it_)==0)
                {
                    if(p_tmp->delete_record(tuple_buf)==false)
                    {
                        cout<<"delete error"<<endl;
                        return false;
                    }
                    break;
                }
                p_tmp->tuple_add(tuple_buf, s);
            }
            if((char*)tuple_buf - (char*)(p_tmp->get_head()) <= p_tmp->cur_capacity() - s)
            {
                if(p_tmp->cur_capacity()==0)
                {
                    if(p_tmp==data_head_)//if the deleted node is head node
                    {
                        p_par = p_tmp->get_next();
                    }
                    else
                    {
                        p_par->set_next(p_tmp->get_next());                        
                    }
                    delete p_tmp;
                }
                break;
            }
            p_par = p_tmp;
            p_tmp = p_tmp->get_next();
        }
    

    }
}

bool WriteTable::update(string& arg)
{
    Parser* par = new Parser(' ');
    char *p_data[30] = {NULL};
    par->parse_line((char*)(arg.c_str()), (const char**)(p_data));
    vector<char*>::iterator it_;
    vector<char*> updated_field;
    vector<char*>updated_data;
    vector<int> updated_col;
    for (int i = 2; p_data[i]!=NULL;i++)
    {
        if(strcmp(p_data[i+1],"="))
        {
            updated_field.push_back(p_data[i]);
            updated_col.push_back(schema_->getColPos(updated_field[i]));
            if(updated_col[i]==-1)
            {
                cout<<p_data[i]<<"attribute isn't exist"<<endl;
                return false;
            }
        }
        else if(strcmp(p_data[i-1],"="))
        {
            updated_data.push_back(p_data[i]);
        }
    }
    char p_qr[100];
    vector<char*> id_array;
    size_t p_find;
    p_find = arg.find("where");
    arg.copy(p_qr,arg.size()-(p_find+6),p_find + 6);
    query(p_qr, id_array);
    if(id_array.empty())
    {
        cout << "Clause no result" << endl;
        return false;
    }
    //to be done: check the updated data type
    for (it_ = id_array.begin(); it_ != id_array.end();++it_)
    {

        void *tuple_buf = search_tuple(*it_);
        if(!tuple_buf)
        {
            cout<<*it_<<"doesn't exist"<<endl;
            return false;
        }
        for (int i=0; i < updated_col.size();i++)
        {
            parse_updated_data(tuple_buf, updated_data[i], updated_col[i]);
        }

    }
    return true;
}

void WriteTable::query(char q_clause[200], vector <char*>& id_res)
{
    vector<string>items;
    char* item = strtok(q_clause,",");
    while(item!=NULL)
    {
        items.push_back(item);
        item = strtok(NULL,",");
    }

    vector<string>colName;
    vector<string>value;
    vector<int>colIndex;
    for(unsigned int i=0; i<items.size(); i++)
    {
        int tmp = items[i].find("=");
        colName.push_back(items[i].substr(0,tmp));
        value.push_back(items[i].substr(tmp+1,items[i].size()));
    }
    for(unsigned int i=0; i<colName.size(); i++)
    {
        colIndex.push_back(schema_->getColPos(colName[i]));
    }

    LinkedTupleBuffer* cur = data_head_;
    TupleBuffer::Iterator itr = cur->createIterator();
    while(cur)
    {
        void* tupleAddr = itr.next();
        while(tupleAddr)
        {
            if(!cur->empty_tuple(tupleAddr))
            {
                bool same = true;
                vector<string>entry = schema_->output_tuple(tupleAddr);
                for(unsigned int i=0;i<colName.size();i++)
                {
                    if(!entry[colIndex[i]].compare(value[i]))
                    {
                        same = false;
                        break;
                    }
                }
                if(same)
                {
                    int idIdex = schema_->getColPos("id");
                    id_res.push_back((char*)(entry[idIdex].c_str()));
                }
            }
            tupleAddr = itr.next();
        }
        cur = cur->get_next();
    }
}

void WriteTable::parse_updated_data(void *dest, char *input, int col)
{
    ColumnType type = schema_->get_column_type(col);
    switch(type)
    {
        case CT_INTEGER:
            int val;
            val = atoi(input);
            schema_->write_data(dest, col, &val);
            break;
        case CT_LONG:
            long long val3;
            val3 = atoll(input);
            schema_->write_data(dest, col, &val3);
            break;
        case CT_DECIMAL:
            double val2;
            val2 = atof(input);
            schema_->write_data(dest, col, &val2);
            break;
        case CT_CHAR:
            schema_->write_data(dest, col, input);
            break;
        case CT_POINTER:
            cout<<"pointer cannot be transferred!"<<endl;
            break;
    }
}

void* WriteTable::search_tuple(char* s_id)
{
    LinkedTupleBuffer *p_tmp = data_head_;
    while(p_tmp)
    {
        int s = schema_->get_tuple_size();
        void *tuple_buf = p_tmp->get_head();
        while((char*)tuple_buf - (char*)(p_tmp->get_head()) <= p_tmp->cur_capacity() - s)
        {
            if(p_tmp->empty_tuple(tuple_buf))
            {
                p_tmp->tuple_add(tuple_buf, s);
                continue;
            }
            void* offset;
            int colindex = schema_->getColPos("id");
            offset = schema_->calc_offset(tuple_buf, colindex);
            if(strcmp((char*)offset,s_id)==0)
            {
                return tuple_buf;
            }
            p_tmp->tuple_add(tuple_buf, s);
        }
        p_tmp = p_tmp->get_next();
    }
    return NULL;
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

bool WriteTable::insert(const char* input){
    char* argv[100];
    int col = schema_->getColPos("ID");
    int arg_num = mystrtok(argv,input);
    char* id = argv[col];
    //printf("arg_num = %d\n",arg_num);
    unsigned int s = schema_ -> columnCounts();
    if(s != arg_num){
        printf("the table size and input size is different!\n");
    }  
    void* new_data = search_tuple(id);
    if(new_data == NULL){
        printf("the id is exit! can't insert again\n");
	return false;
    }
    schema_->parse_tuple(new_data,(const char**)(argv));
    return true;
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
        cur = cur -> get_next();
    }
    return ret; 
}
	
	

