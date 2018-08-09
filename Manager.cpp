#include"Manager.h"

bool Manager::init(int bufferSize,string path)
{
    bufferSize_ = bufferSize;
    path_ = path;
    string fileName = "metadata";
    string fileType = ".txt";
    string full = path_+fileName+fileType;
    ifstream in;
    in.open(full.c_str());
    if(in.is_open())
    {
        string line;
        while(getline(in,line))
        {
            string tableName = line.substr(0,line.find(" "));
            existTables_.push_back(tableName);
        }
        return true;
    }
    else
    {
        cout << "ERROR: open metadata error when init." << endl;
        return false;
    }
}

WriteTable* Manager::getTable(string tableName)
{
    if(isExist(tableName))
    {
        WriteTable* ret = isOpen(tableName);
        if(!ret)
            ret = loadTable(tableName);
        return ret;
    }
    else
        return 0;
}

WriteTable* Manager::isOpen(string tableName)
{
    for(unsigned int i=0; i<openTables_.size(); i++)
    {
        WriteTable* table = openTables_[i];
        Schema* s = table->schema();
        if(!(s->getTableName()).compare(tableName))
            return openTables_[i];
    }
    return 0;
}

bool Manager::isExist(string tableName)
{
    for(unsigned int i=0; i<existTables_.size(); i++)
    {
        if(!tableName.compare(existTables_[i]))
            return true;
    }
    return false;
}

Schema* Manager::loadSchema(string tableName)
{
    string fileType = ".txt";
    string fileName = "metadata";
    string full = path_+fileName+fileType;
    ifstream in;
    in.open(full.c_str());
    if(in.is_open())
    {
        string line;
        while(getline(in,line))
        {
            string lineTableName = line.substr(0,line.find(" "));
            if(!lineTableName.compare(tableName))
            {
                Schema* ret = new Schema;
                if(ret==0)
                    cout << "ERROR: memory allocate for schema fail." << endl;
                else
                    ret->create(line);
                return ret;
            }
        }
        return 0;
    }
    else
    {
        cout << "ERROR: open metadata error when load schema." << endl;
        return 0;
    }

}

WriteTable* Manager::loadTable(string tableName)
{
    string fileType = ".txt";
    string full = path_+tableName+fileType;
    ifstream in;
    in.open(full.c_str());
    if(in.is_open())
    {
        Schema* s =loadSchema(tableName);
        WriteTable* ret = new WriteTable;
        if(ret==0)
            cout << "ERROR: memory allocate for table fail." << endl;
        else
        {
            ret->init(s,bufferSize_);
            ret->load(full," ");
            openTables_.push_back(ret);
            isModify_.push_back(false);
            in.close();
        }
        return ret;
    }
    else
    {
        cout << "ERROR: open table file error when load table." << endl;
        return 0;
    }
}

bool Manager::preserve(WriteTable* ptr)
{
    int index;
    for(unsigned int i=0; i<openTables_.size(); i++)
    {
        if(ptr == openTables_[i])
        {
            index = i;
            break;
        }
    }
    //if table is modified then preserve
    if(isModify_[index])
    {
        string tableName = (ptr->schema())->getTableName();
        string fileName = tableName;
        string fileType = ".txt";
        string full = path_+fileName+fileType;
        ofstream out;
        out.open(full.c_str(),ios::out);
        if(out.is_open())
        {
            LinkedTupleBuffer* cur = ptr->get_root();
            while(cur)
            {
                TupleBuffer::Iterator itr = cur->createIterator();
                void* tupleAddr = itr.next();
                while(tupleAddr)
                {
                    if(!cur->empty_tuple(tupleAddr))
                    {
                        char sep = 'a';
                        string line = ptr->schema()->pretty_print(tupleAddr,sep);
                        out << line.c_str() << endl;
                    }
                    tupleAddr = itr.next();
                }
                cur = cur->get_next();
            }
            isModify_[index] = false;
            return true;
        }
        else
        {
            cout << "ERROR: open file error when preserving" << (fileName+fileType).c_str() << endl;
            return false;
        }
    }
    return true;
}

int Manager::auto_preserve()
{
    int okCount = 0;
    for(unsigned int i=0; i<openTables_.size(); i++)
    {
        if(preserve(openTables_[i]))
        {
            okCount++;
        }
    }
    return okCount;
}

bool Manager::close(string tablName)
{
    WriteTable* x = isOpen(tablName);
    int index;
    for(unsigned int i=0;i<openTables_.size();i++)
    {
        if(x==openTables_[i])
        {
            index = i;
            break;
        }
    }

    if(!x)
    {
        cout << "The table " << tablName.c_str() << "is not opened." <<endl;
        return false;
    }
    else
    {
        if(preserve(x))
        {
            x->close();
            delete x;
            openTables_.erase(openTables_.begin()+index);
            isModify_.erase(isModify_.begin()+index);
            return true;
        }
        else
        {
            cout << "preserve fail and stop to close . " <<endl;
            return false;
        }
    }
}
