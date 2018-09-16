#include"Manager.h"

bool Manager::init(int bufferSize,string path)
{
    bufferSize_ = bufferSize;
    path_ = path;
    return updateExistTable();
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
    return 0;
}

WriteTable* Manager::isOpen(string tableName)
{
    pthread_rwlock_rdlock(&openLock_);
    for(unsigned int i=0; i<openTables_.size(); i++)
    {
        WriteTable* table = openTables_[i];
        Schema* s = table->schema();
        if(!(s->getTableName()).compare(tableName))
        {
            pthread_rwlock_unlock(&openLock_);
            return openTables_[i];
        }
    }
    pthread_rwlock_unlock(&openLock_);
    return 0;
}

bool Manager::isExist(string tableName)
{
    pthread_rwlock_rdlock(&existLock_);
    for(unsigned int i=0; i<existTables_.size(); i++)
    {
        if(!tableName.compare(existTables_[i]))
        {
            pthread_rwlock_unlock(&existLock_);
            return true;
        }
    }
    pthread_rwlock_unlock(&existLock_);
    return false;
}

Schema* Manager::loadSchema(string tableName)
{
    string fileName = "metadata";
    string full = path_+fileName+fileType_;
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
                in.close();
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

    string full = path_+tableName+fileType_;
    Schema* s =loadSchema(tableName);
    WriteTable* ret = new WriteTable;
    if(ret==0)
    {
        cout << "ERROR: memory allocate for table fail." << endl;
        return 0;
    }
    ret->init(s,bufferSize_);

    pthread_rwlock_wrlock(&openLock_);
    openTables_.push_back(ret);


    ifstream in;
    in.open(full.c_str());
    if(in.is_open())
    {
        ret->load(full," ");
        in.close();
    }
    pthread_rwlock_unlock(&openLock_);
    return ret;
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
    if(ptr->isModify())
    {
        string tableName = (ptr->schema())->getTableName();
        string fileName = tableName;
        string full = path_+fileName+fileType_;
        ofstream out;
        out.open(full.c_str(),ios::out|ios::ate);
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
                        char sep = ' ';
                        string line = ptr->schema()->pretty_print(tupleAddr,sep);
                        out << line.c_str() << endl;
                    }
                    tupleAddr = itr.next();
                }
                cur = cur->get_next();
            }
            ptr->setUnmodify();
            out.close();
            return true;
        }
        else
        {
            cout << "ERROR: open file error when preserving" << (fileName+fileType_).c_str() << endl;
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

bool Manager::updateExistTable()
{
    string fileName = "metadata";
    string full = path_+fileName+fileType_;
    ifstream in;
    in.open(full.c_str());
    if(in.is_open())
    {
        string line;
        pthread_rwlock_wrlock(&existLock_);
        while(getline(in,line))
        {
            string tableName = line.substr(0,line.find(" "));
            existTables_.push_back(tableName);
        }
        pthread_rwlock_unlock(&existLock_);
        in.close();
        return true;
    }
    else
    {
        cout << "Warming: matadata file doesn't exist or can't open." << endl;
        return false;
    }
}

bool Manager::saveSchema(string schema)
{
    ofstream f;
    string fileName = "metadata";
    string full = path_+fileName+fileType_;
    f.open(full.c_str(),ios::app);
    if(f.is_open())
    {
        f << schema.c_str() <<endl;
        f.close();
        return true;
    }
    else
        return false;
}

Schema* Manager::creatSchema(string input)
{
    string tableName = input.substr(0,input.find(' '));
    if(isExist(tableName))
    {
        cout << "The table name is repeated."<<endl;
        return 0;
    }

    Schema* s = new Schema;
    if(!s)
    {
        cout << "new schema error." << endl;
        return 0;
    }
    if(!s->create(input))
    {
        delete s;
        return 0;
    }
    if(!saveSchema(input))
    {
        cout << "schema preserve error." << endl;
        delete s;
        return 0;
    }
    return s;
}


WriteTable* Manager::creatTable(Schema* s)
{
    
    
    WriteTable* w = new WriteTable;
    if(!w)
    {
        cout << "new table error." << endl;
        return 0;
    }
    else
        w->init(s,bufferSize_);

    pthread_rwlock_wrlock(&existLock_);
    existTables_.push_back(s->getTableName());
    pthread_rwlock_unlock(&existLock_);

    pthread_rwlock_wrlock(&openLock_);
    openTables_.push_back(w);
    pthread_rwlock_unlock(&openLock_);
    return w;
}

//------------------operator function---------------------
bool Manager::create(string input)
{
    Schema* s = creatSchema(input);
    if(!s)
        return false;
    WriteTable* w = creatTable(s);
    if(!w)
        return false;
    return true;
}

bool Manager::loadTuple(string tableName,string path,string sep)
{
    WriteTable* w = getTable(tableName);
    if(!w)
        return false;
    if(w->get_root()!=0 && w->get_root()->getTupleCount()!=0)
    {
        cout << "load error: must load into an empty table."<<endl;
        return false;
    }
    w->load(path,sep);
    w->setModify();
    return true;
}

bool Manager::insertTuple(string tableName,vector<CVpair>& data)
{
    WriteTable* w = getTable(tableName);
    if(!w)
        return false;
    if(w->insert(data))
    {
        w->setModify();
        return true;
    }
    return false;
}

bool Manager::updateTuple(string tableName,vector<CVpair>& clause,vector<CVpair>& data)
{
    WriteTable* w = getTable(tableName);
    if(!w)
    {
        cout << "update error: table isn't existed." << endl;
        return false;
    }
    if(w->update(clause,data))
    {
        w->setModify();
        return true;
    }
    return false;
}

bool Manager::deleteTuple(string tableName,vector<CVpair>& clause)
{
    WriteTable* w= getTable(tableName);
    if(!w)
        return false;
    if(w->deleteTuple(clause))
    {
        w->setModify();
        return true;
    }
    return false;
}

bool Manager::deleteTable(string tableName) // wair for locking
{
    if(!isExist(tableName))
    {
        cout << "the table doesn't exist." <<endl;
        return false;
    }

    for(unsigned int i=0; i<openTables_.size(); i++)
    {
        string tn = openTables_[i]->schema()->getTableName();
        if(!tn.compare(tableName))
            close(tableName);
    }

    ifstream in;
    ofstream out;
    string inFile = path_+string("metadata")+fileType_;
    string outFile = path_+string("tmp")+fileType_;
    in.open(inFile.c_str());
    out.open(outFile.c_str());
    if(!in.is_open())
    {
        cout << "open metadata error when deleting." << endl;
        return false;
    }
    if(!out.is_open())
    {
        cout << "create new file error."<<endl;
        return false;
    }

    string line;
    while(getline(in,line))
    {
        string tmpTableName = line.substr(0,line.find(' '));
        if(tmpTableName.compare(tableName))
        {
            out << line.c_str() << endl;
        }
    }
    in.close();
    out.close();
    remove((tableName+fileType_).c_str());
    remove(inFile.c_str());
    rename(outFile.c_str(),inFile.c_str());
    for(unsigned int i=0; i<existTables_.size(); i++)
    {
        if(!existTables_[i].compare(tableName))
        {
            existTables_.erase(existTables_.begin()+i);
            break;
        }
    }
    return true;
}

bool Manager::rangeQuery(string tableName,double x,double y,double r)
{
    WriteTable* w = getTable(tableName);
    if(!w)
        return false;
    vector<void*>tuples = w->RangeQuery(x,y,r);
    if(!tuples.size())
    {
        cout << "no tuple in the range." << endl;
    }
    else
    {
        w->printTuples(tuples);
    }
    return true;
}

bool Manager::close(string tableName)  // wait for locking
{
    WriteTable* x = isOpen(tableName);
    int index;
    pthread_rwlock_rdlock(&openLock_);
    for(unsigned int i=0; i<openTables_.size(); i++)
    {
        if(x==openTables_[i])
        {
            index = i;
            break;
        }
    }
    pthread_rwlock_unlock(&openLock_);
    if(!x)
        cout << "Close error : The table " << tableName.c_str() << " is not opened." <<endl;
    else
    {
        
        if(preserve(x))
        {
            pthread_rwlock_wrlock(&openLock_);
            x->close();
            delete x;
            openTables_.erase(openTables_.begin()+index);
            pthread_rwlock_unlock(&openLock_);
        }
        
        else
            cout << "preserve fail and stop to close . " <<endl;
    }
    return true;
}

