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
            isModify_[index] = false;
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

    openTables_.push_back(w);
    isModify_.push_back(false);
    existTables_.push_back(s->getTableName());
    return w;
}

void Manager::modify(WriteTable* w)
{
    for(unsigned int i=0; i<openTables_.size(); i++)
    {
        if(w==(openTables_[i]))
            isModify_[i] = true;
    }
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
    WriteTable* w =getTable(tableName);
    if(!w)
        return false;
    else
    {
        w->load(path,sep);
        modify(w);
        return true;
    }
}

bool Manager::insertTuple(string tableName,vector<CVpair>& data)
{
    WriteTable* w = getTable(tableName);
    if(!w)
        return false;
    else
    {
        if(w->insert(data))
        {
            modify(w);
            return true;
        }
        else
            return false;
    }
}

bool Manager::updateTuple(string tableName,vector<CVpair>& clause,vector<CVpair>& data)
{
    WriteTable* w = getTable(tableName);
    if(!w)
        return false;
    else
    {
        if(w->update(clause,data))
        {
            modify(w);
            return true;
        }
        else
            return false;
    }
}

bool Manager::deleteTuple(string tableName,vector<CVpair>& clause)
{
    WriteTable* w= getTable(tableName);
    if(!w)
        return false;
    else
    {
        if(w->deleteTuple(clause))
        {
            modify(w);
            return true;
        }
        else
            return false;
    }
}

bool Manager::deleteTable(string tableName)
{
    if(!isExist(tableName))
    {
        cout << "the table doesn't exist." <<endl;
        return false;
    }
    else
    {
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
    }
}

bool Manager::rangeQuery(string tableName,double x,double y,double r)
{
    WriteTable* w = getTable(tableName);
    if(!w)
        return false;
    else
    {
        vector<void*>tuples = w->RangeQuery(x,y,r);
        if(tuples.size())
        {
            cout << "no tuple in the range." << endl;
        }
        else
        {
            w->printTupless(tuples);
        }
        return true;
    }
}

bool Manager::close(string tableName)
{
    WriteTable* x = isOpen(tableName);
    int index;
    for(unsigned int i=0; i<openTables_.size(); i++)
    {
        if(x==openTables_[i])
        {
            index = i;
            break;
        }
    }

    if(!x)
        cout << "The table " << tableName.c_str() << "is not opened." <<endl;
    else
    {
        if(preserve(x))
        {
            x->close();
            delete x;
            openTables_.erase(openTables_.begin()+index);
            isModify_.erase(isModify_.begin()+index);
        }
        else
            cout << "preserve fail and stop to close . " <<endl;
    }
}

