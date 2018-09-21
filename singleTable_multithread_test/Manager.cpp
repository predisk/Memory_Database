
#include"Manager.h"

bool Manager::init(string path)
{
    path_ = path;
    return updateExistTable();
}

Table* Manager::getTable(string tableName)
{
    if(isExist(tableName))
    {
        Table* ret = isOpen(tableName);
        if(!ret)
            ret = loadTable(tableName);
        return ret;
    }
    return 0;
}

Table* Manager::isOpen(string tableName)
{
    pthread_rwlock_rdlock(&openLock_);
    for(unsigned int i=0; i<openTables_.size(); i++)
    {
        Table* table = openTables_[i];
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

Table* Manager::loadTable(string tableName)
{

    string full = path_+tableName+fileType_;
    Schema* s =loadSchema(tableName);
    Table* ret = new Table;
    if(ret==0)
    {
        cout << "ERROR: memory allocate for table fail." << endl;
        return 0;
    }
    ret->init(s);

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

//lock?
bool Manager::preserve(Table* ptr) {
	return ptr->save(path_,ptr->schema()->getTableName,fileType_);
}

//lock?
int Manager::auto_preserve() {
	int OK_cout;
	for(unsigned int i = 0; i < openTables_.size(); i++ ) {
		Table* tp = openTables_[i];
		tp->save(path_,tp->schema()->getTableName,fileType_);
	}
	return OK_cout;
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


Table* Manager::creatTable(Schema* s) {
    Table* w = new Table;
    if(!w)
    {
        cout << "new table error." << endl;
        return 0;
    }
    else
        w->init(s);

    pthread_rwlock_wrlock(&existLock_);
    existTables_.push_back(s->getTableName());
    pthread_rwlock_unlock(&existLock_);

    pthread_rwlock_wrlock(&openLock_);
    openTables_.push_back(w);
    pthread_rwlock_unlock(&openLock_);
    return w;
}

//------------------operator function---------------------
bool Manager::create(string input) {
    Schema* s = creatSchema(input);
    if(!s)
        return false;
    Table* w = creatTable(s);
    if(!w)
        return false;
    return true;
}

/*bool Manager::loadTuple(string tableName,string path,string sep) {
    Table* w = getTable(tableName);
    if(!w)
        return false;
    if(w->get_root()!=0 && w->get_root()->getTupleCount()!=0)
    {
        cout << "load error: must load into an empty table."<<endl;
        return false;
    }
    w->load(path,sep);
    return true;
}*/

bool Manager::insertTuple(string tableName,vector<CVpair>& data) {
    Table* w = getTable(tableName);
    if(!w)
        return false;
    return w->insert(data);

}

bool Manager::updateTuple(string tableName,vector<CVpair>& clause,
	    vector<CVpair>& data) {
    Table* w = getTable(tableName);
    if(!w) {
        cout << "update error: table isn't existed." << endl;
        return false;
    }
    return w->update(clause,data);
}

bool Manager::deleteTuple(string tableName,vector<CVpair>& clause) {
    Table* w= getTable(tableName);
    if(!w)
        return false;
    return w->delete_tuple(clause);
}

//lock?
bool Manager::deleteTable(string tableName) {
    if(!isExist(tableName)) {
        cout << "the table doesn't exist." <<endl;
        return false;
    }

    for(unsigned int i=0; i<openTables_.size(); i++) {
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
    if(!in.is_open()) {
        cout << "open metadata error when deleting." << endl;
        return false;
    }
    if(!out.is_open()) {
        cout << "create new file error."<<endl;
        return false;
    }

    string line;
    while(getline(in,line)) {
        string tmpTableName = line.substr(0,line.find(' '));
        if(tmpTableName.compare(tableName)) {
            out << line.c_str() << endl;
        }
    }
    in.close();
    out.close();
    remove((tableName+fileType_).c_str());
    remove(inFile.c_str());
    rename(outFile.c_str(),inFile.c_str());
    for(unsigned int i=0; i<existTables_.size(); i++) {
        if(!existTables_[i].compare(tableName)) {
            existTables_.erase(existTables_.begin()+i);
            break;
        }
    }
    return true;
}

bool Manager::rangeQuery(string tableName,double x,double y,double r) {
    Table* w = getTable(tableName);
    if(!w)
        return false;
    w->RangeQuery(x,y,r);
    return true;
}

//lock?
/*bool Manager::close(string tableName)
{
    Table* x = isOpen(tableName);
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
}*/

