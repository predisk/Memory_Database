#include "table.h"
void Table::init(Schema *s)
{
    schema_ = s;
    ht_ = new HashTable;
}

/*Table::LoadErrorT Table::load(const string& filepattern,
                              const string& separators)
{
    Loader loader(separators[0]);
    loader.load(filepattern, *this);
    return LOAD_OK;
}*/

bool Table::insert(vector<CVpair> entry) {
    if(schema()->columnCounts()!=entry.size()) {
        cout << "insert error: the column count is different from schema." << endl;
        return false;
    }
    string id_str ="";
    for(unsigned int i=0; i<entry.size(); i++)
    {
        if(!(entry[i].first).compare("id"))
        {
            id_str = entry[i].second;
            break;
        }
    }
    if(!id_str.compare("")) {
        cout << "insert error: no col is named \'id\'" << endl;
        return false;
    }
    vector<string> input;
    for(unsigned int i=0; i<entry.size(); i++)
        input.push_back("");

    for(unsigned int i=0; i<entry.size(); i++)
    {
        int index = schema()->getColPos(entry[i].first);
        if(index<0)
        {
            cout << "insert error: can't find the col named "  << entry[i].first.c_str() <<endl;
            return false;
        }
        input[index] = entry[i].second;
    }
    unsigned int s = schema_->get_tuple_size();
    void *target = new char[s];
    schema_->parse_tuple(target, input);
    std::stringstream ss (id_str);
    long id_long = 0;
    ss >> id_long;

    ht_->table_rdlock();
    ht_->slot_wrlock(id_long);
    if(!ht_->insert(id_long,target)) //key is repeated
    {
        delete [] target;
        ht_->slot_unlock(id_long);
        ht_->table_unlock();
        return false;
    }
    is_modify_ = true;
    if(can_append_) {
    	pthread_rwlock_wrlock(&append_lock_);
    	append_list_.push_back(target);
    	pthread_rwlock_unlock(&append_lock_);
    }
    ht_->slot_unlock(id_long);
    ht_->table_unlock();
    return true;
}

bool Table::delete_tuple(vector<CVpair> clause)
{
    if((clause.size() == 1) &&
            (!clause[0].first.compare("id")))
    {
        std::stringstream ss(clause[0].second);
        long id_long;
        ss >> id_long;
        ht_->table_rdlock();
        ht_->slot_wrlock(id_long);
        if(ht_->delete_node(id_long))
        {
            is_modify_ = true;
            can_append_ = false;
            ht_->slot_unlock(id_long);
            ht_->table_unlock();
            return true;
        }
        ht_->slot_unlock(id_long);
        ht_->table_unlock();
        return false;
    }
    ht_->table_wrlock();
    vector<KVpair>query_result;
    query_result = query(clause);
    if(!query_result.size())
    {
        cout << "table delete error : no such tuple." <<endl;
        ht_->table_unlock();
        return false;
    }
    else
    {
        for(unsigned int i=0; i<query_result.size(); i++)
        {
            ht_->delete_node(query_result[i].first);
        }
        is_modify_ = true;
        can_append_ = false;
        ht_->table_unlock();
        return true;
    }
}

void Table::RangeQuery(double centerX,double centerY,double r)
{
    ht_->table_wrlock();
    vector<void*>tuples_addr;
    //tuples_addr = kd_tree_p->search(centerX,centerY,r);
    printTuples(tuples_addr);
    ht_->table_unlock();
}

bool Table::update(vector<CVpair> clause,vector<CVpair> newCV) {
    //split newCV
    Schema* s =schema();
    vector<string>value;
    vector<int>col_index;
    int id_index = -1;
    string id = "id";
    for(unsigned int i=0; i<newCV.size(); i++) {
        string colName = newCV[i].first;
        if(!id.compare(newCV[i].first))
            id_index = i;
        col_index.push_back(s->getColPos(colName));
        value.push_back(newCV[i].second);
    }
    long id_new = -1;
    if(id_index >= 0) {
        std::stringstream ss_new(value[id_index]);
        ss_new >> id_new;
    }
    if((clause.size() == 1) &&
            (!clause[0].first.compare("id")))
    {
        //search target tuple only by id, so just need to lock slot
        std::stringstream ss_old(clause[0].second);
        long id_old = -1;
        ss_old >> id_old;
        if(id_index>=0) {
            //search target tuple by id and need to update id

            //比较的应该是slot而不是单纯id！！！！！
            unsigned int slot_small = 0,slot_big = 0;
            unsigned int slot_old = ht_->hash_func(id_old);
            unsigned int slot_new = ht_->hash_func(id_new);
            if(slot_old == slot_new) {
                ht_->table_rdlock();
                ht_->slot_wrlock(id_old);
                if(ht_->update_key(id_old,id_new)) {
                    void* addr = search_tuple(id_new);
                    for(unsigned int i=0; i<value.size(); i++) 
                        parse_updated_data(addr,(char*)value[i].c_str(),col_index[i]);
                    is_modify_ = true;
                    can_append_ = false;
                    ht_->slot_wrlock(id_new);
                    ht_->table_unlock();
                    return true;
                } else {//id_old is nonexistent
                    ht_->slot_unlock(id_old);
                    ht_->table_unlock();
                    return false;
                }
            } else if(slot_old > slot_new) {
                slot_small = slot_new;
                slot_big = slot_old;
            } else {
                slot_small = slot_old;
                slot_big = slot_new;
            }
            ht_->table_rdlock();
            ht_->slot_wrlock(slot_small);
            ht_->slot_wrlock(slot_big);
            if(ht_->update_key(id_old,id_new)) {
                void* addr = search_tuple(id_new);
                for(unsigned int i=0; i<value.size(); i++)
                    parse_updated_data(addr,(char*)value[i].c_str(),col_index[i]);
                is_modify_=true;
                can_append_=false;
                ht_->slot_unlock(slot_big);
                ht_->slot_unlock(slot_small);
                ht_->table_unlock();
                return true;
            } else {    //new_id is repeated or old_id is nonexistent
                ht_->slot_unlock(slot_big);
                ht_->slot_unlock(slot_small);
                ht_->table_unlock();
                return false;
            }
        } else {
            // search tuple by id but not need to update id
            ht_->table_rdlock();
            ht_->slot_wrlock(id_old);
            void* addr = search_tuple(id_old);
            if(addr) {
                for(unsigned int i=0; i<value.size(); i++) {
                    parse_updated_data(addr,(char*)value[i].c_str(),col_index[i]);
                }
                is_modify_=true;
                can_append_=false;
                ht_->slot_unlock(id_old);
                ht_->table_unlock();
                return true;
            } else {
                cout << "Table update error : no such tuple" << endl;
                ht_->slot_unlock(id_old);
                ht_->table_unlock();
                return false;
            }
        }
    }
    //search tuple not only by id,so need to traverse and get table_wrlock
    vector<KVpair>query_result;
    ht_->table_wrlock();
    query_result = query(clause);
    if(!query_result.size()) {
        cout << "Table update error: qeury result is empty." <<endl;
        ht_->table_unlock();
        return false;
    }
    if(id_index>=0) {
        //need to update id
        if(query_result.size()>1) {
            cout << "Table update error: can't update the id of more than one tuple."<<endl;
            ht_->table_unlock();
            return false;
        }
        //so the count of query_result must be one
        long id_old = query_result[0].first;
        long id_small =-1, id_big = -1;
        if(id_old > id_new) {
            id_small = id_new;
            id_big = id_old;
        } else {
            id_small = id_old;
            id_big = id_new;
        }
        if(ht_->update_key(id_old,id_new)) {
            void* addr = query_result[0].second;
            for(unsigned int i=0; i<value.size(); i++) {
                parse_updated_data(addr,(char*)value[i].c_str(),col_index[i]);
            }
            is_modify_=true;
            can_append_=false;
            ht_->table_unlock();
            return true;
        } else { 
            //new_id is repeated or old_id is nonexistent
            ht_->table_unlock();
            return false;
        }
    } else {
        // no need to update id
        for(unsigned int i=0; i<query_result.size(); i++) {
            void* dest = query_result[i].second;
            for(unsigned int j=0; j<value.size(); j++)
                parse_updated_data(dest,(char*)(value[j].c_str()),col_index[j]);
        }
        is_modify_=true;
        can_append_=false;
        ht_->table_unlock();
        return true;
    }
}



bool Table::save(string path,string file_name,string file_type) {
    string full_name = path + file_name + file_type;
    ht_->table_wrlock();
    if(is_modify_) {
        ofstream out;
        if(can_append_) {
            out.open(full_name.c_str(),ios::out|ios::app);
            if(out.is_open()) {
                char sep = ' ';
                for(unsigned int i=0; i<append_list_.size(); i++) {
                    string line = schema()->pretty_print(append_list_[i],sep);
                    out << line.c_str() << endl;
                }
                out.close();
                can_append_ = true;
                is_modify_ = false;
                append_list_.clear();
                ht_->table_unlock();
                return true;
            } else { // file open error
                cout << "Table save error:open table file error." << endl;
                ht_->table_unlock();
                return false;
            }
        } else { //can't append into file because delete or update
            out.open(full_name.c_str(),ios::out|ios::ate);
            if(out.is_open()) {
                vector<KVpair>all_tuples = ht_->traverse();
                char sep = ' ';
                for(unsigned int i=0; i<all_tuples.size(); i++) {
                    string line = schema()->pretty_print(all_tuples[i].second,sep);
                    out << line.c_str() << endl;
                }
                out.close();
                can_append_ = true;
                is_modify_ = false;
                append_list_.clear();
                ht_->table_unlock();
                return true;
            } else { // open failed
                cout << "Table save error:open table file failed." << endl;
                ht_->table_unlock();
                return false;
            }
        }
    } else { //the table doesn't change
        ht_->table_unlock();
        return true;
    }
}

bool Table::close(string path,string file_name,string file_type) { 
    ht_->table_wrlock();
    string table_name = schema()->getTableName();
    if(save(path,file_name,file_type)) {
        ht_->close();
        ht_->table_unlock();
        return true;;
    } else {
        cout << "Table close error:be sure you can save." << endl;
        ht_->table_unlock();
        return false;
    }
}

void Table::printTuples()
{
    ht_->table_wrlock();
    vector<KVpair>all_tupes = ht_->traverse();
    for(unsigned int i=0; i<all_tupes.size(); i++)
    {
        char sep = ' ';
        string tmp = schema()->pretty_print(all_tupes[i].second,sep);
        cout << tmp.c_str() << endl;
    }
    ht_->table_unlock();
}

void Table::printTuples(vector<void*>input)
{
    char sep = ' ';
    for(unsigned int i=0; i<input.size(); i++)
    {
        string line = schema()->pretty_print(input[i],sep);
        cout << line.c_str() <<endl;
    }
}

void Table::parse_updated_data(void *dest, char *input, int col) {
    ColumnType type = schema_->get_column_type(col);
    switch(type) {
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

double Table::distance(double x,double y,
                       double centerX,double centerY)
{
    return sqrt(pow(x-centerX,2)+pow(y-centerY,2));
}

vector<KVpair> Table::query(vector<CVpair> clause) {
    vector<string>colName;
    vector<string>value;
    vector<int>colIndex;
    for(unsigned int i = 0; i < clause.size(); i++) {
        colName.push_back(clause[i].first);
        value.push_back(clause[i].second);
        colIndex.push_back(schema_->getColPos(colName[i]));
    }
    vector<KVpair>all_tuples = ht_->traverse();
    vector<KVpair>result;
    for(unsigned int i=0; i<all_tuples.size(); i++) {
        bool same = true;
        KVpair cur = all_tuples[i];
        vector<string>entry = schema_->output_tuple(cur.second);
        for(unsigned int i=0; i<colName.size(); i++)
        {
            if(entry[colIndex[i]].compare(value[i]))
            {
                same = false;
                break;
            }
        }
        if(same)
        {
            //int idIdex = schema_->getColPos("id");
            result.push_back(cur);
        }
    }
    return result;
}

void* Table::search_tuple(long id_long) {
    void* addr = ht_->get_addr(id_long);
    if(addr)
        return addr;
    return 0;
}

/*void Table::append(const char **data, unsigned int count) {
    unsigned int s = schema_->get_tuple_size();
    if(schema_->columnCounts() == count) {
        void *target = new char[s];
        schema_->parse_tuple(target, data);
        ht_->insert()
    } else {
        std::cout << "ERROR COUNT!" << endl;
    }
}*/