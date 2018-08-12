#include<sstream>

class Command
{
public:
    vector<CVpair> clauseToCVpair(string where);
    bool insertTuple(string cmd,string& tableName,vector<CVpair>& data);
    bool deleteTuple(string cmd,string& tableName,vector<CVpair>& clause);
    bool update(string cmd,string& tableName,vector<CVpair>& clause,vector<CVpair>& newCV);
    bool RangeQuery(string arg,double& x,double& y,double& r);
};

vector<CVpair> Command::clauseToCVpair(string where)
{
    vector<string>fields;
    vector<string>values;
    while(1)
    {
        int splitIndex = where.find(",");
        if(splitIndex<0)
        {
            fields.push_back(where.substr(0,where.find("=")));
            values.push_back(where.substr(where.find("=")+1,where.length()));
            break;
        }
        else
        {
            string item = where.substr(0,splitIndex);
            fields.push_back(item.substr(0,item.find("=")));
            values.push_back(item.substr(item.find("=")+1,item.length()));
            where = where.substr(splitIndex+1,where.length());
        }
    }
    vector<CVpair>clause;
    for(unsigned int i=0;i<fields.size();i++)
    {
        clause.push_back(make_pair(fields[i],values[i]));
    }
    return clause;
}

//INSERT into tablename (field1, field2,...,fieldn) VALUES (value1, value2, ..., valuen)
bool Command::insertTuple(string cmd,string& tableName,vector<CVpair>& data)
{
    transform(cmd.begin(),cmd.end(),cmd.begin(),::tolower);
    cmd = cmd.substr(cmd.find("into")+5,cmd.length());
    tableName = cmd.substr(0,cmd.find(" "));

    string fieldLine= cmd.substr(cmd.find("(")+1,cmd.find(")")-cmd.find("(")-1);

    vector<string>fields;
    while(1)
    {
        int splitIndex = fieldLine.find(",");
        if(splitIndex<0)
        {
            fields.push_back(fieldLine);
            break;
        }
        else
        {
            string item = fieldLine.substr(0,splitIndex);
            fields.push_back(item);
            fieldLine = fieldLine.substr(splitIndex+1,fieldLine.length());
        }
    }

    cmd = cmd.substr(cmd.find(")")+2,cmd.length());
    string valueLine = cmd.substr(cmd.find("(")+1,cmd.find(")")-cmd.find("(")-1);
    vector<string>values;
    while(1)
    {
        int splitIndex = valueLine.find(",");
        if(splitIndex<0)
        {
            values.push_back(valueLine);
            break;
        }
        else
        {
            string item = valueLine.substr(0,splitIndex);
            values.push_back(item);
            valueLine = valueLine.substr(splitIndex+1,valueLine.length());
        }
    }
    if(fields.size()!=values.size())
    {
        cout << "the count of field and value is not fitted.";
        return false;
    }

    for(unsigned int i=0;i<fields.size();i++)
    {
        data.push_back(make_pair(fields[i],values[i]));
    }
    return true;
}

//DELETE from tablename <where id=value,x=3>
bool Command::deleteTuple(string cmd,string& tableName,vector<CVpair>& clause)
{
    transform(cmd.begin(),cmd.end(),cmd.begin(),::tolower);
    cmd = cmd.substr(cmd.find("from")+5,cmd.length());
    tableName = cmd.substr(0,cmd.find(" "));
    string where = cmd.substr(cmd.find("where")+6,cmd.find(">")-cmd.find("where")-6);
    clause = clauseToCVpair(where);
    return true;
}

//UPDATE tablename SET field1=new_value1,field2=new_value2  <where id=value>
bool Command::update(string cmd,string& tableName,vector<CVpair>&clause,vector<CVpair>& newCV)
{
    transform(cmd.begin(),cmd.end(),cmd.begin(),::tolower);
    cmd = cmd.substr(cmd.find(" ")+1,cmd.length());
    tableName = cmd.substr(0,cmd.find(" "));
    string data = cmd.substr(cmd.find("set")+4,cmd.find("<")-cmd.find("set")-5);
    string where = cmd.substr(cmd.find("<")+1,cmd.find(">")-cmd.find("<")-1);
    clause = clauseToCVpair(where);
    newCV = clauseToCVpair(data);
    return true;
}

//x,y,r
bool Command::RangeQuery(string arg,double& x,double& y,double& r)
{
    string strX = arg.substr(0,arg.find(","));
    arg = arg.substr(arg.find(",")+1,arg.length());
    string strY = arg.substr(0,arg.find(","));
    arg = arg.substr(arg.find(","),arg.length());
    string strR = arg;
    istringstream istX(strX),istY(strY),istR(strR);
    istX >> x;
    istY >> y;
    istR >> r;
    return true;
}










