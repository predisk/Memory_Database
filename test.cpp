#include <fstream>
#include <assert.h>
#include <sstream>
#include <string>
#include "table.h"
#include <sys/time.h>
#include <ctime>
#include <iostream>
#include <semaphore.h>
#include <pthread.h>
#include <stdlib.h>
#include <stdio.h>
#include <unistd.h>
#include "schema.h"
#include "table.h"
#include "Manager.h"
#include "command.h"

using namespace std;

#define QUEUE_SIZE 10000

enum type
{
    INSERT,
    DELETE,
    UPDATE,
    ELSE,
    RANGE_QUERY,
    
};

typedef struct Msg
{
    enum type msg_type;
    int msg_len;
    int msg_src;
    int msg_dst;
}Msg_Hdr_t;

typedef struct msg_src{
    Msg_Hdr_t hdr;
    string data;
}Msg_t;

typedef struct Queue_s
{
    int head;
    int rear;
    sem_t sem;
    Msg_t data[QUEUE_SIZE];
}Queue_t;


enum SRC_ADDR
{
    THREAD1,
    THREAD2,
    THREAD3,
    HANDLER,
};

int MsgQueueInit(Queue_t *Q);
int MsgDeQueue(Queue_t *Q,Msg_t* msg);
int MsgEnQueue(Queue_t *Q,Msg_t *msg);
void* msg_handler(void* arg);
void *msg_sender1(void *arg);
void msg_printer(Msg_t* msg);
int msg_send();

Queue_t MsgQueue;
//WriteTable *tb;
Manager m;
Command cmd;
ofstream OutFile;
ofstream OutFile1;
ofstream OutFile2;
ofstream OutFile3;
unsigned long delete_num;
unsigned long insert_num;
unsigned long update_num;
enum type message_type;
unsigned long insert_total_time;
unsigned long update_total_time;
unsigned long delete_total_time;

int main(int argc,char* argv[])
{
    int ret;
    pthread_t thread1;
    pthread_t handler_thread;

    insert_num = 0;
    delete_num = 0;
    update_num = 0;
    insert_total_time = 0;
    delete_total_time = 0;
    update_total_time = 0;
    OutFile1.open("insert_result.txt");
    OutFile2.open("update_result.txt");
    OutFile3.open("delete_result.txt");
    OutFile.open("save_result.txt");
    /*
    m.init(40);
    m.create("TABLE taxi(id INT,x INT,y INT");
    m.loadTuple("taxi","a.txt"," ");
    */
    
    message_type = INSERT;
    m.init(100);
    m.create("taxi (id int,x int,y int)");
    //m.loadTuple("taxi","a.txt"," ");
    ret = MsgQueueInit((Queue_t *)&MsgQueue);
    if(ret != 0)
        return -1;
    if(pthread_create(&handler_thread,NULL,msg_handler,NULL)){
        printf("create handler thread fail!\n");
        return -1;
    }

    if(pthread_create(&thread1,NULL,msg_sender1,NULL)){
        printf("create thread1 thread fail!\n");
        return -1;
    }

    /*
    if(pthread_create(&thread2,NULL,(void*)msg_sender2,NULL)){
        printf("create thread2 thread fail!\n");
        return -1;
    }

    if(pthread_create(&thread3,NULL,(void*)msg_sender3,NULL)){
        printf("create thread3 thread fail!\n");
        return -1;s
    }
    */
    while(1)
        sleep(1);
    return 0;
}

int MsgQueueInit(Queue_t* Q)
{
    if(!Q){
        cout<<"Invalid Queue!"<<endl;
        return -1;
    }
    Q->rear = 0;
    Q->head = 0;
    sem_init(&Q->sem,0,1);
    return 0;
}

//get a message from the request queue
int MsgDeQueue(Queue_t* Q,Msg_t* msg)
{
    if(!Q){
        cout<<"Invalid Queue!"<<endl;
        return -1;
    }
    if(Q->rear == Q->head){
        cout<<"Queue Empty"<<endl;
        return -1;
    }
    memcpy(msg,&(Q->data[Q->head]),sizeof(Msg_t));
    cout << "MsgDeQueue the " << Q->head << "message" << endl;
    cout << "message data is " << msg->data << endl;
    //printf("message data is %s\n", msg.data);
    Q->head = (Q->head+1)%QUEUE_SIZE;
    return 0;
}

//insert a message into request queue
int MsgEnQueue(Queue_t *Q,Msg_t* msg)
{
    if(Q->head == (Q->rear+1)%QUEUE_SIZE){
        cout<<"full Queue!"<<endl;
        return -1;
    }
    sem_wait(&Q->sem);
    memcpy(&(Q->data[Q->rear]),msg,sizeof(Msg_t));
    Q->rear = (Q->rear +1)%QUEUE_SIZE;
    cout << "Q->rear = " << Q->rear << endl;
    sem_post(&Q->sem);
    return 0;
}

void msg_printer(Msg_t* msg)
{
    if(!msg) return;
    char *temp[] = { "INSERT", "DELETE" ,"UPDATE","ELSE"};
    //sprintf("%s:I have recieved a message\n", __FUNCTION__);
    printf("%s: msgtype:%s   msg_src:%d  dst:%d\n\n",__FUNCTION__,temp[msg->hdr.msg_type],msg->hdr.msg_src,msg->hdr.msg_dst);  
}

int handler(Msg_t msg)
{
    enum type input_type = msg.hdr.msg_type;
    string input = msg.data;
    //cout << "in handler string input is " << input << endl;
    string tableName;
    struct timeval start, end;
    long unsigned diff;
    int flag = 0;

    gettimeofday(&start, NULL);

    transform(input.begin(),input.end(),input.begin(),::tolower);
    string head = input.substr(0,input.find(' '));
    string remain = input.substr(input.find(' ')+1,input.length());

    if(!head.compare("save")){
        m.save("taxi");
    }
    //LOAD tableName path=C:\\Document\\Projects\\CPPtest sep=' '
    else if(!head.compare("load"))
    {

        string tableName = remain.substr(0,remain.find(' '));
        string path = remain.substr(remain.find("path")+5,remain.find("sep")-remain.find("path")-6);
        string sep = remain.substr(remain.find("sep")+5,remain.length()-remain.find("sep")-6);
        m.loadTuple(tableName,path,sep);
    }

    //INSERT into tablename (field1,field2,fieldn) VALUES (value1,value2,valuen)
    else if(!head.compare("insert"))
    {
        vector<CVpair> data;
        cmd.insertTuple(input,tableName,data);
        m.insertTuple(tableName,data);
    }

    //UPDATE tablename SET field1=new_value1,field2=new_value2  <where id=value,x=3>
    else if(!head.compare("update"))
    {
        vector<CVpair>clause;
        vector<CVpair>data;
        cmd.update(input,tableName,clause,data);
        m.updateTuple(tableName,clause,data);
    }

    //DELETE from tablename <where id=value,x=4>
    else if(!head.compare("delete"))
    {
        if((int)remain.find("where") >=0)
        {
            vector<CVpair>clause;
            cmd.deleteTuple(input,tableName,clause);
            m.deleteTuple(tableName,clause);
        }
        else
        {
            cout << "If you decide to delete the whole table please input YES" << endl;
            string sure;
            getline(cin,sure);
            transform(sure.begin(),sure.end(),sure.begin(),::tolower);
            if(!sure.compare("yes"))
            {
                string tableName = remain.substr(remain.find("from")+5,remain.find("<")-remain.find("from")-6);
                m.deleteTable(tableName);
            }
        }
    }

    //RANGEQUERY tableName (x,y,r)
    else if(!head.compare("rangequery"))
    {
        string tableName = remain.substr(0,remain.find(' '));
        string arg = remain.substr(remain.find('(')+1,remain.find(')')-remain.find('(')-1);
        double x,y,r;

        string strX = arg.substr(0,arg.find(","));
        arg = arg.substr(arg.find(",")+1,arg.length());
        string strY = arg.substr(0,arg.find(","));
        arg = arg.substr(arg.find(",")+1,arg.length());
        string strR = arg;
        istringstream istX(strX),istY(strY),istR(strR);
        istX >> x;
        istY >> y;
        istR >> r;
        m.rangeQuery(tableName,x,y,r);
    }

    //CLOSE tableName
    else if(!head.compare("close"))  //ok
    {
        string tableName = remain;
        m.close(tableName);
    }
    else if(!head.compare("exit"))  //ok
    {
        m.auto_preserve();
        exit(0);
    }
    else
    {
        cout << "Invalid command" <<endl;
    }

    gettimeofday(&end, NULL);

    char *msgtype[] = {"INSERT", "DELETE", "UPDATE", "RANGE_QUERY", "ELSE"};
    diff = (end.tv_sec - start.tv_sec) * 1000000 + (end.tv_usec - start.tv_usec);
    cout << msgtype[msg.hdr.msg_type] << " use time " << diff << "microsecond" << endl;
    switch(input_type)
    {
        case INSERT:
            insert_num++;
            insert_total_time += diff;
            if(insert_total_time <=70000000) OutFile1 << diff << endl;
            if(insert_total_time >= 60000000 && insert_total_time <=60040000){
                flag = 1;
                OutFile1 << "one minute insert " << insert_num << endl;
                //OutFile1.close();
            }
            break;

        case UPDATE:
            update_num++;
            update_total_time +=diff;
            if(update_total_time <=70000000) OutFile2 << diff << endl;
            if(update_total_time >=60000000 && update_total_time <=60200000){
                flag = 2;
                OutFile2<<"one minute update "<<update_num<<endl;
                //OutFile2.close();
            }
            break;

        case DELETE:
            delete_num++;
            delete_total_time += diff;
            if(delete_total_time <=70000000) OutFile3 <<diff<<endl;
            if(delete_total_time >=60000000 && delete_total_time <=60200000){
                flag = 2;
                OutFile3<<"one minute delete "<<delete_num<<endl;
                OutFile3.close();
            }
            break;
        default:
            OutFile <<diff<<endl;
            cout << "invalid type" << endl;
    }

    /*
    switch(input_type)
    {

        case INSERT:
            gettimeofday(&start,NULL);
            cmd.insertTuple(input,tableName,input_data);
            m.insertTuple(tableName,input_data);
            gettimeofday(&end, NULL);
            diff = (end.tv_sec - start.tv_sec) * 1000000 + (end.tv_usec - start.tv_usec);
            cout << "insert " << diff << "microsecond" << endl;
            total_time += diff;
            insert_num++;
            if(total_time >= 60000000 && total_time <=60000800){
                flag = 1;
                OutFile << "one minutes insert " << insert_num << endl;
            }
            break;
        
        case DELETE:
            gettimeofday(&start, NULL);
            cmd.deleteTuple(input,tableName,input_data);
            m.deleteTuple(tableName, input_data);
            gettimeofday(&end, NULL);
            diff = (end.tv_sec - start.tv_sec) * 1000000 + (end.tv_usec - start.tv_usec);
            cout<<"delete "<<diff <<endl;
            delete_total_time += diff;
            if(delete_total_time >=60000000 && delete_total_time <= 60000800 ){
                flag = 0;
                OutFile << "one minutes delete " << delete_total_time << endl;
            }
            break;

        case UPDATE:
            gettimeofday(&start, NULL);
            cmd.update(input,tableName,clause,input_data);
            m.updateTuple(tableName, clause, input_data);
            gettimeofday(&end,NULL);
            diff = (end.tv_sec - start.tv_sec) * 1000000 + (end.tv_usec - start.tv_usec);
            OutFile<<diff <<endl;
            update_total_time += diff;
            update_num++;
            if(update_total_time >=60000000 && update_total_time <= 60000800 ){
                flag = 2;
                OutFile << "one minutes update " << update_num << endl;
            }
            break;
        default:

            cout << "error: message type not exist" << endl;
    }
    */

    return flag;
}

void* msg_handler(void* arg)
{
    //sleep(5);
    while(1)
    {
        Msg_t msg;
        memset(&msg, 0 ,sizeof(Msg_t));
        int res = MsgDeQueue((Queue_t*)&MsgQueue,&msg);
        if(res != 0){
            sleep(1);
            continue;
        }
        int result=handler(msg);
        msg_printer(&msg);
        if(result == 1){
            message_type = UPDATE;
        }
        if(result == 2){
            message_type = DELETE;
        }
        if (result == 3){
            message_type = INSERT;
        }
        //sleep(1);
    }
    return NULL;
}


void* msg_sender1(void* arg)
{
    cout<<"this is msg sender"<<endl;
    Msg_t msg;
    srand(time(NULL));
    int id=0;
    int update_id = 0;
    int delete_id = 0;
    while(1){
        memset(&msg, 0, sizeof(Msg_t));
        msg.hdr.msg_type = message_type;
        msg.hdr.msg_src = THREAD1;
        msg.hdr.msg_dst = HANDLER;
        ostringstream os;
        string input_data;
        char temp[60];
        if(message_type == INSERT){
            input_data = "insert into taxi (id,x,y) VALUES (";
            os<<id;
            id++;
            input_data += os.str();
            os.str("");
            input_data = input_data + ',';
            os<<rand()%5000;
            input_data+=os.str();
            os.str("");
            input_data = input_data + ',';
            os<<rand()%5000;
            input_data+= os.str();
            input_data += ")";
            os.str("");
            //input_data.copy(temp, input_data.length(), 0);
            cout << "input_data is " << input_data << endl;
            //input_data = input_data.c_str();
            
            strcpy(temp, input_data.data());
            //cout << "temp is " << temp << endl;
        }
        else if(message_type == UPDATE){
            input_data = "update taxi set id=";
            os << update_id;
            update_id++;
            input_data += os.str();
            os.str("");
            input_data += ",x=";
            os << rand() % 5000;
            input_data += os.str();
            input_data+="<where id=";
            os.str("");
            os << rand() % id;
            input_data += os.str();
            input_data += ">";
            os.str("");
            cout << "input_data is " << input_data << endl;
            strcpy(temp, input_data.data());
        }
        //delete from taxi <where id=3>
        else if(message_type == DELETE){
            input_data = "delete from taxi <where id=";
            os << delete_id;
            delete_id++;
            input_data+= os.str();
            os.str("");
            input_data += ">";
            cout<<"input_data is "<<input_data;
            strcpy(temp, input_data.data());
        }

        msg.data = temp;
        cout << "msg.data is " << msg.data << endl;
        int is_full=MsgEnQueue((Queue_t *)&MsgQueue, &msg);
        if(is_full == -1)
            sleep(1);

        int rand_save = rand();
        if(rand_save %100 == 1){
            memset(&msg, 0, sizeof(Msg_t));
            msg.hdr.msg_type = ELSE;
            msg.hdr.msg_src = THREAD1;
            msg.hdr.msg_dst = HANDLER;
            input_data = "save";
            memset(temp, 0, sizeof(temp));
            strcpy(temp, input_data.data());
            msg.data = temp;
            int is_full = MsgEnQueue((Queue_t *)&MsgQueue, &msg);
            if(is_full == -1)
                sleep(1);
        }
        printf("%s:Thread 1 send a message\n", __FUNCTION__);
    }
    return NULL;
}



