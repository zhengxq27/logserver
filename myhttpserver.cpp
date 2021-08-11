#include <iostream>
#include <string>
#include <cstring>
#include <event2/event.h>
#include <event2/bufferevent.h>
#include <event2/http.h>
#include <event2/http_struct.h>
#include <event2/http_compat.h>
#include <event2/util.h>
#include <event2/keyvalq_struct.h>
#include <event.h>
#include <thread>
#include <boost/lockfree/spsc_queue.hpp>
#include <boost/atomic.hpp>
#include <unistd.h>
#include <fcntl.h>
#include <sys/stat.h>

#include "storage.h"
using namespace std;

#define THREADSNUM 20

// to be changed.
boost::lockfree::spsc_queue<string> ring_buffer1{1000000};
boost::lockfree::spsc_queue<string> ring_buffer2{1000000};

// 全局类对象在首次被使用的时候初始化
Storage myStorage;

int count1;
int count2;

void consumer1()
{
    while( true )
    {
        while( !ring_buffer1.read_available() )
        {
            count1++;
            if( count1 == 10 )
            {
                usleep(5);
                count1 = 0;
            }
        }
        if( myStorage.ring_buffer1_can_access )
        {
            vector<string>logs;
            string log = ring_buffer1.front();
            logs.push_back( log );
            ring_buffer1.pop();
            myStorage.writeLogs1(logs);
        }
        else if( !myStorage.ring_buffer1_can_access )
        {
            cout << "ring buffer is full." << endl;
            vector<string> logs;
            while( ring_buffer1.read_available() )
            {
                string log = ring_buffer1.front();
                logs.push_back(log);
                ring_buffer1.pop();
            }
            // 先设置ring_buffer1_can_access为true再调用writechunk函数
            myStorage.ring_buffer1_can_access = true;
            myStorage.writeLogs1(logs);
        }
    }
}

void consumer2()
{
    while( true )
    {
        while( !ring_buffer2.read_available() )
        {
            count2++;
            if( count2 == 10 )
            {
                usleep(5);
                count2 = 0;
            }
        }
        if( myStorage.ring_buffer1_can_access ) // ring_buffer1可以访问，此时新数据不会再添加到ring_buffer2中
        {
            vector<string> logs;
            while( ring_buffer2.read_available() )
            {
                string log = ring_buffer2.front();
                logs.push_back(log);
                ring_buffer2.pop();
            }
            myStorage.writeLogs2(logs);
        }
        else
        {
            vector<string> logs;
            string log = ring_buffer2.front();
            logs.push_back(log);
            ring_buffer2.pop();
            myStorage.writeLogs2(logs);
        }
    }
}

void answerTimepointQuery(string stime, struct evhttp_request* req)
{
    vector<string> res = myStorage.queryByTime(stime);
    struct evbuffer* buf = evbuffer_new();
    if( !buf )
    {
        cout << "fails to create reponse buffer.\n";
        exit(0);
    }
    evhttp_add_header(req->output_headers, "Access-Control-Allow-Origin", "*");
    string res_str = "";
    for(int i = 0; i < res.size(); i++)
    {
       res_str += res[i];
       res_str += "\n";
    }   
    evbuffer_add_printf(buf, res_str.c_str());
    evhttp_send_reply(req, HTTP_OK,"OK", buf);
    evbuffer_free(buf);
}

void answerTimeRangeQuery(string stimeRange, struct evhttp_request* req, int page)
{
    vector<string> res = myStorage.queryByTimeRange(stimeRange, page);
    struct evbuffer* buf = evbuffer_new();
    if( !buf )
    {
        cout << "fails to create reponse buffer.\n";
        exit(0);
    }
    evhttp_add_header(req->output_headers, "Access-Control-Allow-Origin", "*");
    string res_str = "";
    for(int i = 0; i < res.size(); i++)
    {
       res_str += res[i];
    }   
    evbuffer_add_printf(buf, res_str.c_str());
    evhttp_send_reply(req, HTTP_OK,"OK", buf);
    evbuffer_free(buf);
}

void answerNumQuery(int num, struct evhttp_request* req)
{
    vector<string> res = myStorage.queryRecentN(num);
    struct evbuffer* buf = evbuffer_new();
    if( !buf )
    {
        cout << "fails to create reponse buffer.\n";
        exit(0);
    }
    evhttp_add_header(req->output_headers, "Access-Control-Allow-Origin", "*");
    string res_str = "";
    for(int i = 0; i < res.size(); i++)
    {
       res_str += res[i];
       res_str += "\n";
    }   
    evbuffer_add_printf(buf, res_str.c_str());
    evhttp_send_reply(req, HTTP_OK,"OK", buf);
    evbuffer_free(buf);
}

void answerGlobalQuery(string word, struct evhttp_request* req)
{
    vector<string> res = myStorage.globalSearch(word);
    struct evbuffer* buf = evbuffer_new();
    if( !buf )
    {
        cout << "fails to create reponse buffer.\n";
        exit(0);
    }
    evhttp_add_header(req->output_headers, "Access-Control-Allow-Origin", "*");
    string res_str = "";
    for(int i = 0; i < res.size(); i++)
    {
       res_str += res[i];
       res_str += "\n";
    }   
    evbuffer_add_printf(buf, res_str.c_str());
    evhttp_send_reply(req, HTTP_OK,"OK", buf);
    evbuffer_free(buf);
}

void generic_handler(struct evhttp_request* req, void* arg) // producer
{
    if( evhttp_request_get_command(req) == EVHTTP_REQ_GET )
    {
        const char* uri = evhttp_request_uri(req); // 修改了这里
        struct evhttp_uri* decoded = evhttp_uri_parse(uri);
        if( !decoded ){
            printf("It's not a good URI. Sending BADREQUEST\n");
            evhttp_send_error(req, HTTP_BADREQUEST, 0);
        }
        struct evkeyvalq params;
        char* decode_uri = evhttp_decode_uri(uri);
        evhttp_parse_query(decode_uri, &params);
        const char* ctime = evhttp_find_header(&params,"time");
        const char* cnum = evhttp_find_header(&params, "num");
        const char* cword = evhttp_find_header(&params, "word");
        const char* cpage = evhttp_find_header(&params, "page");
        const char* ctimeRange = evhttp_find_header(&params, "timeRange");

        if( ctime != nullptr )
        {
            string stime = ctime;
            thread Rthread(answerTimepointQuery,stime,req);
            Rthread.detach();
        }
        else if (ctimeRange != nullptr )
        {
            string stimeRange = ctimeRange;
            int page = 1; // 默认传值1
            if( cpage != nullptr )
            {
                string spage = cpage;
                page = stoi( spage );
            }
            thread Rthread(answerTimeRangeQuery,stimeRange,req,page);
            Rthread.detach();
        }
        else if( cnum != nullptr )
        {
            string snum = cnum;
            int num = stoi( snum );
            thread Rthread(answerNumQuery,num,req);
            Rthread.detach();
        }
        else if( cword != nullptr )
        {
            string sword = cword;
            thread Rthread(answerGlobalQuery,sword,req);
            Rthread.detach();
        }
    }
    else if( evhttp_request_get_command(req) == EVHTTP_REQ_POST )
    {
        const char* uri = evhttp_request_uri(req); // 修改了这里
        struct evhttp_uri* decoded = evhttp_uri_parse(uri);
        if( !decoded ){
            printf("It's not a good URI. Sending BADREQUEST\n");
            evhttp_send_error(req, HTTP_BADREQUEST, 0);
        }
        struct evkeyvalq params;
        int buffer_data_len = EVBUFFER_LENGTH(req->input_buffer);
        char* http_body = new char[buffer_data_len + 1];
        memset(http_body,'\0', buffer_data_len+1);
        memcpy(http_body, EVBUFFER_DATA(req->input_buffer),buffer_data_len);
        
        string http_body_str = http_body;

        if( myStorage.ring_buffer1_can_access )
        {
            if( !ring_buffer1.push(http_body_str) )
            {
                myStorage.ring_buffer1_can_access = false;
                ring_buffer2.push( http_body_str );
            }
        }
        else
            ring_buffer2.push( http_body_str );

        struct evbuffer* buf = evbuffer_new();
        evbuffer_add_printf(buf,"OK");
        evhttp_send_reply(req, HTTP_OK, "OK", buf);
        evbuffer_free(buf);
    }
}

int main(int argc, char* argv[])
{
    if( argc < 3 ){
        printf("%s ip port\n", argv[0]); exit(0);
    }
    char* ip = argv[1];
    short port = atoi( argv[2] );
    
    count1 = 0;
    count2 = 0;

    myStorage.ring_buffer1_can_access = true;
    // myStorage.setThreshold(100000);
    
    thread Consumer1(consumer1);
    thread Consumer2(consumer2);

    struct event_base* base = event_base_new();
    struct evhttp* http_server = evhttp_new(base);
    if( !http_server )
        return 1;
    
    int ret = evhttp_bind_socket(http_server,ip, port);
    if( ret != 0 )
        return 1;
    
    evhttp_set_gencb(http_server, generic_handler, NULL);
    printf("http server start oK! \n");
    event_base_dispatch(base);
    /* free the http server when no longer used. */
    evhttp_free(http_server);

    Consumer1.join();
    Consumer2.join();

    return 0;
}
