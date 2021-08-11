#ifndef _STORAGE_H
#define _STORAGE_H

#include "log.h"
#include "../bloom/bloom_filter.hpp"
#include <vector>
#include <unordered_map>
#include <boost/atomic.hpp>
#include <mutex>
#include <map>
#include <sentencepiece_processor.h>
#include <fstream>

using namespace std;



struct time_term
{
    int segment; // 记录日志存在于哪个segmentfile中
    int offset;  // 日志在该segment file中的偏移量 
    int count; // 日志编号，记录当前日志是第几条日志
};

struct word_offset_info
{
    int segment_num;
    int offset;
};

class Storage
{
public:
    Storage();
    ~Storage();
    void setThreshold(int threshold)
    {
        this->threshold = threshold;
    }
    vector<string> queryByTime(string stime);
    // time range format: 2021-07-06_09:00:00~2021-08-15_09:00:00 (分钟级精确度)
    vector<string> queryByTimeRange(string timeRange, int page = 1);
    vector<string> queryRecentN(int n);
    vector<string> globalSearch(string word);

    bloom_filter generateBloomfilter( int element_count );
    void bloomfilterInitialize();

    void updateSegmentNum();
    void updateSysStartFlag();
    void readSysInfo();
    void readBloomFilters();
    void readTermIndexs();
    void updateTimeTermIndex(int serialNum);
    void updateWordTermIndex(int serialNum);
    void updateBloomFiltersToFile();

    /////////////////////////////////////// add from 2021/08/06
    void writeLogs1(vector<string> logs);
    void writeLogs2(vector<string> logs);
    void readTimeIndex();
    int stimeToItime(string stime);
    void updateSysInfo();
    void generateSysInfoFile();
    void updateWordTermIndex();
    void readWordTermIndex();

public:
    boost::atomic_bool ring_buffer1_can_access;
private:

    ofstream outfile1;
    ofstream outfile2;

    int tlogoffset1;
    int tlogoffset2;

    map<int, time_term> time_index;

    sentencepiece::SentencePieceProcessor processor;

    int logcount1; 
    int logcount2;

    string target_dir;
    int threshold;
    bloom_filter global_filter;
    bloom_filter time_filter;
    int flag = 0;

    boost::atomic_int segment_num;
    boost::atomic_int log_count;

    map<string, vector<word_offset_info>> word_term_index;
    
    map<string, vector<int>> word_term_dict1;
    map<string, vector<int>> word_term_dict2;
    map<int,int> num_to_offset1;
    map<int,int> num_to_offset2;

    mutex time_index_mtx;
    mutex bloom_filter_mtx;


    /*
        * word_term_index
            * word_term_index item structure
                *   word_prefix : <segment_num, offset> <segment_num, offset> <segmeng_num, offset>
                *   内存里应该用什么结构？
                        * map<string, vector<word_info>> 
                *   word_prefix啥时候更新？
                        * 达到threshold才更新
                *   word_prefix写进磁盘的格式？
                        * word_prefix segment_num1-offset1 segment_num2-offset2 segment_num3-offset3 ... 
                        * 
                *   word_prefix是总共一份还是每个dict一份？
                        * word_prefix可以总共一份 
        * word_term_dictionary
            * word_term_dict item structure
                * word_string : log_offset1, log_offset2, log_offset3..... 
                * log_offset
                * 内存里使用的数据结构？ 
                    * map<string,vector<int>> word_term_dict;
                * 注意,内存里的word_term_dict不做查询用途
                * 如何设定格式以方便生成word_term_dict文件？
                    * 知道的信息: logs[i], i, word, logs[i].length()
                    * 思路
                        * 首先生成  log编号 : log_offset结构
                            * log 编号如何确定？ 由log_count决定
                            * 
                        * word: log编号1 log编号2
                        * 最后生成 word : log_offset结构 
        * segment_file
        

        * 参考：time_time_index是达到threshold才会更新
        * 初步想法
            * word index达到threshold才更新
            * word dict在内存里实时更新
            * 达到threshold
                * word dict写入文件
                * 文件里和内存里的word index同步更新  
            * word_prefix 记录到文件里的格式？
                * word_prefix segment_num1-offset1 segment_num2-offset2 segment_num3-offset3 ... 
    */

};

#endif