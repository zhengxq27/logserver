#include <iostream>
#include <fstream>
#include <sstream>

#include <boost/archive/text_oarchive.hpp>
#include <boost/archive/text_iarchive.hpp>
#include <boost/archive/binary_iarchive.hpp>
#include <boost/archive/binary_oarchive.hpp>
#include <boost/serialization/string.hpp>
#include <boost/serialization/vector.hpp>

#include <sys/mman.h>
#include <unistd.h>
#include <fcntl.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <vector>

#include "storage.h"
#include "sysInfo.h"

#define THRESHOLD 1000000

Storage::~Storage() {}
Storage::Storage()
{
    target_dir = "/data/code/logdata/";
    threshold = THRESHOLD;
    ring_buffer1_can_access = true;

    const auto status = processor.Load("tokenizer.model"); // load the tokenizer
    if( !status.ok() )
    {
        cout << status.ToString() << endl;
        exit(0);
    }

    logcount1 = 0;
    logcount2 = 0;

    tlogoffset1 = 0;
    tlogoffset2 = 0;
   
    // read the sys start flag
    fstream sys_start_flag;
    sys_start_flag.open(target_dir + "sys_start_flag.txt", ios::in | ios::out);
    if( !sys_start_flag.is_open() )
    {
        cout << "system loading failures." << endl;
        exit(0);
    }
    string line;
    getline(sys_start_flag, line);
    sys_start_flag.close();

    flag = stoi(line);
    if( flag == 0 )
    {
        segment_num = 0;
        log_count = 0;
        // generate a sys info file, record the segment num infomation
        generateSysInfoFile();
        bloomfilterInitialize(); // 初始化bloom filter
        // update the system start flag
        updateSysStartFlag();
        
        string segment_file = "segment" + to_string(segment_num) + ".txt";
        outfile1.open(target_dir + segment_file, ios::app);
        if( !outfile1.is_open() )
        {
            cout << "system initializing fails." << endl;
            exit(0);
        }
        segment_num++;
        updateSysInfo();
    }
    else if( flag == 1 )
    {
        // read the system info file to get the segment_num;
        readSysInfo();
        // read the bloom filters from file 
        readBloomFilters();
        // read the time index from file
        readTimeIndex();
        // read the word term index from file
        readWordTermIndex();
        cout << "size of word term index: " << word_term_index.size() << endl;
        string segment_file = "segment" + to_string(segment_num-1) + ".txt"; 
        outfile1.open(target_dir + segment_file, ios::app);
        if( !outfile1.is_open() )
        {
            cout << "system initializing fails." << endl;
            exit(0);
        }
        outfile1.seekp(0, ios::end);
        int fileSize = outfile1.tellp();
        if( fileSize != 0 )
        {
            outfile1.close();
            outfile1.open(target_dir + "segment" + to_string(segment_num) + ".txt");
            if( !outfile1.is_open() )
            {
                cout << "sysytem initializing fails." << endl;
                exit(0);
            }
            segment_num++;
            updateSysInfo();
        }
        cout << "initialize OK." << endl;
    }

    
}

void Storage::writeLogs1(vector<string> logs)
{
    for(int i = 0; i < logs.size(); i++)
    {
        outfile1 << logs[i] << endl;
        log_count++;
        // update the time index
        int rn = logs[i].find('\n');
        string stime = logs[i].substr(0,rn);
        time_t itime = stimeToItime(stime);
        time_filter.insert(itime); // update the bloom filter
        map<int, time_term>::iterator iter;
        iter = time_index.find( itime );
        if( iter == time_index.end() )
        {
            time_term val;
            val.count = log_count;
            val.segment = segment_num-1; // segment_num-1
            val.offset = tlogoffset1;
            time_index[itime] = val;
        }
        /* word term dict */
        vector<string> words;
        int index = logs[i].find_last_of('\n');
        string log = logs[i].substr(index+1);
        processor.Encode(log, &words);
        for(int j = 0; j < words.size(); j++)
            words[j] = words[j].substr(3);
        map<string, vector<int>>::iterator witer;
        for(int j = 0; j < words.size(); j++)
        {
            witer = word_term_dict1.find( words[j] );
            if( witer != word_term_dict1.end() )
            {
                witer->second.push_back(logcount1);
            }
            else
            {
                vector<int> val;
                val.push_back(logcount1);
                word_term_dict1[ words[j] ] = val;
            }     
        }
        num_to_offset1[ logcount1 ] = tlogoffset1;
        logcount1++;
        tlogoffset1 += logs[i].length() + 1;

        // 如果这里ring_buffer1对网络线程来说不可访问，则ring_buffer1中的数据全部存在一个段文件里不分段
        if( logcount1 >= threshold && ring_buffer1_can_access )
        {
            outfile1.close();
            // update the time index to file
            ofstream time_index_file;
            time_index_file.open(target_dir + "time_index.txt", ios::out);
            if( !time_index_file.is_open() )
            {
                cout << "udpate time index file fails." << endl;
                exit(0);
            }
            for(iter = time_index.begin(); iter != time_index.end(); iter++)
            {
                string line = to_string(iter->first) + " " + to_string(iter->second.segment)
                            + " " + to_string(iter->second.offset) + " " + to_string(iter->second.count);
                time_index_file << line << endl; 
            }
            time_index_file.close();

            // write the word term dict to file
            string new_word_term_dict_name = "word_term_dict" + to_string(segment_num-1) + ".txt";
            ofstream new_word_term_dict;
            new_word_term_dict.open(target_dir + new_word_term_dict_name, ios::out);
            if( !new_word_term_dict.is_open() )
            {
                cout << "if: create a new word term dictionary fails." << endl;
                exit(0);
            }
            map<string, vector<int>>::iterator witer;
            for(witer = word_term_dict1.begin(); witer != word_term_dict1.end(); witer++)
            {
                string line = witer->first + " ";
                vector<int> val = witer->second;
                for(int i = 0; i < val.size(); i++)
                {
                    line += to_string( num_to_offset1[ val[i] ] );
                    line += " ";
                }
                new_word_term_dict << line << endl;
            }
            new_word_term_dict.close();
            updateWordTermIndex();

            // 打开新的文件
            string new_segment_file = "segment" + to_string(segment_num) + ".txt";
            outfile1.open(target_dir + new_segment_file, ios::app);
            if( !outfile1.is_open() )
            {
                cout << "open segment file fails. " << endl;
                exit(0);
            }
                        
            logcount1 = 0;
            tlogoffset1 = 0;
            segment_num++;
            updateSysInfo();
            updateBloomFiltersToFile();
        }
    }
    if( !ring_buffer1_can_access )
    {
        outfile1.close();
        // update the time index to file
        ofstream time_index_file;
        time_index_file.open(target_dir + "time_index.txt", ios::out);
        if( !time_index_file.is_open() )
        {
            cout << "udpate time index file fails." << endl;
            exit(0);
        }
        map<int, time_term>::iterator iter;
        for(iter = time_index.begin(); iter != time_index.end(); iter++)
        {
            string line = to_string(iter->first) + " " + to_string(iter->second.segment)
                        + " " + to_string(iter->second.offset) + " " + to_string(iter->second.count);
            time_index_file << line << endl; 
        }
        time_index_file.close();

        string new_segment_file = "segment" + to_string(segment_num) + ".txt";
        outfile1.open(target_dir + new_segment_file, ios::app);
        if( !outfile1.is_open() )
        {
            cout << "open segment file fails. " << endl;
            exit(0);
        }
        logcount1 = 0;
        tlogoffset1 = 0;
        segment_num++;
        updateSysInfo();
        updateBloomFiltersToFile();
        ring_buffer1_can_access = true; // 确认把ringbuffer1清空之后再把ring_buffer1_can_access设置为true
    }
}

void Storage::writeLogs2(vector<string> logs)
{
    if( !outfile2.is_open() )
    {
        string segment_file_name = "segment" + to_string(segment_num) + ".txt";
        outfile2.open(target_dir + segment_file_name, ios::app);
        segment_num++;
        updateSysInfo();
    }
    for(int i = 0; i < logs.size(); i++)
    {
        outfile2 << logs[i] << endl;
        log_count++;
        // update the time index
        int rn = logs[i].find('\n');
        string stime = logs[i].substr(0,rn);
        time_t itime = stimeToItime(stime);
        time_filter.insert(itime);
        map<int, time_term>::iterator iter;
        iter = time_index.find( itime );
        if( iter == time_index.end() )
        {
            time_term val;
            val.count = log_count;
            val.segment = segment_num-1;
            val.offset = tlogoffset1;
            time_index[itime] = val;
        }
        
        logcount2++;
        tlogoffset2 += logs[i].length() + 1;

        // 如果segment_file达到阈值且ring_buffer1不可访问，则ring_buffer2正常写文件，正常分段
        // 如果ring_buffer1可以访问，则ring_buffer2写的文件不会分段
        if( logcount2 >= threshold && !ring_buffer1_can_access )
        {
            outfile2.close();
            // update the time index to file
            ofstream time_index_file;
            time_index_file.open(target_dir + "time_index.txt", ios::out);
            if( !time_index_file.is_open() )
            {
                cout << "udpate time index file fails." << endl;
                exit(0);
            }
            for(iter = time_index.begin(); iter != time_index.end(); iter++)
            {
                string line = to_string(iter->first) + " " + to_string(iter->second.segment)
                            + " " + to_string(iter->second.offset) + " " + to_string(iter->second.count);
                time_index_file << line << endl; 
            }
            time_index_file.close();

            string new_segment_file = "segment" + to_string(segment_num) + ".txt";
            outfile2.open(target_dir + new_segment_file, ios::app);
            if( !outfile2.is_open() )
            {
                cout << "open segment file fails. " << endl;
                exit(0);
            }

            logcount2 = 0;
            tlogoffset2 = 0;
            segment_num++;
            updateSysInfo();
            updateBloomFiltersToFile();
        }
    }
    if( ring_buffer1_can_access )
    {
        outfile2.close();
        // update the time index to file
        ofstream time_index_file;
        time_index_file.open(target_dir + "time_index.txt", ios::out);
        if( !time_index_file.is_open() )
        {
            cout << "udpate time index file fails." << endl;
            exit(0);
        }
        map<int, time_term>::iterator iter;
        for(iter = time_index.begin(); iter != time_index.end(); iter++)
        {
            string line = to_string(iter->first) + " " + to_string(iter->second.segment)
                        + " " + to_string(iter->second.offset) + " " + to_string(iter->second.count);
            time_index_file << line << endl; 
        }
        time_index_file.close();

        logcount2 = 0;
        tlogoffset2 = 0;
        updateSysInfo(); // 注意这里没有segment_num++
        updateBloomFiltersToFile();
    }
}

vector<string> Storage::queryByTime(string stime)
{
    vector<string> res;
    time_t itime = stimeToItime(stime);
    if( !time_filter.contains(itime) ) // time bloom filter testing
    {
        return res;
    }
    map<int, time_term>::iterator iter = time_index.find(itime);
    if( iter == time_index.end() ) // not in disk, query finishes.
        return res;
    else
    {
        time_term val = iter->second;
        int segment = val.segment;
        int offset = val.offset;
        int count = val.count;

        string segment_file_name = "segment" + to_string(segment) + ".txt";
        ifstream segment_file;
        segment_file.open(target_dir + segment_file_name, ios::in);
        if( !segment_file.is_open() )
        {
            cout << "segment file open fails." << endl;
            exit(0);
        }
        segment_file.seekg(offset, ios::beg);
        int n = count < 101 ? count : 100;
        string str;
        string line;
        for(int i = 0; i < 3*n; i++)
        {
            getline(segment_file, line);
            str += line;
            str += "\n";
        }
        res.push_back(str);
        segment_file.close();
        return res;
    }
    return res;
}

vector<string> Storage::queryByTimeRange(string timeRange, int page)
{
    int pass_num = (page-1)*100;

    vector<string> res;
    string begin_time = timeRange.substr(0,19);
    string end_time = timeRange.substr(20,19);
    int ibegin_time = stimeToItime(begin_time);
    int iend_time = stimeToItime(end_time);

    int begin_segment, end_segment;
    int begin_offset, end_offset;
    int begin_count, end_count;

    map<int, time_term>::iterator iter1 = time_index.find(ibegin_time);
    if( iter1 != time_index.end() )
    {
        begin_segment = iter1->second.segment;
        begin_offset = iter1->second.offset;
        begin_count = iter1->second.count;
    }
    else
        return res;

    map<int, time_term>::iterator iter2 = time_index.find(iend_time);
    if( iter2 != time_index.end() )
    {
        end_segment = iter2->second.segment;
        end_offset = iter2->second.offset;
        iter2++; // 找下一个
        end_count = iter2->second.count;
    }
    else
        return res;

    int num = end_count - begin_count;
    if( num <= 100 ) // 直接不理会pass_num
    {
        if( begin_segment == end_segment )
        {
            ifstream segment_file;
            string segment_file_name = "segment" + to_string(begin_segment) + ".txt";
            segment_file.open(target_dir + segment_file_name, ios::in);
            if( !segment_file.is_open() )
            {
                cout << "segment file open fails. " << endl;
                exit(0);
            }
            segment_file.seekg(begin_offset, ios::beg);
            string line;
            for(int i = 0; i < num; i++)
            {
                string log;
                for(int j = 0; j < 3; j++)
                {
                    getline(segment_file,line);
                    log += line;
                    log += "\n";
                }
                res.push_back(log);
            }
            return res;
        }
    }
    else
    {
        string str = "the log num is: " + to_string(num) + "\n";
        res.push_back(str);
        if( begin_segment == end_segment )
        {
            ifstream segment_file;
            string segment_file_name = "segment" + to_string(begin_segment) + ".txt";
            segment_file.open(target_dir + segment_file_name, ios::in);
            if( !segment_file.is_open() )
            {
                cout << "segment file open fails. " << endl;
                exit(0);
            }
            if( pass_num > 0 )
            {
                map<int, time_term>::iterator iter = time_index.find(ibegin_time);
                map<int, time_term>::iterator former = iter;
                while( iter->second.count - begin_count < pass_num )
                {
                    former = iter;
                    iter++;
                }
                int diff = pass_num - (former->second.count - begin_count);
                segment_file.seekg(former->second.offset, ios::beg);
                string line;
                for(int i = 0; i < 3*diff; i++) // notice that each log has three lines
                    getline(segment_file,line);
                int n = (end_count - begin_count)- pass_num;
                n = n > 100 ? 100 : n;
                
                for(int i = 0; i < n; i++)
                {
                    string log = "";
                    for(int j = 0; j < 3; j++)
                    {
                        getline(segment_file, line);
                        log += line;
                        log += "\n";
                    }
                    res.push_back(log);
                }
                return res;

            }
            else
            {
                segment_file.seekg(begin_offset,ios::beg);
                string log;
                string line;
                for(int i = 0; i < 100; i++)
                {
                    string log;
                    for(int j = 0; j < 3; j++)
                    {
                        getline(segment_file,line);
                        log += line;
                        log += "\n";
                    }
                    res.push_back(log);
                }
                return res;
            }
           
        }
    }
    return res;
}
// to be changed
vector<string> Storage::queryRecentN(int n)
{
    
    vector<string>res;
    int num = n > 1000 ? 1000 : n; // 设置最大的可追溯条数为100
    string file_name = "segment" + to_string(segment_num-1) + ".txt";
    ifstream file;
    file.open(target_dir + file_name, ios::in);
    if( !file.is_open() )
    {
        cout << "segment file open fails." << endl;
        exit(0);
    }

    file.seekg(0, ios::end);
    int fileSize = file.tellg();
    if( fileSize == 0 )
    {
        file.close();
        file.open(target_dir + "segment" + to_string(segment_num-2) + ".txt", ios::in);
        if( !file.is_open() )
        {
            cout << "segment file open fails." << endl;
            exit(0);
        }
        file.seekg(0, ios::end);
    }

    for(int i = 0; i < n*3+1; i++)
    {
        while( file.peek() != file.widen('\n') )
            file.seekg(-1, ios::cur);
        file.seekg(-1, ios::cur);
    }
    file.seekg(2, ios::cur);
    string line;
    while( getline(file,line) )
        res.push_back(line);
    return res;

}

vector<string> Storage::globalSearch(string word)
{
    vector<string> res;
    string word_prefix = word;
    if( word_prefix.length() > 5 )
        word_prefix = word_prefix.substr(0,5);

    map<string, vector<word_offset_info>>::iterator iter;
    iter = word_term_index.find(word_prefix);
    if( iter != word_term_index.end() )
    {
        vector<word_offset_info> val = iter->second;
        for(int i = 0; i < val.size(); i++)
        {
            int segment = val[i].segment_num;
            int offset = val[i].offset;
            string word_term_dict_name = "word_term_dict" + to_string(segment) + ".txt";
            ifstream word_term_dict;
            word_term_dict.open(target_dir + word_term_dict_name, ios::in);
            if( !word_term_dict.is_open() )
            {
                cout << "open word term dictionary fails." << endl;
                exit(0); // to be changed
            }
            word_term_dict.seekg(offset, ios::beg);
            string line;
            while( getline(word_term_dict, line) ) // 从word_prefix起始的地方开始找
            {
                int index = line.find(' ');
                string key = line.substr(0, index);
                if( key == word )
                {
                    // 找到要找的单词就停止
                    break;
                } 
            }
            word_term_dict.close();

            vector<string> vline;
            stringstream ss(line);
            string word;
            while( ss >> word )
                vline.push_back( word );
            
            ifstream segment_file;
            segment_file.open(target_dir + "segment" + to_string(segment) + ".txt");
            if( !segment_file.is_open() )
            {
                cout << "open segment file fails." << endl;
                exit(0);
            }
            string log;
            string Line;
            for(int j = 1; j < vline.size(); j++)
            {
                segment_file.seekg(stoi(vline[j]), ios::beg);
                for(int t = 0; t < 3; t++)
                {
                    getline(segment_file,Line);
                    log += Line;
                    log += "\n";
                }
                res.push_back(log);
            }
            segment_file.close();
        }
        return res;
    }
    else
        return res;
}

bloom_filter Storage::generateBloomfilter(int element_count )
{
    bloom_parameters parameters;
    parameters.projected_element_count = element_count;
    parameters.false_positive_probability = 0.00001;
    if( !parameters )
    {
        std::cout << "Error - Invalid set of bloom filter parameters!" << std::endl;
        exit(0);
    }
    parameters.compute_optimal_parameters();
    bloom_filter filter(parameters);
    return filter;  // return a value, not a pointer
}

void Storage::bloomfilterInitialize()
{
    // geneate two bloom filter
    global_filter = generateBloomfilter(300000);
    time_filter = generateBloomfilter(1000000);

    // write the global filter to file
    ofstream write_global_filter;
    write_global_filter.open(target_dir + "global_bloom_filter.data", ios::out);
    if( !write_global_filter.is_open() )
    {
        cout << "initialize the global bloom filter fails." << endl;
        exit(0);
    }
    boost::archive::text_oarchive global_filter_oa(write_global_filter);
    global_filter_oa << global_filter;
    write_global_filter.close();

    // write the time filter to file
    ofstream write_time_filter;
    write_time_filter.open(target_dir + "time_bloom_filter.data", ios::out);
    if( !write_time_filter.is_open() )
    {
        cout << "initialize the time bloom filter fails." << endl;
        exit(0);
    }
    boost::archive::text_oarchive time_filter_oa(write_time_filter);
    time_filter_oa << time_filter;
    write_time_filter.close();
}

void Storage::updateSysInfo()
{
    sysInfo m_sysinfo;
    m_sysinfo.set_segment_num(segment_num);
    m_sysinfo.seg_log_count(log_count);
    ofstream update_sys_info;
    update_sys_info.open(target_dir + "system_info.data", ios::out);
    if( !update_sys_info.is_open() )
    {
        cout << "update system info fails. " << endl;
        exit(0);
    }
    boost::archive::text_oarchive update_sys_info_oa(update_sys_info);
    update_sys_info_oa << m_sysinfo;
    update_sys_info.close();
}

void Storage::updateSysStartFlag()
{
    ofstream update_sys_start_flag;
    update_sys_start_flag.open(target_dir + "sys_start_flag.txt", ios::out);
    if( !update_sys_start_flag.is_open() )
    {
        cout << "update system start flag fails." << endl;
        exit(0);
    }
    update_sys_start_flag << to_string(1) << endl;
    update_sys_start_flag.close();
}

void Storage::readSysInfo()
{
     // read the sys info from file
    ifstream sys_info_file;
    sys_info_file.open(target_dir + "system_info.data", ios::in);
    if( !sys_info_file.is_open() )
    {
        cout << "system initializing failure." << endl;
        exit(0);
    }
    sysInfo m_sysInfo;
    boost::archive::text_iarchive sys_info_ia(sys_info_file);
    sys_info_ia >> m_sysInfo;
    sys_info_file.close();
    segment_num = m_sysInfo.get_segment_num();
    log_count = m_sysInfo.get_log_count();
}

void Storage::readBloomFilters()
{
    // read the time bloom filter from file
    ifstream read_time_filter;
    read_time_filter.open(target_dir + "time_bloom_filter.data", ios::in);
    if( !read_time_filter.is_open() )
    {
        cout << "initialize the time bloom filter fails." << endl;
        exit(0);
    }
    boost::archive::text_iarchive time_filter_ia(read_time_filter);
    time_filter_ia >> time_filter; 
    read_time_filter.close();

    // read the global bloom filter from file
    ifstream read_global_filter;
    read_global_filter.open(target_dir + "global_bloom_filter.data", ios::in);
    if( !read_global_filter.is_open() )
    {
        cout << "initialize the global bloom filter fails." << endl;
        exit(0);
    }
    boost::archive::text_iarchive global_filter_ia(read_global_filter);
    global_filter_ia >> global_filter;
    read_global_filter.close();
}

void Storage::readTermIndexs()
{
    // read the global term index from file
    ifstream read_word_term_index;
    read_word_term_index.open(target_dir + "word_term_index.txt", ios::in);
    if( !read_word_term_index.is_open() )
    {
        cout << "initializing word term index fails." << endl;
        exit(0);
    }
    string line1;
    string word1;
    while ( getline(read_word_term_index, line1) )
    {
        stringstream ss(line1);
        vector<string> vline;
        while( ss >> word1 )
            vline.push_back(word1);
        
        string word_prefix = vline[0];
        vector<word_offset_info> val;
        for(int i = 1; i < vline.size(); i++)
        {
            int _index = vline[i].find('-');
            int segment = stoi( vline[i].substr(0, _index)); // to be tested
            int offset = stoi( vline[i].substr(_index+1) );
            word_offset_info addOne;
            addOne.segment_num = segment;
            addOne.offset = offset;
            val.push_back(addOne);
        }
    }
    read_word_term_index.close();
}

void Storage::updateWordTermIndex(int curSegment_num) // 把segment_num传递过来
{
    /* 先更新内存里的word term index */
    string word_term_dict_file_name = "word_term_dict" + to_string(curSegment_num) + ".txt";
    ifstream word_term_dict_file;
    word_term_dict_file.open(target_dir + word_term_dict_file_name, ios::in);
    if( !word_term_dict_file.is_open() )
    {
        cout << "udpate word term index fails when open word term dictionary file." << endl;
        exit(0);
    }
    vector<string> word_term_dict_lines;
    string line;
    while( getline(word_term_dict_file,line) )
        word_term_dict_lines.push_back( line );
    word_term_dict_file.close();
    
    map<string, vector<word_offset_info>>::iterator iter;
    for(int i = 0; i < word_term_dict_lines.size(); i++)
    {
        vector<string> words;
        stringstream ss( word_term_dict_lines[i] );
        string word;
        while( ss >> word )
            words.push_back(word);
        string word_prefix = words[0];
        if( word_prefix.length() > 5 )
            word_prefix = word_prefix.substr(0,5);
        
        iter = word_term_index.find(word_prefix);
        if( iter == word_term_index.end() )
        {
            vector<word_offset_info> val;
            for(int j = 1; j < words.size(); j++)
            {
                word_offset_info addOne;
                addOne.segment_num = curSegment_num;
                addOne.offset = stoi( words[j] );
                val.push_back(addOne);
            }
            word_term_index[ word_prefix ] = val;
        }
        else
        {
            for(int j = 1; j < words.size(); j++)
            {
                word_offset_info addOne;
                addOne.segment_num = curSegment_num;
                addOne.offset = stoi( words[j] );
                iter->second.push_back(addOne);
            }
        }
    }

    /* 将内存里更新完的word term index写入文件 */
    ofstream word_term_index_file;
    word_term_index_file.open(target_dir + "word_term_index.txt", ios::out);
    while( !word_term_index_file.is_open() )
    {
        cout << "update word term index file fails." << endl;
        exit(0);
    }

    for(iter = word_term_index.begin(); iter != word_term_index.end(); iter++)
    {
        string line = iter->first + " ";
        vector<word_offset_info> val = iter->second;
        for(int i = 0; i < val.size(); i++)
        {
            string str = to_string(val[i].segment_num) + "-" + to_string(val[i].offset);
            line += str;
            line += " ";
        }
        word_term_index_file << line << endl;
    }
    word_term_index_file.close();
}
void Storage::updateBloomFiltersToFile()
{
    // update the time filter
    ofstream update_time_filter;
    update_time_filter.open(target_dir + "time_bloom_filter.data", ios::out);
    if( !update_time_filter.is_open() )
    {
        cout << "udpate the time bloom filter fails. " << endl;
        exit(0);
    }   
    boost::archive::text_oarchive time_filter_oa(update_time_filter);
    time_filter_oa << time_filter;
    update_time_filter.close();

    // udpate the global filter
    ofstream update_global_filter;
    update_global_filter.open(target_dir + "global_bloom_filter.data", ios::out);
    if( !update_global_filter.is_open() )
    {
        cout << "update the global bloom filter fails." << endl;
        exit(0);
    }
    boost::archive::text_oarchive global_filter_oa(update_global_filter);
    global_filter_oa << global_filter;
    update_global_filter.close();
}

void Storage::readTimeIndex()
{
    ifstream time_index_file;
    time_index_file.open(target_dir + "time_index.txt", ios::in);
    if( !time_index_file.is_open() )
    {
        cout << "initialize time index fails. " << endl;
        exit(0);
    }
    string line;
    while( getline(time_index_file,line) )
    {
        vector<string> vline;
        stringstream ss(line);
        string word;
        while( ss >> word )
            vline.push_back(word);
        
        int itime = stoi( vline[0] );
        time_term val;
        val.segment = stoi( vline[1] );
        val.offset = stoi( vline[2] );
        val.count = stoi( vline[3] );
        time_index[itime] = val;        
    }
    time_index_file.close();
    //cout << "size of time index: " << time_index.size() << endl;
}

int Storage::stimeToItime(string stime)
{
    tm tm_;
    strptime(stime.c_str(), "%Y-%m-%d_%H:%M:%S", &tm_);
    tm_.tm_isdst = -1;
    time_t itime = mktime(&tm_);
    return itime;
}

void Storage::generateSysInfoFile()
{
    sysInfo m_sysinfo;
    m_sysinfo.set_segment_num(0);
    m_sysinfo.seg_log_count(0);
    ofstream update_sys_info;
    update_sys_info.open(target_dir + "system_info.data", ios::out);
    if( !update_sys_info.is_open() )
    {
        cout << "generate system info file fails." << endl;
        exit(0);
    }
    boost::archive::text_oarchive update_sys_info_oa(update_sys_info);
    update_sys_info_oa << m_sysinfo;
    update_sys_info.close();
}

void Storage::updateWordTermIndex()
{
    /* 先更新内存里的word term index */
    string word_term_dict_file_name = "word_term_dict" + to_string(segment_num-1) + ".txt";
    ifstream word_term_dict_file;
    word_term_dict_file.open(target_dir + word_term_dict_file_name, ios::in);
    if( !word_term_dict_file.is_open() )
    {
        cout << "udpate word term index fails when open word term dictionary file." << endl;
        exit(0);
    }

    vector<string> word_term_dict_lines;
    string line;
    while( getline(word_term_dict_file,line) )
        word_term_dict_lines.push_back( line );
    word_term_dict_file.close();
    
    int offset = 0;
    map<string, vector<word_offset_info>>::iterator iter;
    for(int i = 0; i < word_term_dict_lines.size(); i++)
    {
        int index = word_term_dict_lines[i].find(' ');
        string word_prefix = word_term_dict_lines[i].substr(0, index);
        if( word_prefix.length() > 5 )
            word_prefix = word_prefix.substr(0,5);
        
        iter = word_term_index.find(word_prefix);
        if( iter == word_term_index.end() )
        {
            vector<word_offset_info> val;
            word_offset_info addOne;
            addOne.segment_num = segment_num-1;
            addOne.offset = offset;
            val.push_back(addOne);
            word_term_index[ word_prefix ] = val;
        }
        else
        {
            word_offset_info addOne;
            addOne.segment_num = segment_num-1;
            addOne.offset = offset;
            iter->second.push_back(addOne);
        }
        offset += word_term_dict_lines[i].length();
        offset += 1;
    }

    /* 将内存里更新完的word term index写入文件 */
    ofstream word_term_index_file;
    word_term_index_file.open(target_dir + "word_term_index.txt", ios::out);
    while( !word_term_index_file.is_open() )
    {
        cout << "update word term index file fails." << endl;
        exit(0);
    }

    for(iter = word_term_index.begin(); iter != word_term_index.end(); iter++)
    {
        string line = iter->first + " ";
        vector<word_offset_info> val = iter->second;
        for(int i = 0; i < val.size(); i++)
        {
            string str = to_string(val[i].segment_num) + "-" + to_string(val[i].offset);
            line += str;
            line += " ";
        }
        word_term_index_file << line << endl;
    }
    word_term_index_file.close();
}

void Storage::readWordTermIndex()
{
    // read the word term index from file
    ifstream read_word_term_index;
    read_word_term_index.open(target_dir + "word_term_index.txt", ios::in);
    if( !read_word_term_index.is_open() )
    {
        cout << "initializing word term index fails." << endl;
        exit(0);
    }
    string line1;
    string word1;
    while ( getline(read_word_term_index, line1) )
    {
        stringstream ss(line1);
        vector<string> vline;
        while( ss >> word1 )
            vline.push_back(word1);
        
        string word_prefix = vline[0];
        if( word_prefix.length() > 5 )
            word_prefix = word_prefix.substr(0,5);
        
        vector<word_offset_info> val;
        for(int i = 1; i < vline.size(); i++)
        {
            int _index = vline[i].find('-');
            int segment = stoi( vline[i].substr(0, _index)); // to be tested
            int offset = stoi( vline[i].substr(_index+1) );
            word_offset_info addOne;
            addOne.segment_num = segment;
            addOne.offset = offset;
            val.push_back(addOne);
        }
        word_term_index[ word_prefix ] = val; 
    }
    cout << word_term_index.size() << endl;

    string key = "abart";
    if( word_term_index.find(key) != word_term_index.end() )
        cout << "bingo" << endl;
    else 
        cout << "sad." << endl;
    read_word_term_index.close();
}