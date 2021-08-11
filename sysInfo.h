#ifndef _SYS_INFO_H
#define _SYS_INFO_H

#include <boost/archive/text_oarchive.hpp>
#include <boost/archive/text_iarchive.hpp>
#include <boost/archive/binary_iarchive.hpp>
#include <boost/archive/binary_oarchive.hpp>

class sysInfo
{
private:
    int segment_num;
    int log_count;

    friend class boost::serialization::access;
    template<class Archive>
    void serialize(Archive & ar, const unsigned int version)
    {
        ar& segment_num;
        ar& log_count;
    }
    
public:
    sysInfo() 
    {
        segment_num = 0;
        log_count = 0;
    }
    sysInfo(int segment_num, int log_count) 
    {
        this->segment_num = segment_num;
        this->log_count = log_count;
    }
    ~sysInfo() {}

    int get_segment_num()
    {
        return segment_num;
    }
    void set_segment_num(int segment_num)
    {
        this->segment_num = segment_num;
    }
    int get_log_count()
    {
        return log_count;
    }
    void seg_log_count(int log_count)
    {
        this->log_count = log_count;
    }
};

#endif