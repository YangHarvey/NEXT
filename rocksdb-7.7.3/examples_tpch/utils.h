#include <string>
#include <vector>
#include <cstdint>
#include <cstring>
#include <ctime>

using namespace std;

const string start_date = "1992-01-01";
const string end_date = "1998-12-31";

inline void splitRow(const string& src, vector<string>& res, string delim = "|") {
    size_t size = src.size();
    size_t pos = 0;
    for (size_t i = 0; i < size; ++i) {
        pos = src.find(delim, i);
        if (pos < size) {
            string s = src.substr(i, pos - i);
            res.push_back(s);
            i = pos + delim.size() - 1;
        }
    }
}

inline double gapDays(const string& date1, const string& date2) {
    std::tm tm1, tm2;
    std::memset(&tm1, 0, sizeof(tm1));
    std::memset(&tm2, 0, sizeof(tm2));
    strptime(date1.data(), "%Y-%m-%d", &tm1);
    strptime(date2.data(), "%Y-%m-%d", &tm2);
    double gapSecs = static_cast<double>(std::difftime(std::mktime(&tm2), std::mktime(&tm1)));
    return gapSecs / 86400;
}

inline double uniformDate(const string& cur_date) {
    double all_days = gapDays(start_date, end_date);
    double cur_days = gapDays(start_date, cur_date);
    return cur_days / all_days;
} 

inline string fill_key(const string& key, size_t sz) {
    string result = string(sz - key.length(), '0') + key;
    return std::move(result);
}

inline string fill_value(const string& value, size_t sz) {
    if (value.length() >= sz) {
        // 如果输入字符串长度大于等于目标大小，截断到目标大小
        return value.substr(0, sz);
    } else {
        // 如果输入字符串长度小于目标大小，用 '0' 填充
        string result = string(sz - value.length(), '0') + value;
        return std::move(result);
    }
}

enum Lineitem {
    l_orderkey = 0,
    l_partkey = 1,
    l_suppkey = 2,
    l_linenumber = 3,
    l_quantity = 4,
    l_extendedprice = 5,
    l_discount = 6,
    l_tax = 7,
    l_returnflag = 8,   
    l_linestatus = 9,   
    l_shipdate = 10,    
    l_commitdate = 11,   
    l_receiptdate = 12,
    l_shipinstruct = 13, 
    l_shipmode = 14,     
    l_comment = 15     
};