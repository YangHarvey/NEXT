#include <algorithm>
#include <cstdint>
#include <cstdio>
#include <string>
#include <fstream>
#include <iostream>
#include <iomanip>
#include <unistd.h>
#include <sstream>
#include <vector>
#include <chrono>
#include <math.h>

#include "rocksdb/db.h"
#include "rocksdb/flush_block_policy.h"
#include "rocksdb/options.h"
#include "rocksdb/slice.h"
#include "rocksdb/slice_transform.h"
#include "rocksdb/table.h"
#include "util/coding.h"
#include "util/rtree.h"
#include "util/hilbert_curve.h"
// #include "util/z_curve.h"
#include "utils.h"

using namespace rocksdb;

std::string serialize_id(int iid) {
    std::string key;
    key.append(reinterpret_cast<const char*>(&iid), sizeof(int));
    return key;
}

std::string serialize_value(double xValueMin, double xValueMax, double yValueMin, double yValueMax) {
    std::string val;
    // The R-tree stores boxes, hence duplicate the input values
    val.append(reinterpret_cast<const char*>(&xValueMin), sizeof(double));
    val.append(reinterpret_cast<const char*>(&xValueMax), sizeof(double));
    val.append(reinterpret_cast<const char*>(&yValueMin), sizeof(double));
    val.append(reinterpret_cast<const char*>(&yValueMax), sizeof(double));
    return val;
}

std::string serialize_query(uint64_t iid_min,
                            uint64_t iid_max, double value_min,
                            double value_max) {
    std::string key;
    key.append(reinterpret_cast<const char*>(&iid_min), sizeof(uint64_t));
    key.append(reinterpret_cast<const char*>(&iid_max), sizeof(uint64_t));
    key.append(reinterpret_cast<const char*>(&value_min), sizeof(double));
    key.append(reinterpret_cast<const char*>(&value_max), sizeof(double));
    key.append(reinterpret_cast<const char*>(&value_min), sizeof(double));
    key.append(reinterpret_cast<const char*>(&value_max), sizeof(double));
    return key;
}

uint64_t decode_value(std::string& value) {
    return *reinterpret_cast<const uint64_t*>(value.data());
}

struct Key {
    Mbr mbr;
};


Key deserialize_key(Slice key_slice) {
    Key key;
    key.mbr = ReadKeyMbr(key_slice);
    return key;
}

void serialize_item(int lid, const vector<string> &rows, std::string *key, std::string *secondary_key, std::string *value) {
    // Encode key
    key->append(reinterpret_cast<const char*>(&lid), sizeof(int));

    // Encode secondary key (l_shipdate + l_quantity)
    double shipdate = uniformDate(rows[l_shipdate].c_str());
    double quantity = std::stod(rows[l_quantity].c_str());
        // since min == max, we can use the same value for both
    secondary_key->append(reinterpret_cast<const char*>(&shipdate), sizeof(double));
    secondary_key->append(reinterpret_cast<const char*>(&shipdate), sizeof(double));
    secondary_key->append(reinterpret_cast<const char*>(&quantity), sizeof(double));
    secondary_key->append(reinterpret_cast<const char*>(&quantity), sizeof(double));

    // Encode value (start with secondary key)
    value->append(secondary_key->data(), secondary_key->size());
        // append the rest of the row
        // l_orderkey, l_linenumber
    value->append(fill_key(rows[l_orderkey], 16));
    value->append(fill_key(rows[l_linenumber], 8));

        // shipdate, quantity, l_receiptdate
    value->append(reinterpret_cast<const char*>(&shipdate), sizeof(double));
    value->append(reinterpret_cast<const char*>(&quantity), sizeof(double));
    double receiptdate = uniformDate(rows[l_receiptdate].c_str());
    value->append(reinterpret_cast<const char*>(&receiptdate), sizeof(double));

        // partkey
    uint64_t partkey = std::stoull(rows[l_partkey].c_str(), nullptr, 10);
    value->append(reinterpret_cast<const char*>(&partkey), sizeof(uint64_t));

        // suppkey
    uint64_t suppkey = std::stoull(rows[l_suppkey].c_str(), nullptr, 10);
    value->append(reinterpret_cast<const char*>(&suppkey), sizeof(uint64_t));

        // extendedprice
    double extendedprice = std::atof(rows[l_extendedprice].c_str());
    value->append(reinterpret_cast<const char*>(&extendedprice), sizeof(double));

        // discount
    double discount = std::atof(rows[l_discount].c_str());
    value->append(reinterpret_cast<const char*>(&discount), sizeof(double));

        // tax
    double tax = std::atof(rows[l_tax].c_str());
    value->append(reinterpret_cast<const char*>(&tax), sizeof(double));

        // returnflag 固定1字节
    char returnflag = rows[l_returnflag][0];
    value->append(1, returnflag);  // 固定1字节

        // linestatus
    char linestatus = rows[l_linestatus][0];
    value->append(1, linestatus);  // 固定1字节
    
        // commitdate
    double commitdate = uniformDate(rows[l_commitdate].c_str());
    value->append(reinterpret_cast<const char*>(&commitdate), sizeof(double));

        // shipinstruct, shipmode, comment
    value->append(fill_value(rows[l_shipinstruct], 64));
    value->append(fill_value(rows[l_shipmode], 64));
    value->append(fill_value(rows[l_comment], 128));
}


// A comparator that interprets keys from Noise. It's a length prefixed
// string first (the keypath) followed by the value and the Internal Id.
class NoiseComparator : public rocksdb::Comparator {
public:
    const char* Name() const {
        return "rocksdb.NoiseComparator";
    }

    int Compare(const rocksdb::Slice& const_a, const rocksdb::Slice& const_b) const {
        Slice slice_a = Slice(const_a);
        Slice slice_b = Slice(const_b);

        // keypaths are the same, compare the value. The previous
        // `GetLengthPrefixedSlice()` did advance the Slice already, hence a call
        // to `.data()` can directly be used.
        const int* value_a = reinterpret_cast<const int*>(slice_a.data());
        const int* value_b = reinterpret_cast<const int*>(slice_b.data());

        return slice_a.compare(slice_b);

    }

    void FindShortestSeparator(std::string* start,
                               const rocksdb::Slice& limit) const {
        return;
    }

    void FindShortSuccessor(std::string* key) const  {
        return;
    }
};

class NoiseComparator1 : public rocksdb::Comparator {
public:
    const char* Name() const {
        return "rocksdb.NoiseComparator";
    }

    int Compare(const rocksdb::Slice& const_a, const rocksdb::Slice& const_b) const {
        Slice slice_a = Slice(const_a);
        Slice slice_b = Slice(const_b);

        // keypaths are the same, compare the value. The previous
        // `GetLengthPrefixedSlice()` did advance the Slice already, hence a call
        // to `.data()` can directly be used.
        const int* value_a = reinterpret_cast<const int*>(slice_a.data());
        const int* value_b = reinterpret_cast<const int*>(slice_b.data());

        // specific comparator to allow random output order
        return 1;
    }

    void FindShortestSeparator(std::string* start,
                               const rocksdb::Slice& limit) const {
        return;
    }

    void FindShortSuccessor(std::string* key) const  {
        return;
    }
};

// 格式化时间显示（秒转换为小时:分钟:秒）
std::string format_time(double seconds) {
    int hours = static_cast<int>(seconds) / 3600;
    int minutes = (static_cast<int>(seconds) % 3600) / 60;
    int secs = static_cast<int>(seconds) % 60;
    std::ostringstream oss;
    if (hours > 0) {
        oss << hours << "h " << minutes << "m " << secs << "s";
    } else if (minutes > 0) {
        oss << minutes << "m " << secs << "s";
    } else {
        oss << secs << "s";
    }
    return oss.str();
}

// 显示进度条
void show_progress(int current, int total, 
                   std::chrono::high_resolution_clock::time_point start_time,
                   std::chrono::nanoseconds total_duration) {
    auto now = std::chrono::high_resolution_clock::now();
    auto elapsed = std::chrono::duration_cast<std::chrono::milliseconds>(now - start_time).count();
    double elapsed_seconds = elapsed / 1000.0;
    
    double progress = (current * 100.0) / total;
    int bar_width = 50;
    int pos = static_cast<int>(bar_width * progress / 100.0);
    
    // 计算速度（条/秒）
    double speed = current > 0 ? current / elapsed_seconds : 0;
    
    // 计算预计剩余时间
    double remaining_seconds = 0;
    if (current > 0 && current < total) {
        remaining_seconds = (elapsed_seconds / current) * (total - current);
    }
    
    std::cout << "\r[";
    for (int i = 0; i < bar_width; ++i) {
        if (i < pos) std::cout << "=";
        else if (i == pos) std::cout << ">";
        else std::cout << " ";
    }
    std::cout << "] " << std::fixed << std::setprecision(1) << progress << "% "
              << "(" << current << "/" << total << ") "
              << "速度: " << std::setprecision(0) << speed << " 条/s "
              << "已用: " << format_time(elapsed_seconds);
    
    if (remaining_seconds > 0) {
        std::cout << " 预计剩余: " << format_time(remaining_seconds);
    }
    
    std::cout << std::flush;
}

int main(int argc, char* argv[]) {

    std::string kDBPath = argv[1];

    int dataSize = int(atoi(argv[2]));
    std::ifstream dataFile(argv[3]);
    std::cout << "data size: " << dataSize << std::endl;

    DB* db;
    Options options;

    NoiseComparator cmp;
    options.comparator = &cmp;
    NoiseComparator1 sec_cmp;
    options.sec_comparator = &sec_cmp;

    options.info_log_level = DEBUG_LEVEL;
    options.statistics = rocksdb::CreateDBStatistics();

    options.max_write_buffer_number = 5;
    options.max_background_jobs = 8;

    BlockBasedTableOptions block_based_options;
    
    // For per file secondary index in SST file
    block_based_options.create_secondary_index = true;
    block_based_options.create_sec_index_reader = true;
    
    // For global secondary index in memory
    options.create_global_sec_index = true;

    options.table_factory.reset(NewBlockBasedTableFactory(block_based_options));
    options.memtable_factory.reset(new rocksdb::SkipListSecFactory);
    
    options.allow_concurrent_memtable_write = false;

    // Set the write buffer size to 64 MB
    options.write_buffer_size = 64 * 1024 * 1024;

    Status s;

    int id;
    uint32_t op;
    double perimeter;
    double low[2], high[2];

    // Failed to open, probably it doesn't exist yet. Try to create it and
    // insert data
    if (true) {
        options.create_if_missing = true;
        s = DB::Open(options, kDBPath, &db);
        std::cout << "Create if missing: " << s.ToString() << std::endl;
        assert(s.ok());

        std::cout << "start writing data" << std::endl;
        // auto totalDuration = std::chrono::duration<long long, std::milli>(0);
        std::chrono::nanoseconds totalDuration{0};
        
        // 记录开始时间用于进度条
        auto overall_start = std::chrono::high_resolution_clock::now();
        const int update_interval = std::max(1, dataSize / 100); // 每1%更新一次，至少每1条更新

        std::string line;
        int lineCount = 0;
        while(std::getline(dataFile, line)) {
            if(lineCount == dataSize){
                break;
            }
            lineCount++;

            vector<string> rows;
            splitRow(line, rows);
            std::string key, secondary_key, value;
            serialize_item(lineCount, rows, &key, &secondary_key, &value);

            auto start = std::chrono::high_resolution_clock::now();
            s = db->Put(WriteOptions(), key, value);
            // std::cout << "write first data" << std::endl;
            auto end = std::chrono::high_resolution_clock::now(); 
            auto duration = std::chrono::duration_cast<std::chrono::nanoseconds>(end - start);
            totalDuration = totalDuration + duration;
            
            // 更新进度条（每update_interval条或最后一条）
            if (lineCount % update_interval == 0 || lineCount == dataSize) {
                show_progress(lineCount, dataSize, overall_start, totalDuration);
            }
        }
        
        // 完成时换行
        std::cout << std::endl;
        
        std::cout << "Status: " << s.ToString() << std::endl;
        assert(s.ok());

        std::cout << "end writing data" << std::endl;
        auto overall_end = std::chrono::high_resolution_clock::now();
        auto overall_duration = std::chrono::duration_cast<std::chrono::milliseconds>(overall_end - overall_start);
        std::cout << "Total execution time: " << format_time(overall_duration.count() / 1000.0) 
                  << " (" << overall_duration.count() << " ms)" << std::endl;
        std::cout << "Total write time: " << totalDuration.count() << " nanoseconds" << std::endl;

        // sleep to complete the background compactions 
        sleep(480);

        db->Close();

    }

    delete db;

    return 0;
}