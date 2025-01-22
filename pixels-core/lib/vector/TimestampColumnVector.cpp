//
// Created by liyu on 12/23/23.
//

#include "vector/TimestampColumnVector.h"
#include <cmath>
#include <cstdint>
#include <iomanip>
#include <sstream>

TimestampColumnVector::TimestampColumnVector(int precision, bool encoding)
    : TimestampColumnVector(VectorizedRowBatch::DEFAULT_SIZE, precision,
                            encoding) {}

TimestampColumnVector::TimestampColumnVector(uint64_t len, int precision,
                                             bool encoding)
    : ColumnVector(len, encoding) {
    this->precision = precision;
    posix_memalign(reinterpret_cast<void **>(&this->times), 64,
                   len * sizeof(long));
    memoryUsage += (long)sizeof(long) * len;
}

void TimestampColumnVector::close() {
    if (!closed) {
        ColumnVector::close();
        if (encoding && this->times != nullptr) {
            free(this->times);
        }
        this->times = nullptr;
    }
}

void TimestampColumnVector::print(int rowCount) {
    throw InvalidArgumentException("not support print longcolumnvector.");
    //    for(int i = 0; i < rowCount; i++) {
    //        std::cout<<longVector[i]<<std::endl;
    //		std::cout<<intVector[i]<<std::endl;
    //    }
}

TimestampColumnVector::~TimestampColumnVector() {
    if (!closed) {
        TimestampColumnVector::close();
    }
}

void *TimestampColumnVector::current() {
    if (this->times == nullptr) {
        return nullptr;
    } else {
        return this->times + readIndex;
    }
}

/**
 * Set a row from a value, which is the days from 1970-1-1 UTC.
 * We assume the entry has already been isRepeated adjusted.
 *
 * @param elementNum
 * @param days
 */
void TimestampColumnVector::set(int elementNum, long ts) {
    if (elementNum >= writeIndex) {
        writeIndex = elementNum + 1;
    }
    isNull[elementNum] = false;
    times[elementNum] = roundMicrosToPrecision(ts, precision);
}

void TimestampColumnVector::add(std::string &value) {
    try {
        // 解析时间戳字符串 "YYYY-MM-DD HH:MM:SS[.fraction]"
        std::tm tm = {};
        std::istringstream ss(value);
        ss >> std::get_time(&tm, "%Y-%m-%d %H:%M:%S");
        if (ss.fail()) {
            throw InvalidArgumentException(
                "Invalid timestamp format. Expected YYYY-MM-DD HH:MM:SS");
        }

        // 转换为time_t (秒数)
        std::time_t time = std::mktime(&tm);

        // 处理微秒部分
        long micros = 0;
        if (ss.get() == '.') {
            std::string frac;
            ss >> frac;
            if (!frac.empty()) {
                // 处理最多6位小数（微秒）
                frac = frac.substr(0, 6);
                micros = std::stol(frac) * std::pow(10, 6 - frac.length());
            }
        }

        std::tm epoch = {};
        epoch.tm_year = 70;
        epoch.tm_mon = 0;
        epoch.tm_mday = 1;
        epoch.tm_hour = 0;
        epoch.tm_min = 0;
        epoch.tm_sec = 0;
        long epoch_ts = std::mktime(&epoch);
        long diff = time - epoch_ts;

        // 转换为微秒时间戳
        long timestamp = diff * 1000000L + micros;

        // 根据精度进行舍入
        timestamp = roundMicrosToPrecision(timestamp, precision);

        add(timestamp);
    } catch (const std::exception &e) {
        throw InvalidArgumentException("Failed to parse timestamp string: " +
                                       value);
    }
}

long TimestampColumnVector::roundMicrosToPrecision(long micros, int precision) {
    if (precision < 6) {
        long factor = std::pow(10, 6 - precision);
        return (micros / factor) * factor;
    }
    return micros;
}

void TimestampColumnVector::add(bool value) {
    throw InvalidArgumentException("Cannot convert boolean to timestamp.");
}

void TimestampColumnVector::add(int64_t value) {
    if (writeIndex >= length) {
        ensureSize(writeIndex * 2, true);
    }
    int index = writeIndex++;
    times[index] = value;
    isNull[index] = false;
}

void TimestampColumnVector::add(int value) {
    // 将int值转换为int64
    add(static_cast<int64_t>(value));
}

void TimestampColumnVector::ensureSize(uint64_t size, bool preserveData) {
    ColumnVector::ensureSize(size, preserveData);
    if (length < size) {
        long *oldTimes = times;
        posix_memalign(reinterpret_cast<void **>(&times), 64,
                       size * sizeof(int64_t));
        if (preserveData) {
            std::copy(oldTimes, oldTimes + length, times);
        }
        delete[] oldTimes;
        memoryUsage += (uint64_t)sizeof(uint64_t) * (size - length);
        resize(size);
    }
}