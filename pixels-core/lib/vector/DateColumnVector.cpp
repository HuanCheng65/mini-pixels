//
// Created by yuly on 06.04.23.
//

#include "vector/DateColumnVector.h"
#include <iomanip>
#include <sstream>

DateColumnVector::DateColumnVector(uint64_t len, bool encoding)
    : ColumnVector(len, encoding) {
    posix_memalign(reinterpret_cast<void **>(&dates), 32,
                   len * sizeof(int32_t));
    memoryUsage += (long)sizeof(int) * len;
}

void DateColumnVector::close() {
    if (!closed) {
        if (encoding && dates != nullptr) {
            free(dates);
        }
        dates = nullptr;
        ColumnVector::close();
    }
}

void DateColumnVector::print(int rowCount) {
    for (int i = 0; i < rowCount; i++) {
        std::cout << dates[i] << std::endl;
    }
}

DateColumnVector::~DateColumnVector() {
    if (!closed) {
        DateColumnVector::close();
    }
}

/**
 * Set a row from a value, which is the days from 1970-1-1 UTC.
 * We assume the entry has already been isRepeated adjusted.
 *
 * @param elementNum
 * @param days
 */
void DateColumnVector::set(int elementNum, int days) {
    if (elementNum >= writeIndex) {
        writeIndex = elementNum + 1;
    }
    dates[elementNum] = days;
    // TODO: isNull
}

void *DateColumnVector::current() {
    if (dates == nullptr) {
        return nullptr;
    } else {
        return dates + readIndex;
    }
}

void DateColumnVector::add(std::string &value) {
    try {
        // 解析日期字符串 "YYYY-MM-DD"
        std::tm tm = {};
        std::istringstream ss(value);
        ss >> std::get_time(&tm, "%Y-%m-%d");
        if (ss.fail()) {
            throw InvalidArgumentException(
                "Invalid date format. Expected YYYY-MM-DD");
        }

        // 转换为time_t (秒数)
        std::time_t time = std::mktime(&tm);

        // 计算从1970-01-01到当前日期的天数
        // 使用86400秒 = 1天来计算
        int days = static_cast<int>(time / 86400);

        // 存储天数
        add(days);
    } catch (const std::exception &e) {
        throw InvalidArgumentException("Failed to parse date string: " + value);
    }
}

void DateColumnVector::add(bool value) {
    // 布尔值转换为日期没有实际意义，抛出异常
    throw InvalidArgumentException("Cannot convert boolean to date.");
}

void DateColumnVector::add(int64_t value) {
    // 将int64转换为int，因为日期用int存储足够
    add(static_cast<int>(value));
}

void DateColumnVector::add(int value) {
    if (writeIndex >= length) {
        ensureSize(writeIndex * 2, true);
    }
    int index = writeIndex++;
    dates[index] = value;
    isNull[index] = false;
}

void DateColumnVector::ensureSize(uint64_t size, bool preserveData) {
    ColumnVector::ensureSize(size, preserveData);
    if (length < size) {
        int *oldDates = dates;
        posix_memalign(reinterpret_cast<void **>(&dates), 32,
                       size * sizeof(int32_t));
        if (preserveData) {
            std::copy(oldDates, oldDates + length, dates);
        }
        delete[] oldDates;
        memoryUsage += (long)sizeof(int) * (size - length);
        resize(size);
    }
}
