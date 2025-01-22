//
// Created by yuly on 05.04.23.
//

#include "vector/DecimalColumnVector.h"
#include "duckdb/common/types/decimal.hpp"
#include <cmath>
#include <cstring>
#include <sstream>

/**
 * The decimal column vector with precision and scale.
 * The values of this column vector are the unscaled integer value
 * of the decimal. For example, the unscaled value of 3.14, which is
 * of the type decimal(3,2), is 314. While the precision and scale
 * of this decimal are 3 and 2, respectively.
 *
 * <p><b>Note: it only supports short decimals with max precision
 * and scale 18.</b></p>
 *
 * Created at: 05/03/2022
 * Author: hank
 */

DecimalColumnVector::DecimalColumnVector(int precision, int scale,
                                         bool encoding)
    : DecimalColumnVector(VectorizedRowBatch::DEFAULT_SIZE, precision, scale,
                          encoding) {}

DecimalColumnVector::DecimalColumnVector(uint64_t len, int precision, int scale,
                                         bool encoding)
    : ColumnVector(len, encoding) {
    // decimal column vector has no encoding so we don't allocate memory to
    // this->vector
    this->vector = nullptr;
    this->precision = precision;
    this->scale = scale;

    using duckdb::Decimal;
    if (precision <= Decimal::MAX_WIDTH_INT16) {
        physical_type_ = PhysicalType::INT16;
        posix_memalign(reinterpret_cast<void **>(&vector), 32,
                       len * sizeof(int16_t));
        memoryUsage += (uint64_t)sizeof(int16_t) * len;
    } else if (precision <= Decimal::MAX_WIDTH_INT32) {
        physical_type_ = PhysicalType::INT32;
        posix_memalign(reinterpret_cast<void **>(&vector), 32,
                       len * sizeof(int32_t));
        memoryUsage += (uint64_t)sizeof(int32_t) * len;
    } else if (precision <= Decimal::MAX_WIDTH_INT64) {
        physical_type_ = PhysicalType::INT64;
        posix_memalign(reinterpret_cast<void **>(&vector), 32,
                       len * sizeof(int64_t));
        memoryUsage += (uint64_t)sizeof(int64_t) * len;
    } else if (precision <= Decimal::MAX_WIDTH_INT128) {
        physical_type_ = PhysicalType::INT128;
        posix_memalign(reinterpret_cast<void **>(&vector), 32,
                       len * sizeof(__int128));
        memoryUsage += (uint64_t)sizeof(__int128) * len;
    } else {
        throw std::runtime_error(
            "Decimal precision is bigger than the maximum supported width");
    }
}

void DecimalColumnVector::close() {
    if (!closed) {
        ColumnVector::close();
        if (physical_type_ == PhysicalType::INT16 ||
            physical_type_ == PhysicalType::INT32) {
            free(vector);
        }
        vector = nullptr;
    }
}

void DecimalColumnVector::print(int rowCount) {
    //    throw InvalidArgumentException("not support print
    //    Decimalcolumnvector.");
    for (int i = 0; i < rowCount; i++) {
        std::cout << vector[i] << std::endl;
    }
}

DecimalColumnVector::~DecimalColumnVector() {
    if (!closed) {
        DecimalColumnVector::close();
    }
}

void *DecimalColumnVector::current() {
    if (vector == nullptr) {
        return nullptr;
    } else {
        return vector + readIndex;
    }
}

int DecimalColumnVector::getPrecision() { return precision; }

int DecimalColumnVector::getScale() { return scale; }

void DecimalColumnVector::add(std::string &value) {
    try {
        // 解析十进制字符串
        std::stringstream ss(value);
        double doubleValue;
        ss >> doubleValue;
        if (ss.fail()) {
            throw InvalidArgumentException("Invalid decimal format: " + value);
        }

        // 根据scale计算unscaled value
        long unscaledValue =
            static_cast<long>(doubleValue * std::pow(10, scale));

        // 检查精度是否超出范围
        long maxValue = static_cast<long>(std::pow(10, precision)) - 1;
        long minValue = -maxValue;
        if (unscaledValue > maxValue || unscaledValue < minValue) {
            throw InvalidArgumentException("Decimal value exceeds precision: " +
                                           value);
        }

        add(unscaledValue);
    } catch (const std::exception &e) {
        throw InvalidArgumentException("Failed to parse decimal string: " +
                                       value);
    }
}

void DecimalColumnVector::add(bool value) {
    // 布尔值转换为decimal：true=1.0, false=0.0
    add(static_cast<int64_t>(value ? std::pow(10, scale) : 0));
}

void DecimalColumnVector::add(int64_t value) {
    if (writeIndex >= length) {
        ensureSize(writeIndex * 2, true);
    }
    int index = writeIndex++;

    // 根据physical_type_选择正确的存储方式
    switch (physical_type_) {
    case PhysicalType::INT16: {
        auto *int16_vector = reinterpret_cast<int16_t *>(vector);
        int16_vector[index] = static_cast<int16_t>(value);
        break;
    }
    case PhysicalType::INT32: {
        auto *int32_vector = reinterpret_cast<int32_t *>(vector);
        int32_vector[index] = static_cast<int32_t>(value);
        break;
    }
    case PhysicalType::INT64:
    case PhysicalType::INT128:
        vector[index] = value;
        break;
    default:
        throw InvalidArgumentException("Unsupported physical type for decimal");
    }
    isNull[index] = false;
}

void DecimalColumnVector::add(int value) {
    // 将int转换为int64后添加
    add(static_cast<int64_t>(value));
}

void DecimalColumnVector::ensureSize(uint64_t size, bool preserveData) {
    ColumnVector::ensureSize(size, preserveData);
    if (length < size) {
        void *oldVector = vector;
        size_t elementSize;

        // 根据physical_type_确定元素大小
        switch (physical_type_) {
        case PhysicalType::INT16:
            elementSize = sizeof(int16_t);
            break;
        case PhysicalType::INT32:
            elementSize = sizeof(int32_t);
            break;
        case PhysicalType::INT64:
        case PhysicalType::INT128:
            elementSize = sizeof(long);
            break;
        default:
            throw InvalidArgumentException(
                "Unsupported physical type for decimal");
        }

        posix_memalign(reinterpret_cast<void **>(&vector), 32,
                       size * elementSize);

        if (preserveData && oldVector != nullptr) {
            std::memcpy(vector, oldVector, length * elementSize);
        }

        if (oldVector != nullptr) {
            free(oldVector);
        }

        memoryUsage += elementSize * (size - length);
        resize(size);
    }
}
