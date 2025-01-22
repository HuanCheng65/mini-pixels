//
// Created by yuly on 05.04.23.
//

#include "reader/DecimalColumnReader.h"
#include "duckdb/common/types/decimal.hpp"

/**
 * The column reader of decimals.
 * <p><b>Note: it only supports short decimals with max precision and
 * scale 18.</b></p>
 * @author hank
 */
DecimalColumnReader::DecimalColumnReader(std::shared_ptr<TypeDescription> type)
    : ColumnReader(type) {
    int precision = type->getPrecision();
    using duckdb::Decimal;
    if (precision <= Decimal::MAX_WIDTH_INT16) {
        physical_type_ = PhysicalType::INT16;
    } else if (precision <= Decimal::MAX_WIDTH_INT32) {
        physical_type_ = PhysicalType::INT32;
    } else if (precision <= Decimal::MAX_WIDTH_INT64) {
        physical_type_ = PhysicalType::INT64;
    } else if (precision <= Decimal::MAX_WIDTH_INT128) {
        physical_type_ = PhysicalType::INT128;
    } else {
        throw std::runtime_error(
            "Decimal precision is bigger than the maximum supported width");
    }
}

void DecimalColumnReader::close() {
    if (decoder) {
        decoder.reset();
    }
}

void DecimalColumnReader::read(std::shared_ptr<ByteBuffer> input,
                               pixels::proto::ColumnEncoding &encoding,
                               int offset, int size, int pixelStride,
                               int vectorIndex,
                               std::shared_ptr<ColumnVector> vector,
                               pixels::proto::ColumnChunkIndex &chunkIndex,
                               std::shared_ptr<PixelsBitMask> filterMask) {
    std::shared_ptr<DecimalColumnVector> columnVector =
        std::static_pointer_cast<DecimalColumnVector>(vector);

    // 验证精度和小数位数是否匹配
    if (type->getPrecision() != columnVector->getPrecision() ||
        type->getScale() != columnVector->getScale()) {
        throw InvalidArgumentException(
            "reader of decimal(" + std::to_string(type->getPrecision()) + "," +
            std::to_string(type->getScale()) +
            ") doesn't match the column vector of decimal(" +
            std::to_string(columnVector->getPrecision()) + "," +
            std::to_string(columnVector->getScale()) + ")");
    }

    // 确保[offset, offset + size)在同一个pixel内
    assert(offset / pixelStride == (offset + size - 1) / pixelStride);

    if (offset == 0) {
        if (encoding.kind() == pixels::proto::ColumnEncoding_Kind_RUNLENGTH) {
            decoder = std::make_shared<RunLenIntDecoder>(input, true);
        }
        ColumnReader::elementIndex = 0;
        isNullOffset = chunkIndex.isnulloffset();
    }

    int pixelId = elementIndex / pixelStride;
    bool hasNull = chunkIndex.pixelstatistics(pixelId).statistic().hasnull();
    setValid(input, pixelStride, vector, pixelId, hasNull);

    if (encoding.kind() == pixels::proto::ColumnEncoding_Kind_RUNLENGTH) {
        // 处理RunLength编码的数据
        for (int i = 0; i < size; i++) {
            switch (physical_type_) {
            case PhysicalType::INT16: {
                auto *int16_vector =
                    reinterpret_cast<int16_t *>(columnVector->vector);
                int16_vector[vectorIndex + i] =
                    static_cast<int16_t>(decoder->next());
                break;
            }
            case PhysicalType::INT32: {
                auto *int32_vector =
                    reinterpret_cast<int32_t *>(columnVector->vector);
                int32_vector[vectorIndex + i] =
                    static_cast<int32_t>(decoder->next());
                break;
            }
            case PhysicalType::INT64:
            case PhysicalType::INT128: {
                auto *int64_vector =
                    reinterpret_cast<int64_t *>(columnVector->vector);
                int64_vector[vectorIndex + i] = decoder->next();
                break;
            }
            default:
                throw InvalidArgumentException(
                    "Unsupported physical type for decimal");
            }
            elementIndex++;
        }
    } else {
        // 处理未编码的数据
        switch (physical_type_) {
        case PhysicalType::INT16: {
            std::memcpy(reinterpret_cast<int16_t *>(columnVector->vector) +
                            vectorIndex,
                        input->getPointer() + input->getReadPos(),
                        size * sizeof(int16_t));
            input->setReadPos(input->getReadPos() + size * sizeof(int16_t));
            break;
        }
        case PhysicalType::INT32: {
            std::memcpy(reinterpret_cast<int32_t *>(columnVector->vector) +
                            vectorIndex,
                        input->getPointer() + input->getReadPos(),
                        size * sizeof(int32_t));
            input->setReadPos(input->getReadPos() + size * sizeof(int32_t));
            break;
        }
        case PhysicalType::INT64:
        case PhysicalType::INT128: {
            std::memcpy(reinterpret_cast<int64_t *>(columnVector->vector) +
                            vectorIndex,
                        input->getPointer() + input->getReadPos(),
                        size * sizeof(int64_t));
            input->setReadPos(input->getReadPos() + size * sizeof(int64_t));
            break;
        }
        default:
            throw InvalidArgumentException(
                "Unsupported physical type for decimal");
        }
    }
}
