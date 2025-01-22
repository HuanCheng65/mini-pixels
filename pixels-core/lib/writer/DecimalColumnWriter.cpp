/*
 * Copyright 2024 PixelsDB.
 *
 * This file is part of Pixels.
 *
 * Pixels is free software: you can redistribute it and/or modify
 * it under the terms of the Affero GNU General Public License as
 * published by the Free Software Foundation, either version 3 of
 * the License, or (at your option) any later version.
 *
 * Pixels is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * Affero GNU General Public License for more details.
 *
 * You should have received a copy of the Affero GNU General Public
 * License along with Pixels.  If not, see
 * <https://www.gnu.org/licenses/>.
 */

#include "writer/DecimalColumnWriter.h"
#include "duckdb/common/types/decimal.hpp"
#include "utils/BitUtils.h"

DecimalColumnWriter::DecimalColumnWriter(
    std::shared_ptr<TypeDescription> type,
    std::shared_ptr<PixelsWriterOption> writerOption)
    : ColumnWriter(type, writerOption), curPixelVector(pixelStride) {

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

    runlengthEncoding = encodingLevel.ge(EncodingLevel::Level::EL2);
    if (runlengthEncoding) {
        encoder = std::make_unique<RunLenIntEncoder>();
    }
}

int DecimalColumnWriter::write(std::shared_ptr<ColumnVector> vector, int size) {
    auto columnVector = std::static_pointer_cast<DecimalColumnVector>(vector);
    if (!columnVector) {
        throw std::invalid_argument("Invalid vector type");
    }
    void *values = columnVector->vector;

    int curPartLength; // size of the partition which belongs to current pixel
    int curPartOffset =
        0; // starting offset of the partition which belongs to current pixel
    int nextPartLength =
        size; // size of the partition which belongs to next pixel

    while ((curPixelIsNullIndex + nextPartLength) >= pixelStride) {
        curPartLength = pixelStride - curPixelIsNullIndex;
        writeCurPartDecimal(columnVector, values, curPartLength, curPartOffset);
        newPixel();
        curPartOffset += curPartLength;
        nextPartLength = size - curPartOffset;
    }

    curPartLength = nextPartLength;
    writeCurPartDecimal(columnVector, values, curPartLength, curPartOffset);

    return outputStream->getWritePos();
}

void DecimalColumnWriter::writeCurPartDecimal(
    std::shared_ptr<ColumnVector> columnVector, void *values, int curPartLength,
    int curPartOffset) {

    for (int i = 0; i < curPartLength; i++) {
        curPixelEleIndex++;
        if (columnVector->isNull[i + curPartOffset]) {
            hasNull = true;
            if (nullsPadding) {
                curPixelVector[curPixelVectorIndex++] = 0L;
            }
        } else {
            switch (physical_type_) {
            case PhysicalType::INT16: {
                auto *int16_values = reinterpret_cast<int16_t *>(values);
                curPixelVector[curPixelVectorIndex++] =
                    int16_values[i + curPartOffset];
                break;
            }
            case PhysicalType::INT32: {
                auto *int32_values = reinterpret_cast<int32_t *>(values);
                curPixelVector[curPixelVectorIndex++] =
                    int32_values[i + curPartOffset];
                break;
            }
            case PhysicalType::INT64:
            case PhysicalType::INT128: {
                auto *int64_values = reinterpret_cast<int64_t *>(values);
                curPixelVector[curPixelVectorIndex++] =
                    int64_values[i + curPartOffset];
                break;
            }
            default:
                throw InvalidArgumentException(
                    "Unsupported physical type for decimal");
            }
        }
    }
    std::copy(columnVector->isNull + curPartOffset,
              columnVector->isNull + curPartOffset + curPartLength,
              isNull.begin() + curPixelIsNullIndex);
    curPixelIsNullIndex += curPartLength;
}

void DecimalColumnWriter::close() {
    if (runlengthEncoding && encoder) {
        encoder->clear();
    }
    ColumnWriter::close();
}

void DecimalColumnWriter::newPixel() {
    if (runlengthEncoding) {
        std::vector<byte> buffer(curPixelVectorIndex * sizeof(long));
        int resLen;
        encoder->encode(curPixelVector.data(), buffer.data(),
                        curPixelVectorIndex, resLen);
        outputStream->putBytes(buffer.data(), resLen);
    } else {
        std::shared_ptr<ByteBuffer> curVecPartitionBuffer;
        EncodingUtils encodingUtils;
        size_t elementSize;

        switch (physical_type_) {
        case PhysicalType::INT16:
            elementSize = sizeof(int16_t);
            curVecPartitionBuffer =
                std::make_shared<ByteBuffer>(curPixelVectorIndex * elementSize);
            if (byteOrder == ByteOrder::PIXELS_LITTLE_ENDIAN) {
                for (int i = 0; i < curPixelVectorIndex; i++) {
                    encodingUtils.writeShortLE(
                        curVecPartitionBuffer,
                        static_cast<int16_t>(curPixelVector[i]));
                }
            } else {
                for (int i = 0; i < curPixelVectorIndex; i++) {
                    encodingUtils.writeShortBE(
                        curVecPartitionBuffer,
                        static_cast<int16_t>(curPixelVector[i]));
                }
            }
            break;
        case PhysicalType::INT32:
            elementSize = sizeof(int32_t);
            curVecPartitionBuffer =
                std::make_shared<ByteBuffer>(curPixelVectorIndex * elementSize);
            if (byteOrder == ByteOrder::PIXELS_LITTLE_ENDIAN) {
                for (int i = 0; i < curPixelVectorIndex; i++) {
                    encodingUtils.writeIntLE(
                        curVecPartitionBuffer,
                        static_cast<int32_t>(curPixelVector[i]));
                }
            } else {
                for (int i = 0; i < curPixelVectorIndex; i++) {
                    encodingUtils.writeIntBE(
                        curVecPartitionBuffer,
                        static_cast<int32_t>(curPixelVector[i]));
                }
            }
            break;
        case PhysicalType::INT64:
        case PhysicalType::INT128:
            elementSize = sizeof(int64_t);
            curVecPartitionBuffer =
                std::make_shared<ByteBuffer>(curPixelVectorIndex * elementSize);
            if (byteOrder == ByteOrder::PIXELS_LITTLE_ENDIAN) {
                for (int i = 0; i < curPixelVectorIndex; i++) {
                    encodingUtils.writeLongLE(curVecPartitionBuffer,
                                              curPixelVector[i]);
                }
            } else {
                for (int i = 0; i < curPixelVectorIndex; i++) {
                    encodingUtils.writeLongBE(curVecPartitionBuffer,
                                              curPixelVector[i]);
                }
            }
            break;
        default:
            throw InvalidArgumentException(
                "Unsupported physical type for decimal");
        }

        outputStream->putBytes(curVecPartitionBuffer->getPointer(),
                               curVecPartitionBuffer->getWritePos());
    }

    ColumnWriter::newPixel();
}

pixels::proto::ColumnEncoding
DecimalColumnWriter::getColumnChunkEncoding() const {
    pixels::proto::ColumnEncoding columnEncoding;
    if (runlengthEncoding) {
        columnEncoding.set_kind(
            pixels::proto::ColumnEncoding::Kind::ColumnEncoding_Kind_RUNLENGTH);
    } else {
        columnEncoding.set_kind(
            pixels::proto::ColumnEncoding::Kind::ColumnEncoding_Kind_NONE);
    }
    return columnEncoding;
}

bool DecimalColumnWriter::decideNullsPadding(
    std::shared_ptr<PixelsWriterOption> writerOption) {
    if (writerOption->getEncodingLevel().ge(EncodingLevel::Level::EL2)) {
        return false;
    }
    return writerOption->isNullsPadding();
}