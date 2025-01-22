//
// Created by yuly on 05.04.23.
//

#ifndef PIXELS_DECIMALCOLUMNREADER_H
#define PIXELS_DECIMALCOLUMNREADER_H

#include "duckdb/common/types.hpp"
#include "encoding/RunLenIntDecoder.h"
#include "reader/ColumnReader.h"

using PhysicalType = duckdb::PhysicalType;

class DecimalColumnReader : public ColumnReader {
  public:
    explicit DecimalColumnReader(std::shared_ptr<TypeDescription> type);
    void close() override;
    void read(std::shared_ptr<ByteBuffer> input,
              pixels::proto::ColumnEncoding &encoding, int offset, int size,
              int pixelStride, int vectorIndex,
              std::shared_ptr<ColumnVector> vector,
              pixels::proto::ColumnChunkIndex &chunkIndex,
              std::shared_ptr<PixelsBitMask> filterMask) override;

  private:
    PhysicalType physical_type_;
    std::shared_ptr<RunLenIntDecoder> decoder;
};

#endif // PIXELS_DECIMALCOLUMNREADER_H
