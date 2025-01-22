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

//
// Created by whz on 12/9/24.
//

#ifndef DUCKDB_DECIMALCOLUMNWRITER_H
#define DUCKDB_DECIMALCOLUMNWRITER_H
#include "ColumnWriter.h"
#include "duckdb/common/types.hpp"
#include "encoding/RunLenIntEncoder.h"
#include "utils/EncodingUtils.h"

using PhysicalType = duckdb::PhysicalType;

class DecimalColumnWriter : public ColumnWriter {
  public:
    DecimalColumnWriter(std::shared_ptr<TypeDescription> type,
                        std::shared_ptr<PixelsWriterOption> writerOption);

    int write(std::shared_ptr<ColumnVector> vector, int length) override;
    void close() override;
    void newPixel() override;
    void writeCurPartDecimal(std::shared_ptr<ColumnVector> columnVector,
                             void *values, int curPartLength,
                             int curPartOffset);
    bool decideNullsPadding(
        std::shared_ptr<PixelsWriterOption> writerOption) override;
    pixels::proto::ColumnEncoding getColumnChunkEncoding() const override;

  private:
    bool runlengthEncoding;
    std::unique_ptr<RunLenIntEncoder> encoder;
    std::vector<long>
        curPixelVector; // current pixel value vector haven't written out yet
    PhysicalType physical_type_;
};

#endif // DUCKDB_DECIMALCOLUMNWRITER_H
