//
// Created by liyu on 12/23/23.
//

#include "reader/TimestampColumnReader.h"

TimestampColumnReader::TimestampColumnReader(
    std::shared_ptr<TypeDescription> type)
    : ColumnReader(type) {}

void TimestampColumnReader::close() {
    if (decoder) {
        decoder->close();
        decoder = nullptr;
    }
}

void TimestampColumnReader::read(std::shared_ptr<ByteBuffer> input,
                                 pixels::proto::ColumnEncoding &encoding,
                                 int offset, int size, int pixelStride,
                                 int vectorIndex,
                                 std::shared_ptr<ColumnVector> vector,
                                 pixels::proto::ColumnChunkIndex &chunkIndex,
                                 std::shared_ptr<PixelsBitMask> filterMask) {
    std::shared_ptr<TimestampColumnVector> columnVector =
        std::static_pointer_cast<TimestampColumnVector>(vector);
    // if read from start, init the stream and decoder
    if (offset == 0) {
        decoder = std::make_shared<RunLenIntDecoder>(input, true);
        ColumnReader::elementIndex = 0;
        isNullOffset = chunkIndex.isnulloffset();
    }

    int pixelId = elementIndex / pixelStride;
    bool hasNull = chunkIndex.pixelstatistics(pixelId).statistic().hasnull();
    setValid(input, pixelStride, vector, pixelId, hasNull);

    if (encoding.kind() == pixels::proto::ColumnEncoding_Kind_RUNLENGTH) {
        for (int i = 0; i < size; i++) {
            if (elementIndex % pixelStride == 0) {
                pixelId = elementIndex / pixelStride;
                hasNull =
                    chunkIndex.pixelstatistics(pixelId).statistic().hasnull();
            }
            if (!vector->isNull[i + vectorIndex]) {
                columnVector->set(i + vectorIndex, decoder->next());
            }
            elementIndex++;
        }
    } else {
        // Handle non-RUNLENGTH encoding
        std::vector<long> buffer(size);
        encodingUtils.readLongBE(input, buffer.data(), 0, size, sizeof(long));

        for (int i = 0; i < size; i++) {
            if (elementIndex % pixelStride == 0) {
                pixelId = elementIndex / pixelStride;
                hasNull =
                    chunkIndex.pixelstatistics(pixelId).statistic().hasnull();
            }
            if (!vector->isNull[i + vectorIndex]) {
                columnVector->set(i + vectorIndex, buffer[i]);
            }
            elementIndex++;
        }
    }
}
