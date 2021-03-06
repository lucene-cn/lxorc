/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package cn.lucene.lorc.impl.writer;

import java.io.IOException;

import org.apache.hadoop.hive.ql.exec.vector.ColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.LongColumnVector;
import org.apache.hadoop.hive.ql.util.JavaDataModel;

import cn.lucene.lorc.TypeDescription;
import cn.lucene.lorc.impl.CryptoUtils;
import cn.lucene.lorc.impl.IntegerWriter;
import cn.lucene.lorc.impl.OutStream;
import cn.lucene.lorc.impl.PositionRecorder;
import cn.lucene.lorc.impl.StreamName;
import cn.lucene.orc.OrcProto;

public class IntegerTreeWriter extends TreeWriterBase {
  private final IntegerWriter writer;
  private boolean isDirectV2 = true;
  private final boolean isLong;

  public IntegerTreeWriter(TypeDescription schema,
                           WriterEncryptionVariant encryption,
                           WriterContext writer) throws IOException {
    super(schema, encryption, writer);
    OutStream out = writer.createStream(
        new StreamName(id, OrcProto.Stream.Kind.DATA, encryption));
    this.isDirectV2 = isNewWriteFormat(writer);
    this.writer = createIntegerWriter(out, true, isDirectV2, writer);
    if (rowIndexPosition != null) {
      recordPosition(rowIndexPosition);
    }
    this.isLong = schema.getCategory() == TypeDescription.Category.LONG;
  }

  @Override
  OrcProto.ColumnEncoding.Builder getEncoding() {
    OrcProto.ColumnEncoding.Builder result = super.getEncoding();
    if (isDirectV2) {
      result.setKind(OrcProto.ColumnEncoding.Kind.DIRECT_V2);
    } else {
      result.setKind(OrcProto.ColumnEncoding.Kind.DIRECT);
    }
    return result;
  }

  @Override
  public void writeBatch(ColumnVector vector, int offset,
                         int length) throws IOException {
    super.writeBatch(vector, offset, length);
    LongColumnVector vec = (LongColumnVector) vector;
    if (vector.isRepeating) {
      if (vector.noNulls || !vector.isNull[0]) {
        long value = vec.vector[0];
        indexStatistics.updateInteger(value, length);
        if (createBloomFilter) {
          if (bloomFilter != null) {
            bloomFilter.addLong(value);
          }
          bloomFilterUtf8.addLong(value);
        }
        for (int i = 0; i < length; ++i) {
          writer.write(value);
        }
      }
    } else {
      for (int i = 0; i < length; ++i) {
        if (vec.noNulls || !vec.isNull[i + offset]) {
          long value = vec.vector[i + offset];
          writer.write(value);
          indexStatistics.updateInteger(value, 1);
          if (createBloomFilter) {
            if (bloomFilter != null) {
              bloomFilter.addLong(value);
            }
            bloomFilterUtf8.addLong(value);
          }
        }
      }
    }
  }

  @Override
  public void writeStripe(int requiredIndexEntries) throws IOException {
    super.writeStripe(requiredIndexEntries);
    if (rowIndexPosition != null) {
      recordPosition(rowIndexPosition);
    }
  }

  @Override
  void recordPosition(PositionRecorder recorder) throws IOException {
    super.recordPosition(recorder);
    writer.getPosition(recorder);
  }

  @Override
  public long estimateMemory() {
    return super.estimateMemory() + writer.estimateMemory();
  }

  @Override
  public long getRawDataSize() {
    JavaDataModel jdm = JavaDataModel.get();
    long num = fileStatistics.getNumberOfValues();
    return num * (isLong ? jdm.primitive2() : jdm.primitive1());
  }

  @Override
  public void flushStreams() throws IOException {
    super.flushStreams();
    writer.flush();
  }

  @Override
  public void prepareStripe(int stripeId) {
    super.prepareStripe(stripeId);
    writer.changeIv(CryptoUtils.modifyIvForStripe(stripeId));
  }
}
