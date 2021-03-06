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

package cn.lucene.lorc.impl;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.util.Random;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.io.compress.snappy.SnappyDecompressor.SnappyDirectDecompressor;
import org.apache.hadoop.io.compress.zlib.ZlibDecompressor;

/**
 * Shims for versions of Hadoop less than 2.6
 *
 * Adds support for:
 * <ul>
 *   <li>Direct buffer decompression</li>
 *   <li>Zero copy</li>
 * </ul>
 */
public class HadoopShimsPre2_6 implements HadoopShims {

  static class SnappyDirectDecompressWrapper implements DirectDecompressor {
    private final SnappyDirectDecompressor root;
    private boolean isFirstCall = true;

    SnappyDirectDecompressWrapper(SnappyDirectDecompressor root) {
      this.root = root;
    }

    public void decompress(ByteBuffer input, ByteBuffer output) throws IOException {
      if (!isFirstCall) {
        root.reset();
      } else {
        isFirstCall = false;
      }
      root.decompress(input, output);
    }

    @Override
    public void reset() {
      root.reset();
    }

    @Override
    public void end() {
      root.end();
    }
  }

  static class ZlibDirectDecompressWrapper implements DirectDecompressor {
    private final ZlibDecompressor.ZlibDirectDecompressor root;
    private boolean isFirstCall = true;

    ZlibDirectDecompressWrapper(ZlibDecompressor.ZlibDirectDecompressor root) {
      this.root = root;
    }

    public void decompress(ByteBuffer input, ByteBuffer output) throws IOException {
      if (!isFirstCall) {
        root.reset();
      } else {
        isFirstCall = false;
      }
      root.decompress(input, output);
    }

    @Override
    public void reset() {
      root.reset();
    }

    @Override
    public void end() {
      root.end();
    }
  }

  static DirectDecompressor getDecompressor( DirectCompressionType codec) {
    switch (codec) {
      case ZLIB:
        return new ZlibDirectDecompressWrapper
            (new ZlibDecompressor.ZlibDirectDecompressor());
      case ZLIB_NOHEADER:
        return new ZlibDirectDecompressWrapper
            (new ZlibDecompressor.ZlibDirectDecompressor
                (ZlibDecompressor.CompressionHeader.NO_HEADER, 0));
      case SNAPPY:
        return new SnappyDirectDecompressWrapper
            (new SnappyDirectDecompressor());
      default:
        return null;
    }
  }

  public DirectDecompressor getDirectDecompressor( DirectCompressionType codec) {
    return getDecompressor(codec);
 }

  @Override
  public ZeroCopyReaderShim getZeroCopyReader(FSDataInputStream in,
                                              ByteBufferPoolShim pool
                                              ) throws IOException {
    return ZeroCopyShims.getZeroCopyReader(in, pool);
  }

  @Override
  public boolean endVariableLengthBlock(OutputStream output) {
    return false;
  }

  @Override
  public KeyProvider getHadoopKeyProvider(Configuration conf, Random random) {
    return new HadoopShimsPre2_3.NullKeyProvider();
  }
}
