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

import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import cn.lucene.lorc.OrcFile;
import cn.lucene.lorc.impl.WriterImpl;

/**
 * An ORCv2 file writer. The file is divided into stripes, which is the natural
 * unit of work when reading. Each stripe is buffered in memory until the
 * memory reaches the stripe size and then it is written out broken down by
 * columns. Each column is written by a TreeWriter that is specific to that
 * type of column. TreeWriters may have children TreeWriters that handle the
 * sub-types. Each of the TreeWriters writes the column's data as a set of
 * streams.
 *
 * This class is unsynchronized like most Stream objects, so from the creation
 * of an OrcFile and all access to a single instance has to be from a single
 * thread.
 *
 * There are no known cases where these happen between different threads today.
 *
 * Caveat: the MemoryManager is created during WriterOptions create, that has
 * to be confined to a single thread as well.
 *
 */
public class WriterImplV2 extends WriterImpl {

  private static final Logger LOG = LoggerFactory.getLogger(WriterImplV2.class);

  public WriterImplV2(Path path,FSDataOutputStream output,
                      OrcFile.WriterOptions opts) throws IOException {
    super(path,output, opts);
    LOG.warn("ORC files written in " +
        OrcFile.Version.UNSTABLE_PRE_2_0.getName() + " will not be" +
          " readable by other versions of the software. It is only for" +
          " developer testing.");
  }
}
