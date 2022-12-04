/*
 * Copyright 2016 The Apache Software Foundation.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.orc.impl;

import java.io.ByteArrayInputStream;
import java.io.EOFException;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.CharacterCodingException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PositionedReadable;
import org.apache.hadoop.fs.Seekable;
import org.apache.hadoop.fs.FileSystem;
import org.apache.orc.FileFormatException;
import org.apache.hadoop.io.Text;
import org.apache.orc.OrcFile;
import org.apache.orc.OrcProto;
import org.apache.orc.OrcUtils;
import org.apache.orc.Reader;
import org.apache.orc.RecordReader;
import org.apache.orc.StripeStatistics;
import org.apache.orc.TestVectorOrcFile;
import org.junit.Test;
import org.apache.orc.TypeDescription;
import org.junit.Before;
import org.junit.Rule;
import org.junit.rules.ExpectedException;

import static org.junit.Assert.assertEquals;

public class TestReaderImpl {
  private Path workDir = new Path(System.getProperty("example.dir",
      "../../examples/"));

  @Rule
  public ExpectedException thrown = ExpectedException.none();

  private final Path path = new Path("test-file.orc");
  private FSDataInputStream in;
  private int psLen;
  private ByteBuffer buffer;

  @Before
  public void setup() {
    in = null;
  }

  @Test
  public void testEnsureOrcFooterSmallTextFile() throws IOException {
    prepareTestCase("1".getBytes());
    thrown.expect(FileFormatException.class);
    ReaderImpl.ensureOrcFooter(in, path, psLen, buffer);
  }

  @Test
  public void testEnsureOrcFooterLargeTextFile() throws IOException {
    prepareTestCase("This is Some Text File".getBytes());
    thrown.expect(FileFormatException.class);
    ReaderImpl.ensureOrcFooter(in, path, psLen, buffer);
  }

  @Test
  public void testEnsureOrcFooter011ORCFile() throws IOException {
    prepareTestCase(composeContent(OrcFile.MAGIC, "FOOTER"));
    ReaderImpl.ensureOrcFooter(in, path, psLen, buffer);
  }

  @Test
  public void testEnsureOrcFooterCorrectORCFooter() throws IOException {
    prepareTestCase(composeContent("", OrcFile.MAGIC));
    ReaderImpl.ensureOrcFooter(in, path, psLen, buffer);
  }

  @Test
  public void testOptionSafety() throws IOException {
    Reader.Options options = new Reader.Options();
    String expected = options.toString();
    Configuration conf = new Configuration();
    Path path = new Path(TestVectorOrcFile.getFileFromClasspath
        ("orc-file-11-format.orc"));
    Reader reader = OrcFile.createReader(path, OrcFile.readerOptions(conf));
    RecordReader rows = reader.rows(options);
    assertEquals(expected, options.toString());
  }

  private void prepareTestCase(byte[] bytes) throws IOException {
    buffer = ByteBuffer.wrap(bytes);
    psLen = buffer.get(bytes.length - 1) & 0xff;
    in = new FSDataInputStream(new SeekableByteArrayInputStream(bytes));
  }

  private byte[] composeContent(String headerStr, String footerStr) throws CharacterCodingException {
    ByteBuffer header = Text.encode(headerStr);
    ByteBuffer footer = Text.encode(footerStr);
    int headerLen = header.remaining();
    int footerLen = footer.remaining() + 1;

    ByteBuffer buf = ByteBuffer.allocate(headerLen + footerLen);

    buf.put(header);
    buf.put(footer);
    buf.put((byte) footerLen);
    return buf.array();
  }

  private static final class SeekableByteArrayInputStream extends ByteArrayInputStream
          implements Seekable, PositionedReadable {

    public SeekableByteArrayInputStream(byte[] buf) {
      super(buf);
    }

    @Override
    public void seek(long pos) throws IOException {
      this.reset();
      this.skip(pos);
    }

    @Override
    public long getPos() throws IOException {
      return pos;
    }

    @Override
    public boolean seekToNewSource(long targetPos) throws IOException {
      return false;
    }

    @Override
    public int read(long position, byte[] buffer, int offset, int length)
            throws IOException {
      long oldPos = getPos();
      int nread = -1;
      try {
        seek(position);
        nread = read(buffer, offset, length);
      } finally {
        seek(oldPos);
      }
      return nread;
    }

    @Override
    public void readFully(long position, byte[] buffer, int offset, int length)
            throws IOException {
      int nread = 0;
      while (nread < length) {
        int nbytes = read(position + nread, buffer, offset + nread, length - nread);
        if (nbytes < 0) {
          throw new EOFException("End of file reached before reading fully.");
        }
        nread += nbytes;
      }
    }

    @Override
    public void readFully(long position, byte[] buffer)
            throws IOException {
      readFully(position, buffer, 0, buffer.length);
    }
  }

  @Test
  public void testOrcTailStripeStats() throws Exception {
    Configuration conf = new Configuration();
    Path path = new Path(workDir, "orc_split_elim_new.orc");
    FileSystem fs = path.getFileSystem(conf);
    try (ReaderImpl reader = (ReaderImpl) OrcFile.createReader(path,
        OrcFile.readerOptions(conf).filesystem(fs))) {
      OrcTail tail = reader.extractFileTail(fs, path, Long.MAX_VALUE);
      List<StripeStatistics> stats = tail.getStripeStatistics();
      assertEquals(1, stats.size());
      OrcProto.TimestampStatistics tsStats =
          stats.get(0).getColumn(5).getTimestampStatistics();
      assertEquals(-28800000, tsStats.getMinimumUtc());
      assertEquals(-28550000, tsStats.getMaximumUtc());

      // Test Tail and Stats extraction from ByteBuffer
      ByteBuffer tailBuffer = tail.getSerializedTail();
      OrcTail extractedTail = ReaderImpl.extractFileTail(tailBuffer);

      assertEquals(tail.getTailBuffer(), extractedTail.getTailBuffer());
      assertEquals(tail.getTailBuffer().getData(), extractedTail.getTailBuffer().getData());
      assertEquals(tail.getTailBuffer().getOffset(), extractedTail.getTailBuffer().getOffset());
      assertEquals(tail.getTailBuffer().getEnd(), extractedTail.getTailBuffer().getEnd());

      assertEquals(tail.getMetadataOffset(), extractedTail.getMetadataOffset());
      assertEquals(tail.getMetadataSize(), extractedTail.getMetadataSize());

      Reader dummyReader = new ReaderImpl(null,
          OrcFile.readerOptions(OrcFile.readerOptions(conf).getConfiguration())
          .orcTail(extractedTail));
      List<StripeStatistics> tailBufferStats = dummyReader.getVariantStripeStatistics(null);

      assertEquals(stats.size(), tailBufferStats.size());
      OrcProto.TimestampStatistics bufferTsStats = tailBufferStats.get(0).getColumn(5).getTimestampStatistics();
      assertEquals(tsStats.getMinimumUtc(), bufferTsStats.getMinimumUtc());
      assertEquals(tsStats.getMaximumUtc(), bufferTsStats.getMaximumUtc());
    }
  }

  @Test
  public void testGetRawDataSizeFromColIndices() throws Exception {
    Configuration conf = new Configuration();
    Path path = new Path(workDir, "orc_split_elim_new.orc");
    FileSystem fs = path.getFileSystem(conf);
    try (ReaderImpl reader = (ReaderImpl) OrcFile.createReader(path,
        OrcFile.readerOptions(conf).filesystem(fs))) {
      TypeDescription schema = reader.getSchema();
      List<OrcProto.Type> types = OrcUtils.getOrcTypes(schema);
      boolean[] include = new boolean[schema.getMaximumId() + 1];
      List<Integer> list = new ArrayList<Integer>();
      for (int i = 0; i < include.length; i++) {
        include[i] = true;
        list.add(i);
      }
      List<OrcProto.ColumnStatistics> stats = reader.getFileTail().getFooter().getStatisticsList();
      assertEquals(
        ReaderImpl.getRawDataSizeFromColIndices(include, schema, stats),
        ReaderImpl.getRawDataSizeFromColIndices(list, types, stats));
    }
  }
}
