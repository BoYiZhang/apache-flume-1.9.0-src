/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.flume.source.hdfsdir;

import com.google.common.collect.Lists;
import org.apache.flume.Event;
import org.apache.flume.event.EventBuilder;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

import java.util.List;
import java.util.Map;

import static org.apache.flume.source.hdfsdir.HDFSdirSourceConfigurationConstants.BYTE_OFFSET_HEADER_KEY;


/**
 * todo HDFSdirSource通过HDFSFile类操作处理每个日志文件，包含了FSDataInputStream类，
 *  以及记录日志文件偏移量pos,最新更新时间lastUpdated等属性
 *  FSDataInputStream 完美的符合HDFSdirSource的应用场景，FSDataInputStream支持使用seek()方法随机访问文件，
 *  配合position file中记录的日志文件读取偏移量，能够轻松简单的seek到文件偏移量，
 *  然后向后读取日志内容，并重新将新的偏移量记录到position file中。
 */
public class HDFSFile {
  private static final Logger logger = LoggerFactory.getLogger(HDFSFile.class);

  private static final byte BYTE_NL = (byte) 10;
  private static final byte BYTE_CR = (byte) 13;

  private static final int BUFFER_SIZE = 8192;
  private static final int NEED_READING = -1;

  private FSDataInputStream raf;

  private final String path;
  private long pos;
  private long lastUpdated;
  private boolean needHDFS;
  private final Map<String, String> headers;
  private byte[] buffer;
  private byte[] oldBuffer;
  private int bufferPos;
  private long lineReadPos;

  private long length;

  public HDFSFile(FileSystem fileSystem , String path, Map<String, String> headers , long pos)  throws Exception {

    Path hdfsPath = new Path(path) ;

    this.raf = (FSDataInputStream) fileSystem.open(hdfsPath);

    FileStatus fileStatus = fileSystem.listStatus(hdfsPath)[0];

    this.length = fileStatus.getLen();

    if (pos > 0) {
      raf.seek(pos);
      lineReadPos = pos;
    }
    this.path = path;
    this.pos = pos;
    this.lastUpdated = 0L;
    this.needHDFS = true;
    this.headers = headers;
    this.oldBuffer = new byte[0];
    this.bufferPos = NEED_READING;
  }

  public FSDataInputStream getRaf() {
    return raf;
  }

  public String getPath() {
    return path;
  }

  public long getPos() {
    return pos;
  }

  public long getLastUpdated() {
    return lastUpdated;
  }

  public boolean needHDFS() {
    return needHDFS;
  }

  public Map<String, String> getHeaders() {
    return headers;
  }

  public long getLineReadPos() {
    return lineReadPos;
  }

  public void setPos(long pos) {
    this.pos = pos;
  }

  public void setLastUpdated(long lastUpdated) {
    this.lastUpdated = lastUpdated;
  }

  public void setNeedHDFS(boolean needHDFS) {
    this.needHDFS = needHDFS;
  }

  public void setLineReadPos(long lineReadPos) {
    this.lineReadPos = lineReadPos;
  }

  public boolean updatePos(String path, long pos) throws IOException {
    if (this.path.equals(path)) {
      setPos(pos);
      updateFilePos(pos);
      logger.info("Updated position, file: " + path +  ", pos: " + pos);
      return true;
    }
    return false;
  }
  public void updateFilePos(long pos) throws IOException {
    raf.seek(pos);
    lineReadPos = pos;
    bufferPos = NEED_READING;
    oldBuffer = new byte[0];
  }


  public List<Event> readEvents(int numEvents, boolean backoffWithoutNL,
      boolean addByteOffset) throws IOException {

    List<Event> events = Lists.newLinkedList();


    for (int i = 0; i < numEvents; i++) {

      //todo 读取数据 ,并转换为 Event
      Event event = readEvent(backoffWithoutNL, addByteOffset);


      if (event == null) {
        break;
      }

      events.add(event);


    }


    return events;
  }



  private Event readEvent(boolean backoffWithoutNL, boolean addByteOffset) throws IOException {

    Long posTmp = getLineReadPos();

    //todo 按字节读取数据, 然后根据换行符  BYTE_NL =  10 , 截取一行数据, 返回二进制数据.
    LineResult line = readLine();

    if (line == null) {
      return null;
    }

    //todo 过滤掉已经读取的数据.
    if (backoffWithoutNL && !line.lineSepInclude) {
      logger.info("Backing off in file without newline: "
          + path + " , pos: " + raf.getPos());

      updateFilePos(posTmp);

      return null;
    }

    System.out.println("line: " + new String(line.line));
    Event event = EventBuilder.withBody(line.line);

    // todo 是否要增加偏移量
    if (addByteOffset == true) {

      event.getHeaders().put(BYTE_OFFSET_HEADER_KEY, posTmp.toString());

    }
    return event;

  }



  //todo  readFile()按BUFFER_SIZE（默认8KB）作为缓冲读取日志文件数据

  private void readFile() throws IOException {
    //todo 如果 数据长度小于缓冲, 则根据文件容量创建缓冲区


    if ((this.length - raf.getPos()) < BUFFER_SIZE) {

      buffer = new byte[(int) (this.length - raf.getPos())];

    } else {
    //todo 如果 数据长度大于缓冲, 折直接创建缓冲区 8kb
      buffer = new byte[BUFFER_SIZE];

    }

    raf.read(buffer, 0, buffer.length);

    bufferPos = 0;

  }

  private byte[] concatByteArrays(byte[] a, int startIdxA, int lenA,
                                  byte[] b, int startIdxB, int lenB) {
    byte[] c = new byte[lenA + lenB];
    System.arraycopy(a, startIdxA, c, 0, lenA);
    System.arraycopy(b, startIdxB, c, lenA, lenB);
    return c;
  }



  public LineResult readLine() throws IOException {

    LineResult lineResult = null;

    while (true) {

      if (bufferPos == NEED_READING) {

        //todo 当文件指针位置小于文件总长度的时候，就需要读取指针位置到文件最后的数据
        if (raf.getPos() < this.length) {

          readFile();

        } else {

          if (oldBuffer.length > 0) {
            lineResult = new LineResult(false, oldBuffer);
            oldBuffer = new byte[0];


            setLineReadPos(lineReadPos + lineResult.line.length);

          }

          break;
        }
      }


      for (int i = bufferPos; i < buffer.length; i++) {

        if (buffer[i] == BYTE_NL) {

          int oldLen = oldBuffer.length;
          // Don't copy last byte(NEW_LINE)
          int lineLen = i - bufferPos;


          // For windows, check for CR
          if (i > 0 && buffer[i - 1] == BYTE_CR) {

            lineLen -= 1;

          } else if (oldBuffer.length > 0 && oldBuffer[oldBuffer.length - 1] == BYTE_CR) {

            oldLen -= 1;

          }


          lineResult = new LineResult(true,
              concatByteArrays(oldBuffer, 0, oldLen, buffer, bufferPos, lineLen));


          setLineReadPos(lineReadPos + (oldBuffer.length + (i - bufferPos + 1)));


          oldBuffer = new byte[0];


          if (i + 1 < buffer.length) {

            bufferPos = i + 1;

          } else {

            bufferPos = NEED_READING;

          }

          break;


        }

      }


      if (lineResult != null) {
        break;
      }

      // NEW_LINE not showed up at the end of the buffer

      oldBuffer = concatByteArrays(oldBuffer, 0, oldBuffer.length,
                                   buffer, bufferPos, buffer.length - bufferPos);


      bufferPos = NEED_READING;

    }

    return lineResult;

  }

  public void close() {
    try {
      raf.close();
      raf = null;
      long now = System.currentTimeMillis();
      setLastUpdated(now);
    } catch (IOException e) {
      logger.error("Failed closing file: " + path , e);
    }
  }

  private class LineResult {
    final boolean lineSepInclude;
    final byte[] line;

    public LineResult(boolean lineSepInclude, byte[] line) {
      super();
      this.lineSepInclude = lineSepInclude;
      this.line = line;
    }
  }
}
