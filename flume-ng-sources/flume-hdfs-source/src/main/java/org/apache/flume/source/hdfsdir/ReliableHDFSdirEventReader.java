

package org.apache.flume.source.hdfsdir;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Table;
import com.google.gson.stream.JsonReader;
import org.apache.flume.Event;
import org.apache.flume.FlumeException;
import org.apache.flume.annotations.InterfaceAudience;
import org.apache.flume.annotations.InterfaceStability;
import org.apache.flume.client.avro.ReliableEventReader;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;


import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

@InterfaceAudience.Private
@InterfaceStability.Evolving
public class ReliableHDFSdirEventReader implements ReliableEventReader {
  private static final Logger logger = LoggerFactory.getLogger(ReliableHDFSdirEventReader.class);


  private  FileSystem fileSystem ;

  private final List<HDFSdirMatcher> hdfsdirCache;

  private final Table<String, String, String> headerTable;

  private HDFSFile currentFile = null;
  private Map<String, HDFSFile> hdfsFiles = Maps.newHashMap();
  private long updateTime;
  private boolean addByteOffset;
  private boolean cachePatternMatching;
  private boolean committed = true;
  private final boolean annotateFileName;
  private final String fileNameHeader;

  private String filePattern;

  /**
   * Create a ReliableHDFSdirEventReader to watch the given directory.
   */
  private ReliableHDFSdirEventReader(FileSystem fileSystem , Map<String, String> filePaths,String filePattern,
      Table<String, String, String> headerTable, String positionFilePath,
      boolean skipToEnd, boolean addByteOffset, boolean cachePatternMatching,
      boolean annotateFileName, String fileNameHeader) throws Exception {


    // Sanity checks
    Preconditions.checkNotNull(fileSystem);
    Preconditions.checkNotNull(filePaths);
    Preconditions.checkNotNull(positionFilePath);

    if (logger.isDebugEnabled()) {
      logger.debug("Initializing {} with directory={}, metaDir={}",
          new Object[] { ReliableHDFSdirEventReader.class.getSimpleName(), filePaths });
    }

    List<HDFSdirMatcher> hdfsdirCache = Lists.newArrayList();
    for (Entry<String, String> e : filePaths.entrySet()) {
      hdfsdirCache.add(new HDFSdirMatcher(fileSystem, e.getKey(), e.getValue(), filePattern, cachePatternMatching));
    }
    logger.info("hdfsdirCache: " + hdfsdirCache.toString());
    logger.info("headerTable: " + headerTable.toString());

    this.fileSystem = fileSystem;
    this.filePattern = filePattern ;
    this.hdfsdirCache = hdfsdirCache;
    this.headerTable = headerTable;
    this.addByteOffset = addByteOffset;
    this.cachePatternMatching = cachePatternMatching;
    this.annotateFileName = annotateFileName;
    this.fileNameHeader = fileNameHeader;

    //todo 更新 HDFS 文件
    updateHDFSFiles(skipToEnd);
    //todo 更新位置文件
    logger.info("Updating position from position file: " + positionFilePath);
    loadPositionFile(positionFilePath);
  }

  /**
   * Load a position file which has the last read position of each file.
   * If the position file exists, update hdfsFiles mapping.
   *
   * 加载具有每个文件的最后读取位置的位置文件。
   * 如果位置文件存在，更新hdfsFiles映射。
   *
   */
  public void loadPositionFile(String filePath) throws Exception {
    Long  pos;
    String path;

    //todo 验证索引日志文件大小
    File file = new File(filePath);
    if(file.length() == 0 ){
      logger.warn("索引日志文件大小为 0 ......");

      return;
    }

    FileReader fr = null;
    JsonReader jr = null;
    try {
      fr = new FileReader(filePath);
      jr = new JsonReader(fr);
      jr.beginArray();
      while (jr.hasNext()) {

        pos = null;
        path = null;
        jr.beginObject();
        while (jr.hasNext()) {
          switch (jr.nextName()) {
            case "path":
              path = jr.nextString();
              break;
            case "pos":
              pos = jr.nextLong();
              break;
            case "file":
              path = jr.nextString();
              break;
          }
        }
        jr.endObject();

        for (Object v : Arrays.asList( pos, path)) {
          Preconditions.checkNotNull(v, "Detected missing value in position file. "
              + "  pos: " + pos + ", path: " + path);
        }
        HDFSFile tf = hdfsFiles.get(path);
        if (tf != null && tf.updatePos(path, pos)) {
          hdfsFiles.put(path, tf);
        } else {
          logger.info("Missing file: " + path + ", pos: " + pos);
        }
      }
      jr.endArray();
    } catch (FileNotFoundException e) {
      logger.info("File not found: " + filePath + ", not updating position");
    } catch (Exception e) {
      logger.error("Failed loading positionFile: " + filePath, e);
    } finally {
      try {
        if (fr != null) fr.close();
        if (jr != null) jr.close();
      } catch (Exception e) {
        logger.error("Error: " + e.getMessage(), e);
      }
    }
  }

  public Map<String, HDFSFile> getHDFSFiles() {
    return hdfsFiles;
  }

  public void setCurrentFile(HDFSFile currentFile) {
    this.currentFile = currentFile;
  }

  @Override
  public Event readEvent() throws IOException {
    List<Event> events = readEvents(1);
    if (events.isEmpty()) {
      return null;
    }
    return events.get(0);
  }

  @Override
  public List<Event> readEvents(int numEvents) throws IOException {
    return readEvents(numEvents, false);
  }

  @VisibleForTesting
  public List<Event> readEvents(HDFSFile tf, int numEvents) throws IOException {
    setCurrentFile(tf);
    return readEvents(numEvents, true);
  }



  public List<Event> readEvents(int numEvents, boolean backoffWithoutNL)
      throws IOException {

    //todo 如果有未提交的任务,代表之前的任务失败了,进行回滚操作.
    if (!committed) {
      if (currentFile == null) {
        throw new IllegalStateException("current file does not exist. " + currentFile.getPath());
      }
      logger.info("Last read was never committed - resetting position");
      long lastPos = currentFile.getPos();
      currentFile.updateFilePos(lastPos);
    }

    //todo 读取数据
    List<Event> events = currentFile.readEvents(numEvents, backoffWithoutNL, addByteOffset);

    if (events.isEmpty()) {
      return events;
    }



    Map<String, String> headers = currentFile.getHeaders();

    //todo 是否添加头部信息
    if (annotateFileName || (headers != null && !headers.isEmpty())) {

      for (Event event : events) {
        if (headers != null && !headers.isEmpty()) {
          event.getHeaders().putAll(headers);
        }
        if (annotateFileName) {
          event.getHeaders().put(fileNameHeader, currentFile.getPath());
        }
      }


    }
    committed = false;
    return events;
  }

  @Override
  public void close() throws IOException {
    for (HDFSFile tf : hdfsFiles.values()) {
      if (tf.getRaf() != null) tf.getRaf().close();
    }
  }

  /** Commit the last lines which were read. */
  @Override
  public void commit() throws IOException {
    if (!committed && currentFile != null) {
      long pos = currentFile.getLineReadPos();
      currentFile.setPos(pos);
      currentFile.setLastUpdated(updateTime);
      committed = true;
    }
  }


  public List<String> updateHDFSFiles(boolean skipToEnd) throws Exception {
    updateTime = System.currentTimeMillis();
    List<String> updatedInodes = Lists.newArrayList();
    //todo     获取缓存中的 hdfsdir ,
    //todo     hdfsdir对象内容:   {filegroup='f1', filePattern='data.log', cached=true}
    for (HDFSdirMatcher hdfsdir : hdfsdirCache) {
      //todo    hdfsdir     :  {filegroup='f1', filePattern='data.log', cached=true}
      //todo    headerTable :  {f1={headerKey1=markHeaderKey}}
      Map<String, String> headers = headerTable.row(hdfsdir.getFileGroup());
      // todo 获取匹配文件,并将文件按最后修改时间进行排序
      for (FileStatus f : hdfsdir.getMatchingFiles()) {


        String  path = f.getPath().toString();


        HDFSFile tf = hdfsFiles.get(path);


        if (tf == null || !tf.getPath().equals(f.getPath().toString())) {

          //todo , 缓存中没有,或者路径不一样.  就认为是新建的数据.

          long startPos = skipToEnd ? f.getLen() : 0;

          //todo 读取文件获取 操作对象实例 HDFSFile
          tf = openFile(fileSystem, f.getPath().toString(), headers, startPos);


        } else {
          //todo , 缓存中存在, 判断 更新文件修改最后修改日期, 文件的大小是否有过变动.
          boolean updated = tf.getLastUpdated() < f.getModificationTime() || tf.getPos() != f.getLen();


          if (updated) {

            if (tf.getRaf() == null) {


              tf = openFile(fileSystem, f.getPath().toString(), headers, tf.getPos());


            }
            if (f.getLen() < tf.getPos()) {
              logger.info("Pos " + tf.getPos() + " is larger than file size! "
                  + "Restarting from pos 0, file: " + tf.getPath() );

              tf.updatePos(tf.getPath(), 0);


            }

          }


          tf.setNeedHDFS(updated);


        }

        //todo 更新文件
        hdfsFiles.put(path, tf);


        updatedInodes.add(path);


      }
    }
    return updatedInodes;
  }

  public List<String> updateHDFSFiles() throws Exception {
    return updateHDFSFiles(false);
  }

  //todo  方法根据日志文件对象，headers，inode和偏移量pos创建一个HDFSFile对象
  private HDFSFile openFile(FileSystem fileSystem , String path, Map<String, String> headers, long pos) {
    try {
      logger.info("Opening path: " + path + ", pos: " + pos);

      return new HDFSFile(fileSystem,path, headers, pos);
    } catch (Exception e) {
      throw new FlumeException("Failed opening file path : " + path, e);
    }
  }

  /**
   * Special builder class for ReliableHDFSdirEventReader
   */
  public static class Builder {

    private FileSystem fileSystem ;
    private Map<String, String> filePaths;
    private String filePattern ;
    private Table<String, String, String> headerTable;
    private String positionFilePath;
    private boolean skipToEnd;
    private boolean addByteOffset;
    private boolean cachePatternMatching;
    private Boolean annotateFileName =
            HDFSdirSourceConfigurationConstants.DEFAULT_FILE_HEADER;
    private String fileNameHeader =
            HDFSdirSourceConfigurationConstants.DEFAULT_FILENAME_HEADER_KEY;

    public Builder fileSystem(FileSystem fileSystem) {
      this.fileSystem = fileSystem;
      return this;
    }



    public Builder filePaths(Map<String, String> filePaths) {
      this.filePaths = filePaths;
      return this;
    }

    public Builder filePattern(String filePattern) {
      this.filePattern = filePattern;
      return this;
    }

    public Builder headerTable(Table<String, String, String> headerTable) {
      this.headerTable = headerTable;
      return this;
    }

    public Builder positionFilePath(String positionFilePath) {
      this.positionFilePath = positionFilePath;
      return this;
    }

    public Builder skipToEnd(boolean skipToEnd) {
      this.skipToEnd = skipToEnd;
      return this;
    }

    public Builder addByteOffset(boolean addByteOffset) {
      this.addByteOffset = addByteOffset;
      return this;
    }

    public Builder cachePatternMatching(boolean cachePatternMatching) {
      this.cachePatternMatching = cachePatternMatching;
      return this;
    }

    public Builder annotateFileName(boolean annotateFileName) {
      this.annotateFileName = annotateFileName;
      return this;
    }

    public Builder fileNameHeader(String fileNameHeader) {
      this.fileNameHeader = fileNameHeader;
      return this;
    }

    public ReliableHDFSdirEventReader build() throws Exception {
      return new ReliableHDFSdirEventReader(fileSystem, filePaths,filePattern, headerTable, positionFilePath, skipToEnd,
                                            addByteOffset, cachePatternMatching,
                                            annotateFileName, fileNameHeader);
    }
  }

}
