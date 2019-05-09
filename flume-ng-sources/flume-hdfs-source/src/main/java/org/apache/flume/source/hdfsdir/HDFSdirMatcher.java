
package org.apache.flume.source.hdfsdir;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import org.apache.flume.annotations.InterfaceAudience;
import org.apache.flume.annotations.InterfaceStability;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileNotFoundException;
import java.util.*;
import java.util.concurrent.TimeUnit;


@InterfaceAudience.Private
@InterfaceStability.Evolving
public class HDFSdirMatcher {
  private static final Logger logger = LoggerFactory.getLogger(HDFSdirMatcher.class);

  private  FileSystem fileSystem = null ;

  // flag from configuration to switch off caching completely
  //todo 从配置标志完全关闭缓存
  private final boolean cachePatternMatching;

  // id from configuration
  // todo 配置 id
  private final String fileGroup;

  // plain string of the desired files from configuration
  //todo 配置中所需文件的纯字符串
  private final String filePattern;

  // directory monitored for changes
  // todo 监控目录的变化
  private final FileStatus parentDir;

  // cached instance for filtering files based on filePattern
  //todo 用于根据filePattern过滤文件的缓存实例
  private final List<FileStatus> fileFilterList;


  private long lastSeenParentDirMTime = -1;

  private long lastCheckedTime = -1;


  private List<FileStatus> lastMatchedFiles = Lists.newArrayList();


  HDFSdirMatcher(FileSystem fileSystem, String fileGroup, String path,String filePattern, boolean cachePatternMatching) throws Exception {
    // store whatever came from configuration
    this.fileGroup = fileGroup;
    this.filePattern = filePattern;
    this.cachePatternMatching = cachePatternMatching;
    this.fileSystem = fileSystem;

    //todo 首先验证传入的文件是目录还是文件
    Path hdfsPath = new Path(path) ;


    if(fileSystem.exists(hdfsPath)){


      FileStatus fileStatus = fileSystem.getFileStatus(new Path(path));

      Preconditions.checkNotNull(fileStatus);

      if(fileStatus.isDirectory()){
        this.parentDir = fileStatus;

        PathFilter hdfsFilter =new RegexExcludePathFilter(filePattern);

        FileStatus [] fileStatuses = fileSystem.listStatus(hdfsPath, hdfsFilter);

        this.fileFilterList = Arrays.asList(fileStatus);

      }else{
        throw new Exception("输入的监控目录不是文件目录......");
      }


    }else{
      throw new FileNotFoundException();
    }


  }


  List<FileStatus> getMatchingFiles() throws Exception {
    long now = TimeUnit.SECONDS.toMillis(
        TimeUnit.MILLISECONDS.toSeconds(System.currentTimeMillis()));
    long currentParentDirMTime = parentDir.getModificationTime();


    if (!cachePatternMatching ||
        lastSeenParentDirMTime < currentParentDirMTime ||
        !(currentParentDirMTime < lastCheckedTime)) {
      lastMatchedFiles = sortByLastModifiedTime(getMatchingFilesNoCache());
      lastSeenParentDirMTime = currentParentDirMTime;
      lastCheckedTime = now;
    }

    return lastMatchedFiles;
  }

  private List<FileStatus> getMatchingFilesNoCache() throws Exception {
    List<FileStatus> result = Lists.newArrayList();

    FileStatus[]  fileStatuses = fileSystem.listStatus(this.parentDir.getPath(),new RegexExcludePathFilter(this.filePattern));

    if(null != fileStatuses && fileStatuses.length > 0 ){
      result = Arrays.asList(fileStatuses);
    }

    return result;
  }


  private static List<FileStatus> sortByLastModifiedTime(List<FileStatus> files) {
    final HashMap<FileStatus, Long> lastModificationTimes = new HashMap<FileStatus, Long>(files.size());
    for (FileStatus f: files) {
      lastModificationTimes.put(f, f.getModificationTime());
    }
    Collections.sort(files, new Comparator<FileStatus>() {
      @Override
      public int compare(FileStatus o1, FileStatus o2) {
        return lastModificationTimes.get(o1).compareTo(lastModificationTimes.get(o2));
      }
    });

    return files;
  }

  @Override
  public String toString() {
    return "{" +
        "filegroup='" + fileGroup + '\'' +
        ", filePattern='" + filePattern + '\'' +
        ", cached=" + cachePatternMatching +
        '}';
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;

    HDFSdirMatcher that = (HDFSdirMatcher) o;

    return fileGroup.equals(that.fileGroup);

  }

  @Override
  public int hashCode() {
    return fileGroup.hashCode();
  }

  public String getFileGroup() {
    return fileGroup;
  }

}


class RegexExcludePathFilter implements PathFilter {
  private final String regex;
  public RegexExcludePathFilter(String regex) {
    this.regex = regex;
  }
  public boolean accept(Path path) {
    if(null == regex){
      return  true;
    }
    return path.getName().matches(regex);
  }
}