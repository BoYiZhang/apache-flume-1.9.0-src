import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.IOUtils;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

public class TestHDFS {
    public static void main(String[] args) throws Exception {

        Configuration conf = new Configuration();

        FileSystem fileSystem = FileSystem.get(new URI("hdfs://bj-rack001-hadoop002:8020"), conf, "hadoop");

        Path path = new Path("/tmp/zl/flume");








//        FileStatus fileStatus = fileSystem.getFileStatus(path) ;
//        System.out.println(fileStatus);


        path = new Path("hdfs://bj-rack001-hadoop002:8020/tmp/zl/flume/") ;


        pathFilter(fileSystem,path,"data.log");



//        listFiles(fileSystem,path);

//
//        FSDataInputStream inputStream = fileSystem.open(path);
//
//        inputStream.seek(1000);
//        IOUtils.copyBytes(inputStream,System.out,1024);
//
//        inputStream.close();


//        FileStatus [] fileStatus = fileSystem.listStatus(path);
//
//        System.out.println(fileStatus);
    }



    /**
     * 列出 hdfs 路径上的 文件
     * @param fileSystem
     * @param path
     * @throws Exception
     */
    public static void listFiles(FileSystem fileSystem ,  Path path ) throws Exception {

        RemoteIterator<LocatedFileStatus> list = fileSystem.listFiles(path, false);

        while(list.hasNext()){
            LocatedFileStatus locatedFileStatus = list.next();

            System.out.println(locatedFileStatus.getPath());
            System.out.println(locatedFileStatus.getOwner());

       }

    }


    /**
     * 匹配目录下的文件 支持正则表达式
     * @param fileSystem
     * @param path
     * @param filter
     * @throws IOException
     */
    public static void pathFilter(FileSystem fileSystem ,  Path path, String filter ) throws IOException {

//        PathFilter hdfsFilter =new RegexExcludePathFilter("^*.log$");

        PathFilter hdfsFilter =new RegexExcludePathFilter(filter);

        FileStatus [] array = fileSystem.listStatus(path, hdfsFilter);

        for(FileStatus fileStatus : array ){
            System.out.println(fileStatus.getPath());
        }

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