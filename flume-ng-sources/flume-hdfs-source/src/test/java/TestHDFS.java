import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.IOUtils;

import java.net.URI;
import java.net.URISyntaxException;

public class TestHDFS {
    public static void main(String[] args) throws Exception {

        Configuration conf = new Configuration();

        FileSystem fileSystem = FileSystem.get(new URI("hdfs://bj-rack001-hadoop002:8020"), conf, "hadoop");

        Path path = new Path("/tmp/zl/flume/data.log");


//        listFiles(fileSystem,path);



        FSDataInputStream inputStream = fileSystem.open(path);
        inputStream.seek(1000);
        IOUtils.copyBytes(inputStream,System.out,1024);

        inputStream.close();


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


}
