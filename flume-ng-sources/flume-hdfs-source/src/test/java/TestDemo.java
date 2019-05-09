import java.io.FileInputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectStreamConstants;
import java.util.HashMap;
import java.util.Map;

public class TestDemo {


    public static void main(String[] args) throws Exception {

        System.out.println(ObjectStreamConstants.STREAM_MAGIC);
        System.out.println(ObjectStreamConstants.STREAM_VERSION);

//        Map<String, String> map = loadMap("/todo/flume/hdfs/hdfsdir-source.conf");


    }


    public static Map<String, String> loadMap(String name) throws ClassNotFoundException, IOException {
        FileInputStream fileIn = new FileInputStream(name);
        Map map = new HashMap<>();
        try (ObjectInputStream in = new ObjectInputStream(fileIn)) {
            map = (HashMap) in.readObject();
        }
        System.out.println(map.toString());
        return map;
    }
}
