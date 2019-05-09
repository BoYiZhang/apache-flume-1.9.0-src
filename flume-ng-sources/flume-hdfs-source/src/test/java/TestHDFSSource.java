import org.apache.flume.Channel;
import org.apache.flume.ChannelSelector;
import org.apache.flume.Context;
import org.apache.flume.channel.ChannelProcessor;
import org.apache.flume.channel.MemoryChannel;
import org.apache.flume.channel.ReplicatingChannelSelector;
import org.apache.flume.conf.Configurables;
import org.apache.flume.source.hdfsdir.HDFSdirSource;
import org.junit.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class TestHDFSSource {


    static HDFSdirSource source;
    static MemoryChannel channel;



    @Test
    public void testRun() throws Exception {
        source = new HDFSdirSource();

        channel = new MemoryChannel();

        Map<String, String> parameters = new HashMap<>();
        parameters.put("positionFile","/todo/flume/hdfs/hdfsdir_position.json") ;

        parameters.put("channels","c1") ;
        parameters.put("filegroups.f1","/tmp/zl/flume") ;
        parameters.put("filegroups","f1") ;
        parameters.put("fileHeader","true") ;
        parameters.put("type","TAILDIR") ;
        parameters.put("headers.f1.headerKey1","markHeaderKey") ;


        parameters.put("hdfsURI","hdfs://bj-rack001-hadoop002:8020") ;
        parameters.put("hdfsUser","hadoop") ;
        parameters.put("filePattern","data.log") ;



        Configurables.configure(channel, new Context());

        List<Channel> channels = new ArrayList<Channel>();
        channels.add(channel);

        ChannelSelector rcs = new ReplicatingChannelSelector();
        rcs.setChannels(channels);

        source.setChannelProcessor(new ChannelProcessor(rcs));
        source.setName("测试 HDFS source ........");

        source.configure(new Context(parameters));

        source.start();


        while (true) {
            source.process();

            Thread.sleep(source.getMaxBackOffSleepInterval());
        }






    }



}
