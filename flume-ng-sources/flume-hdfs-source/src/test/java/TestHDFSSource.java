import org.apache.flume.Channel;
import org.apache.flume.ChannelSelector;
import org.apache.flume.Context;
import org.apache.flume.channel.ChannelProcessor;
import org.apache.flume.channel.MemoryChannel;
import org.apache.flume.channel.ReplicatingChannelSelector;
import org.apache.flume.conf.Configurables;
import org.junit.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class TestHDFSSource {


    static HDFSSource source;
    static MemoryChannel channel;



    @Test
    public void testRun() throws Exception {
        source = new HDFSSource();

        channel = new MemoryChannel();

        Map<String, String> parameters = new HashMap<>();
        parameters.put("positionFile","/todo/flume/hdfs/taildir_position.json") ;

        parameters.put("channels","c1") ;
        parameters.put("filegroups.f1","/todo/flume/hdfs/input/data.log") ;
        parameters.put("filegroups","f1") ;
        parameters.put("fileHeader","true") ;
        parameters.put("type","TAILDIR") ;
        parameters.put("headers.f1.headerKey1","markHeaderKey") ;



        Configurables.configure(channel, new Context());

        List<Channel> channels = new ArrayList<Channel>();
        channels.add(channel);

        ChannelSelector rcs = new ReplicatingChannelSelector();
        rcs.setChannels(channels);

        source.setChannelProcessor(new ChannelProcessor(rcs));


        source.configure(new Context(parameters));

        source.start();


        source.process();


        Thread.sleep(1000000);


    }



}
