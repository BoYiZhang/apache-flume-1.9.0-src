/**
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
package org.apache.flume.node;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.Set;

import org.apache.flume.Channel;
import org.apache.flume.ChannelFactory;
import org.apache.flume.ChannelSelector;
import org.apache.flume.Context;
import org.apache.flume.FlumeException;
import org.apache.flume.Sink;
import org.apache.flume.SinkFactory;
import org.apache.flume.SinkProcessor;
import org.apache.flume.SinkRunner;
import org.apache.flume.Source;
import org.apache.flume.SourceFactory;
import org.apache.flume.SourceRunner;
import org.apache.flume.annotations.Disposable;
import org.apache.flume.channel.ChannelProcessor;
import org.apache.flume.channel.ChannelSelectorFactory;
import org.apache.flume.channel.DefaultChannelFactory;
import org.apache.flume.channel.MemoryChannel;
import org.apache.flume.conf.*;
import org.apache.flume.conf.FlumeConfiguration.AgentConfiguration;
import org.apache.flume.conf.channel.ChannelSelectorConfiguration;
import org.apache.flume.conf.sink.SinkConfiguration;
import org.apache.flume.conf.sink.SinkGroupConfiguration;
import org.apache.flume.conf.source.SourceConfiguration;
import org.apache.flume.sink.DefaultSinkFactory;
import org.apache.flume.sink.DefaultSinkProcessor;
import org.apache.flume.sink.SinkGroup;
import org.apache.flume.source.DefaultSourceFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;
import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.ListMultimap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

public abstract class AbstractConfigurationProvider implements ConfigurationProvider {

  private static final Logger LOGGER = LoggerFactory.getLogger(AbstractConfigurationProvider.class);

  private final String agentName;
  private final SourceFactory sourceFactory;
  private final SinkFactory sinkFactory;
  private final ChannelFactory channelFactory;

  private final Map<Class<? extends Channel>, Map<String, Channel>> channelCache;

  public AbstractConfigurationProvider(String agentName) {
    super();
    this.agentName = agentName;
    this.sourceFactory = new DefaultSourceFactory();
    this.sinkFactory = new DefaultSinkFactory();
    this.channelFactory = new DefaultChannelFactory();

    channelCache = new HashMap<Class<? extends Channel>, Map<String, Channel>>();
  }

  protected abstract FlumeConfiguration getFlumeConfiguration();

  public MaterializedConfiguration getConfiguration() {

    //  todo SimpleMaterializedConfiguration包含了source、channel、sink三者的map对象
    //  todo private final Map<String, Channel> channels;
    //  todo private final Map<String, SourceRunner> sourceRunners;
    //  todo private final Map<String, SinkRunner> sinkRunners;


    MaterializedConfiguration conf = new SimpleMaterializedConfiguration();

    // todo 读取配置文件 org.apache.flume.node.PropertiesFileConfigurationProvider#getFlumeConfiguration
    FlumeConfiguration fconfig = getFlumeConfiguration();


    // todo 创建 Agent
    AgentConfiguration agentConf = fconfig.getConfigurationFor(getAgentName());

    if (agentConf != null) {


      Map<String, ChannelComponent> channelComponentMap = Maps.newHashMap();
      Map<String, SourceRunner> sourceRunnerMap = Maps.newHashMap();
      Map<String, SinkRunner> sinkRunnerMap = Maps.newHashMap();


      try {
        //todo 加载 Channels, Sources  , Sinks

        loadChannels(agentConf, channelComponentMap);
        loadSources(agentConf, channelComponentMap, sourceRunnerMap);
        loadSinks(agentConf, channelComponentMap, sinkRunnerMap);

        Set<String> channelNames = new HashSet<String>(channelComponentMap.keySet());
        for (String channelName : channelNames) {

          ChannelComponent channelComponent = channelComponentMap.get(channelName);

          if (channelComponent.components.isEmpty()) {
            LOGGER.warn(String.format("Channel %s has no components connected" +
                " and has been removed.", channelName));
            channelComponentMap.remove(channelName);
            Map<String, Channel> nameChannelMap =
                channelCache.get(channelComponent.channel.getClass());
            if (nameChannelMap != null) {
              nameChannelMap.remove(channelName);
            }
          } else {

            LOGGER.info(String.format("Channel %s connected to %s",
                channelName, channelComponent.components.toString()));

            //todo 添加 channel
            conf.addChannel(channelName, channelComponent.channel);

          }

        }

        //todo 添加 source
        for (Map.Entry<String, SourceRunner> entry : sourceRunnerMap.entrySet()) {
          conf.addSourceRunner(entry.getKey(), entry.getValue());
        }

        //todo 添加 sink
        for (Map.Entry<String, SinkRunner> entry : sinkRunnerMap.entrySet()) {
          conf.addSinkRunner(entry.getKey(), entry.getValue());
        }
      } catch (InstantiationException ex) {
        LOGGER.error("Failed to instantiate component", ex);
      } finally {
        channelComponentMap.clear();
        sourceRunnerMap.clear();
        sinkRunnerMap.clear();
      }
    } else {
      LOGGER.warn("No configuration found for this host:{}", getAgentName());
    }
    return conf;
  }

  public String getAgentName() {
    return agentName;
  }



  /*
   * Some channels will be reused across re-configurations. To handle this,
   * we store all the names of current channels, perform the reconfiguration,
   * and then if a channel was not used, we delete our reference to it.
   * This supports the scenario where you enable channel "ch0" then remove it
   * and add it back. Without this, channels like memory channel would cause
   * the first instances data to show up in the seconds.
   *
   * 某些频道将在重新配置中重复使用。 为了处理这个问题，
   * 我们存储当前频道的所有名称，执行重新配置，然后如果没有使用频道，我们删除对它的引用。
   * 这支持启用通道“ch0”然后将其删除并添加回来的情况。
   * 如果没有这个，像内存通道这样的通道会导致第一个实例数据在几秒钟内显示出来。
   *
   */

  private void loadChannels(AgentConfiguration agentConf,
      Map<String, ChannelComponent> channelComponentMap)
          throws InstantiationException {
    LOGGER.info("Creating channels");

    //todo 缓存中的 channel
    ListMultimap<Class<? extends Channel>, String> channelsNotReused =
        ArrayListMultimap.create();
    // assume all channels will not be re-used
    for (Map.Entry<Class<? extends Channel>, Map<String, Channel>> entry :
         channelCache.entrySet()) {
      Class<? extends Channel> channelKlass = entry.getKey();
      Set<String> channelNames = entry.getValue().keySet();
      channelsNotReused.get(channelKlass).addAll(channelNames);
    }

    Set<String> channelNames = agentConf.getChannelSet();
    Map<String, ComponentConfiguration> compMap = agentConf.getChannelConfigMap();


    for (String chName : channelNames) {
      ComponentConfiguration comp = compMap.get(chName);
      if (comp != null) {

        // todo 使用工厂类创建Channel
        Channel channel = getOrCreateChannel(channelsNotReused,
            comp.getComponentName(), comp.getType());

        try {

          //todo 更新配置 , 因为 channelComponentMap 刚开始传进来的时候是空值
          boolean isConf = Configurables.configure(channel, comp);
          if(!isConf){

            if (channel instanceof MemoryChannel) {


              ((MemoryChannel) channel).configure(new Context());
            }
          }

          channelComponentMap.put(comp.getComponentName(),
              new ChannelComponent(channel));
          LOGGER.info("Created channel " + chName);


        } catch (Exception e) {
          String msg = String.format("Channel %s has been removed due to an " +
              "error during configuration", chName);
          LOGGER.error(msg, e);
        }
      }
    }



    //todo 组合没有 ComponentConfiguration配置, 仅仅使用Context的对象.
    for (String chName : channelNames) {
      Context context = agentConf.getChannelContext().get(chName);
      if (context != null) {
        // todo 使用工厂类创建Channel
        Channel channel = getOrCreateChannel(channelsNotReused, chName,
            context.getString(BasicConfigurationConstants.CONFIG_TYPE));

        try {

          // todo 更新配置 , 因为 channelComponentMap 刚开始传进来的时候是空值
          Configurables.configure(channel, context);
          channelComponentMap.put(chName, new ChannelComponent(channel));

          LOGGER.info("Created channel " + chName);
        } catch (Exception e) {
          String msg = String.format("Channel %s has been removed due to an " +
              "error during configuration", chName);
          LOGGER.error(msg, e);
        }
      }
    }

    for (Class<? extends Channel> channelKlass : channelsNotReused.keySet()) {
      Map<String, Channel> channelMap = channelCache.get(channelKlass);
      if (channelMap != null) {
        for (String channelName : channelsNotReused.get(channelKlass)) {
          if (channelMap.remove(channelName) != null) {
            LOGGER.info("Removed {} of type {}", channelName, channelKlass);
          }
        }
        if (channelMap.isEmpty()) {
          //todo 有一些 channel 在配置中没有重新使用, 将会将其从缓存中移除.
          channelCache.remove(channelKlass);

        }
      }
    }
  }

  private Channel getOrCreateChannel(
      ListMultimap<Class<? extends Channel>, String> channelsNotReused,
      String name, String type)
      throws FlumeException {

    // todo 根据传入的类型, 获取对应的类
    Class<? extends Channel> channelClass = channelFactory.getClass(type);

    /*
     * Channel has requested a new instance on each re-configuration
     * todo 根据新的配置, 实例化对象.
     */
    //todo 如何类的注解 Disposable 存在, 则直接进行实例化,并返回 只有 jdbc 和 file 模式用到了
    if (channelClass.isAnnotationPresent(Disposable.class)) {
      Channel channel = channelFactory.create(name, type);
      channel.setName(name);
      return channel;
    }

    Map<String, Channel> channelMap = channelCache.get(channelClass);

    //todo 如果缓存中不存在 channel 的话, 那么直接加入缓存.
    if (channelMap == null) {
      channelMap = new HashMap<String, Channel>();
      channelCache.put(channelClass, channelMap);
    }
    //todo 如果channelMap 中的 channel 为 null ,使用工厂类创建.
    Channel channel = channelMap.get(name);
    if (channel == null) {
      channel = channelFactory.create(name, type);
      channel.setName(name);
      channelMap.put(name, channel);
    }
    //todo 如果缓存中已经存在对应的 channel 的话,那么移除它, 后续的方法会更新它 .
    channelsNotReused.get(channelClass).remove(name);
    return channel;
  }


  private void loadSources(AgentConfiguration agentConf,
      Map<String, ChannelComponent> channelComponentMap,
      Map<String, SourceRunner> sourceRunnerMap)
      throws InstantiationException {

    Set<String> sourceNames = agentConf.getSourceSet();
    Map<String, ComponentConfiguration> compMap =
        agentConf.getSourceConfigMap();
    /*
     * Components which have a ComponentConfiguration object
     *
     * todo 组合配置ComponentConfiguration 的对象
     */
    for (String sourceName : sourceNames) {
      ComponentConfiguration comp = compMap.get(sourceName);
      if (comp != null) {

        SourceConfiguration config = (SourceConfiguration) comp;

        //todo 使用sourceFactory 直接采用根据类型,采用反射方式  实例化 source ,
        Source source = sourceFactory.create(comp.getComponentName(),
            comp.getType());

        try {

          //todo 为 source 匹配对应的 channel
          Configurables.configure(source, config);
          Set<String> channelNames = config.getChannels();


          List<Channel> sourceChannels =
                  getSourceChannels(channelComponentMap, source, channelNames);

          if (sourceChannels.isEmpty()) {
            String msg = String.format("Source %s is not connected to a " +
                "channel",  sourceName);
            throw new IllegalStateException(msg);
          }


          //todo  创建 selector .
          ChannelSelectorConfiguration selectorConfig =
              config.getSelectorConfiguration();


          ChannelSelector selector = ChannelSelectorFactory.create(
              sourceChannels, selectorConfig);

          ChannelProcessor channelProcessor = new ChannelProcessor(selector);
          Configurables.configure(channelProcessor, config);


          source.setChannelProcessor(channelProcessor);



          //todo 将 source 转换为 SourceRunner  !!!!!!!!!!!!!!!1
          sourceRunnerMap.put(comp.getComponentName(),
              SourceRunner.forSource(source) );

          //todo source 向 channel 反向注册 sourceName
          for (Channel channel : sourceChannels) {
            ChannelComponent channelComponent =
                Preconditions.checkNotNull(channelComponentMap.get(channel.getName()),
                                           String.format("Channel %s", channel.getName()));
            channelComponent.components.add(sourceName);

          }
        } catch (Exception e) {
          String msg = String.format("Source %s has been removed due to an " +
              "error during configuration", sourceName);
          LOGGER.error(msg, e);
        }
      }
    }
    /*
     * Components which DO NOT have a ComponentConfiguration object
     * and use only Context
     *
     * todo 组合没有配置 ComponentConfiguration 但是使用 context  的对象
     */
    Map<String, Context> sourceContexts = agentConf.getSourceContext();
    for (String sourceName : sourceNames) {
      Context context = sourceContexts.get(sourceName);
      if (context != null) {

        //todo 使用sourceFactory 实例化 source
        Source source =
            sourceFactory.create(sourceName,
                                 context.getString(BasicConfigurationConstants.CONFIG_TYPE));


        try {
          Configurables.configure(source, context);
          String[] channelNames = context.getString(
              BasicConfigurationConstants.CONFIG_CHANNELS).split("\\s+");

          //todo 为 source 匹配对应的 channel
          List<Channel> sourceChannels =
                  getSourceChannels(channelComponentMap, source, Arrays.asList(channelNames));


          if (sourceChannels.isEmpty()) {
            String msg = String.format("Source %s is not connected to a " +
                "channel",  sourceName);
            throw new IllegalStateException(msg);
          }

          //todo  创建 selector .
          Map<String, String> selectorConfig = context.getSubProperties(
              BasicConfigurationConstants.CONFIG_SOURCE_CHANNELSELECTOR_PREFIX);

          ChannelSelector selector = ChannelSelectorFactory.create(
              sourceChannels, selectorConfig);

          ChannelProcessor channelProcessor = new ChannelProcessor(selector);
          Configurables.configure(channelProcessor, context);
          source.setChannelProcessor(channelProcessor);


          //todo 将 source 转换为 SourceRunner  !!!!!!!!!!!!!!!1
          sourceRunnerMap.put(sourceName,
              SourceRunner.forSource(source));

          //todo source 向 channel 反向注册 sourceName
          for (Channel channel : sourceChannels) {
            ChannelComponent channelComponent =
                Preconditions.checkNotNull(channelComponentMap.get(channel.getName()),
                                           String.format("Channel %s", channel.getName()));
            channelComponent.components.add(sourceName);
          }
        } catch (Exception e) {
          String msg = String.format("Source %s has been removed due to an " +
              "error during configuration", sourceName);
          LOGGER.error(msg, e);
        }
      }
    }
  }

  private List<Channel> getSourceChannels(Map<String, ChannelComponent> channelComponentMap,
                  Source source, Collection<String> channelNames) throws InstantiationException {
    List<Channel> sourceChannels = new ArrayList<Channel>();
    for (String chName : channelNames) {
      ChannelComponent channelComponent = channelComponentMap.get(chName);
      if (channelComponent != null) {
        checkSourceChannelCompatibility(source, channelComponent.channel);
        sourceChannels.add(channelComponent.channel);
      }
    }
    return sourceChannels;
  }

  private void checkSourceChannelCompatibility(Source source, Channel channel)
      throws InstantiationException {
    if (source instanceof BatchSizeSupported && channel instanceof TransactionCapacitySupported) {
      long transCap = ((TransactionCapacitySupported) channel).getTransactionCapacity();
      long batchSize = ((BatchSizeSupported) source).getBatchSize();
      if (transCap < batchSize) {
        String msg = String.format(
            "Incompatible source and channel settings defined. " +
                "source's batch size is greater than the channels transaction capacity. " +
                "Source: %s, batch size = %d, channel %s, transaction capacity = %d",
            source.getName(), batchSize,
            channel.getName(), transCap);
        throw new InstantiationException(msg);
      }
    }
  }

  private void checkSinkChannelCompatibility(Sink sink, Channel channel)
      throws InstantiationException {
    if (sink instanceof BatchSizeSupported && channel instanceof TransactionCapacitySupported) {
      long transCap = ((TransactionCapacitySupported) channel).getTransactionCapacity();
      long batchSize = ((BatchSizeSupported) sink).getBatchSize();
      if (transCap < batchSize) {
        String msg = String.format(
            "Incompatible sink and channel settings defined. " +
                "sink's batch size is greater than the channels transaction capacity. " +
                "Sink: %s, batch size = %d, channel %s, transaction capacity = %d",
            sink.getName(), batchSize,
            channel.getName(), transCap);
        throw new InstantiationException(msg);
      }
    }
  }

  private void loadSinks(AgentConfiguration agentConf,
      Map<String, ChannelComponent> channelComponentMap, Map<String, SinkRunner> sinkRunnerMap)
      throws InstantiationException {


    Set<String> sinkNames = agentConf.getSinkSet();
    Map<String, ComponentConfiguration> compMap =
        agentConf.getSinkConfigMap();
    Map<String, Sink> sinks = new HashMap<String, Sink>();
    /*
     * Components which have a ComponentConfiguration object
     * todo 组合配置ComponentConfiguration 的对象
     */
    for (String sinkName : sinkNames) {
      ComponentConfiguration comp = compMap.get(sinkName);
      if (comp != null) {

        //todo 使用SinkFactory 直接采用根据类型,采用反射方式  实例化 Sink
        SinkConfiguration config = (SinkConfiguration) comp;
        Sink sink = sinkFactory.create(comp.getComponentName(), comp.getType());


        try {

          //todo 为 Sink 匹配对应的 channel
          Configurables.configure(sink, config);
          ChannelComponent channelComponent = channelComponentMap.get(config.getChannel());
          if (channelComponent == null) {
            String msg = String.format("Sink %s is not connected to a " +
                "channel",  sinkName);
            throw new IllegalStateException(msg);
          }

          //todo 检查 channel 是否可用 :  sink 的 batch size 要大于 channel 的 transaction capacity
          checkSinkChannelCompatibility(sink, channelComponent.channel);

          sink.setChannel(channelComponent.channel);

          sinks.put(comp.getComponentName(), sink);

          //todo Sink 向 channel 反向注册 SinkName
          channelComponent.components.add(sinkName);
        } catch (Exception e) {
          String msg = String.format("Sink %s has been removed due to an " +
              "error during configuration", sinkName);
          LOGGER.error(msg, e);
        }
      }
    }
    /*
     * Components which DO NOT have a ComponentConfiguration object
     * and use only Context
     * todo 组合没有配置 ComponentConfiguration 但是使用 context  的对象
     */
    Map<String, Context> sinkContexts = agentConf.getSinkContext();
    for (String sinkName : sinkNames) {
      Context context = sinkContexts.get(sinkName);
      if (context != null) {

        //todo 直接采用根据类型,采用反射方式  实例化 Sink
        Sink sink = sinkFactory.create(sinkName, context.getString(
            BasicConfigurationConstants.CONFIG_TYPE));


        try {

          //todo 为 Sink 匹配对应的 channel
          Configurables.configure(sink, context);
          ChannelComponent channelComponent =
              channelComponentMap.get(
                  context.getString(BasicConfigurationConstants.CONFIG_CHANNEL));
          if (channelComponent == null) {
            String msg = String.format("Sink %s is not connected to a " +
                "channel",  sinkName);
            throw new IllegalStateException(msg);
          }
          //todo 检查 channel 是否可用 :  sink 的 batch size 要大于 channel 的 transaction capacity
          checkSinkChannelCompatibility(sink, channelComponent.channel);


          sink.setChannel(channelComponent.channel);
          sinks.put(sinkName, sink);
          channelComponent.components.add(sinkName);

        } catch (Exception e) {
          String msg = String.format("Sink %s has been removed due to an " +
              "error during configuration", sinkName);
          LOGGER.error(msg, e);
        }
      }
    }

    //todo 对 sink 进行分组
    loadSinkGroups(agentConf, sinks, sinkRunnerMap);
  }

  private void loadSinkGroups(AgentConfiguration agentConf,
      Map<String, Sink> sinks, Map<String, SinkRunner> sinkRunnerMap)
          throws InstantiationException {


    // todo 获取配置中的 group 分组
    Set<String> sinkGroupNames = agentConf.getSinkgroupSet();


    Map<String, ComponentConfiguration> compMap =
        agentConf.getSinkGroupConfigMap();

    Map<String, String> usedSinks = new HashMap<String, String>();

    for (String groupName: sinkGroupNames) {

      ComponentConfiguration comp = compMap.get(groupName);

      if (comp != null) {
        SinkGroupConfiguration groupConf = (SinkGroupConfiguration) comp;
        List<Sink> groupSinks = new ArrayList<Sink>();
        for (String sink : groupConf.getSinks()) {
          Sink s = sinks.remove(sink);
          if (s == null) {
            String sinkUser = usedSinks.get(sink);
            if (sinkUser != null) {
              throw new InstantiationException(String.format(
                  "Sink %s of group %s already " +
                      "in use by group %s", sink, groupName, sinkUser));
            } else {
              throw new InstantiationException(String.format(
                  "Sink %s of group %s does "
                      + "not exist or is not properly configured", sink,
                      groupName));
            }
          }
          groupSinks.add(s);
          usedSinks.put(sink, groupName);
        }
        try {

          SinkGroup group = new SinkGroup(groupSinks);
          Configurables.configure(group, groupConf);

          //todo 创建 sinkRunner
          sinkRunnerMap.put(comp.getComponentName(),
              new SinkRunner(group.getProcessor()));

        } catch (Exception e) {
          String msg = String.format("SinkGroup %s has been removed due to " +
              "an error during configuration", groupName);
          LOGGER.error(msg, e);
        }
      }
    }

    // add any unassigned sinks to solo collectors
    // todo 对未分组的 sink 进行处理
    for (Entry<String, Sink> entry : sinks.entrySet()) {
      if (!usedSinks.containsValue(entry.getKey())) {
        try {

          SinkProcessor pr = new DefaultSinkProcessor();
          List<Sink> sinkMap = new ArrayList<Sink>();
          sinkMap.add(entry.getValue());
          pr.setSinks(sinkMap);
          Configurables.configure(pr, new Context());

          //todo 创建 SinkRunner
          sinkRunnerMap.put(entry.getKey(), new SinkRunner(pr));

        } catch (Exception e) {
          String msg = String.format("SinkGroup %s has been removed due to " +
              "an error during configuration", entry.getKey());
          LOGGER.error(msg, e);
        }
      }
    }


  }





  private static class ChannelComponent {
    final Channel channel;
    final List<String> components;

    ChannelComponent(Channel channel) {
      this.channel = channel;
      components = Lists.newArrayList();
    }
  }

  protected Map<String, String> toMap(Properties properties) {
    Map<String, String> result = Maps.newHashMap();
    Enumeration<?> propertyNames = properties.propertyNames();
    while (propertyNames.hasMoreElements()) {
      String name = (String) propertyNames.nextElement();
      String value = properties.getProperty(name);
      result.put(name, value);
    }
    return result;
  }
}