/**
 * Copyright © 2019 Jeremy Custenborder (jcustenborder@gmail.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.github.jcustenborder.kafka.connect.wits;

import com.github.jcustenborder.kafka.connect.utils.VersionUtil;
import com.github.jcustenborder.kafka.connect.utils.data.SourceRecordDeque;
import com.github.jcustenborder.kafka.connect.utils.data.SourceRecordDequeBuilder;
import com.github.jcustenborder.netty.wits.WITSDecoder;
import com.github.jcustenborder.netty.wits.WITSPacketDecoder;
import com.google.common.net.HostAndPort;
import io.netty.bootstrap.Bootstrap;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import org.apache.kafka.connect.source.SourceRecord;
import org.apache.kafka.connect.source.SourceTask;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetSocketAddress;
import java.util.List;
import java.util.Map;

public class WITSSourceTask extends SourceTask {
  private static final Logger log = LoggerFactory.getLogger(WITSSourceTask.class);

  @Override
  public String version() {
    return VersionUtil.version(this.getClass());
  }

  WITSSourceConnectorConfig config;

  SourceRecordDeque records;
  EventLoopGroup group = new NioEventLoopGroup(1);

  @Override
  public void start(Map<String, String> settings) {
    this.config = new WITSSourceConnectorConfig(settings);
    this.records = SourceRecordDequeBuilder.of()
        .build();
  }


  ChannelFuture channelFuture;

  void checkConnection() throws InterruptedException {
    if (null == this.channelFuture || !this.channelFuture.channel().isActive()) {
      Bootstrap clientBootstrap = new Bootstrap();

      clientBootstrap.group(group);
      clientBootstrap.channel(NioSocketChannel.class);
      HostAndPort hostAndPort = this.config.connectionServers().get(0);
      log.info("Connecting to {}", hostAndPort);
      clientBootstrap.remoteAddress(
          new InetSocketAddress(
              hostAndPort.getHost(),
              hostAndPort.getPort()
          )
      );
      clientBootstrap.handler(new ChannelInitializer<SocketChannel>() {
        protected void initChannel(SocketChannel socketChannel) throws Exception {
          socketChannel.pipeline().addLast(new WITSPacketDecoder());
          socketChannel.pipeline().addLast(new WITSDecoder());
          socketChannel.pipeline().addLast(new RecordChannelInboundHandler(records, config));
        }
      });
      this.channelFuture = clientBootstrap.connect().sync();
    }
  }


  @Override
  public List<SourceRecord> poll() throws InterruptedException {
    checkConnection();

    return this.records.getBatch();
  }

  @Override
  public void stop() {
    log.info("Stopping");
    try {
      log.info("Closing Channel");
      this.channelFuture.channel().close().sync();
      log.info("Shutting down group");
      this.group.shutdownGracefully().sync();
    } catch (InterruptedException e) {
      log.error("Exception thrown", e);
    }
  }
}
