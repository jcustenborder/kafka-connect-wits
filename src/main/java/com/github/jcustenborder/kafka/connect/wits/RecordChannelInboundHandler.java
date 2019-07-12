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

import com.github.jcustenborder.kafka.connect.utils.data.SourceRecordDeque;
import com.github.jcustenborder.netty.wits.Record;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import org.apache.kafka.connect.source.SourceRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.SocketAddress;

class RecordChannelInboundHandler extends SimpleChannelInboundHandler<Record> {
  private static final Logger log = LoggerFactory.getLogger(RecordChannelInboundHandler.class);
  final SourceRecordDeque records;
  final WITSSourceConnectorConfig config;
  final RecordConverter recordConverter;

  public RecordChannelInboundHandler(SourceRecordDeque records, WITSSourceConnectorConfig config) {
    this.records = records;
    this.config = config;
    this.recordConverter = new RecordConverter();
  }

  @Override
  protected void channelRead0(ChannelHandlerContext channelHandlerContext, Record record) throws Exception {
    SocketAddress remoteAddress = channelHandlerContext.channel().remoteAddress();
    log.trace("channelRead0() - remoteAddress = '{}' record = '{}'", remoteAddress, record);
    SourceRecord sourceRecord = this.recordConverter.sourceRecord(record, remoteAddress);
    this.records.add(sourceRecord);
  }
}
