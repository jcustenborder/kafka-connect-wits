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
import com.github.jcustenborder.kafka.connect.utils.config.Description;
import com.github.jcustenborder.kafka.connect.utils.config.DocumentationNote;
import com.google.common.net.HostAndPort;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.connector.Task;
import org.apache.kafka.connect.source.SourceConnector;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

@Description("This connector is used to retrieve data from a WITS server and write the data to " +
    "kafka")
@DocumentationNote("The maximum number of tasks must be at least the number of servers specified")
public class WITSSourceConnector extends SourceConnector {

  Map<String, String> settings;


  @Override
  public void start(Map<String, String> settings) {
    WITSSourceConnectorConfig config = new WITSSourceConnectorConfig(settings);
    /**
     * Do whatever you need to do to setup your connector on a global scale. This is something that
     * will execute once per connector instance.
     */
    this.settings = settings;
  }

  @Override
  public Class<? extends Task> taskClass() {
    return WITSSourceTask.class;
  }

  @Override
  public List<Map<String, String>> taskConfigs(int maxTasks) {
    List<Map<String, String>> taskConfigs = new ArrayList<>();
    WITSSourceConnectorConfig config = new WITSSourceConnectorConfig(this.settings);

    for (HostAndPort hostAndPort : config.connectionServers()) {
      Map<String, String> taskConfig = new LinkedHashMap<>(this.settings);
      taskConfig.put(WITSSourceConnectorConfig.CONNECTION_SERVERS_CONF, hostAndPort.toString());
      taskConfigs.add(taskConfig);
    }

    return taskConfigs;
  }

  @Override
  public void stop() {

  }

  @Override
  public ConfigDef config() {
    return WITSSourceConnectorConfig.config(WITSSourceConnectorConfig.DEFAULT_CONFIG_OPTIONS);
  }

  @Override
  public String version() {
    return VersionUtil.version(this.getClass());
  }
}
