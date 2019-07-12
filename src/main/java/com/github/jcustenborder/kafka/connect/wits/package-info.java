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
@Introduction("WITS is an oil and gas protocol used for transferring data from drilling operations. " +
    "This plugin is used to connect to a WITS server and retrieve data in real time allowing the " +
    "data to be consumed by Kafka based applications. This plugin is specifically for the ascii " +
    "WITS protocol and is not for the later WITSML protocol.")
@Title("WITS Plugin")
@DocumentationWarning("This is a warning")
@PluginOwner("jcustenborder")
@PluginName("kafka-connect-wits")
package com.github.jcustenborder.kafka.connect.wits;

import com.github.jcustenborder.kafka.connect.utils.config.DocumentationWarning;
import com.github.jcustenborder.kafka.connect.utils.config.Introduction;
import com.github.jcustenborder.kafka.connect.utils.config.PluginName;
import com.github.jcustenborder.kafka.connect.utils.config.PluginOwner;
import com.github.jcustenborder.kafka.connect.utils.config.Title;