/*
 * Licensed to Elastic Search and Shay Banon under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Elastic Search licenses this
 * file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.discovery.zookeeper;

import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.discovery.zookeeper.client.ZookeeperClient;
import org.elasticsearch.discovery.zookeeper.client.ZookeeperClientService;
import org.elasticsearch.discovery.zookeeper.embedded.EmbeddedZookeeperService;
import org.elasticsearch.env.Environment;
import org.testng.annotations.*;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static org.elasticsearch.common.settings.ImmutableSettings.settingsBuilder;

/**
 * @author imotov
 */
public abstract class AbstractZookeeperTests {

    protected final ESLogger logger = Loggers.getLogger(getClass());

    protected EmbeddedZookeeperService embeddedZooKeeperService;

    protected List<ZookeeperClient> zookeeperClients = new ArrayList<ZookeeperClient>();

    private Settings defaultSettings = ImmutableSettings.Builder.EMPTY_SETTINGS;

    public void putDefaultSettings(Settings.Builder settings) {
        putDefaultSettings(settings.build());
    }

    public void putDefaultSettings(Settings settings) {
        defaultSettings = ImmutableSettings.settingsBuilder().put(defaultSettings).put(settings).build();
    }


    @BeforeClass public void startZooKeeper() throws IOException, InterruptedException {
        Environment tempEnvironment = new Environment(defaultSettings);
        File zookeeperDataDirectory = new File(tempEnvironment.dataFile(), "zookeeper");
        logger.info("Deleting zookeeper directory {}", zookeeperDataDirectory);
        deleteDirectory(zookeeperDataDirectory);
        embeddedZooKeeperService = new EmbeddedZookeeperService(defaultSettings, tempEnvironment);
        embeddedZooKeeperService.start();
        putDefaultSettings(ImmutableSettings.settingsBuilder()
                .put(defaultSettings)
                .put("discovery.zookeeper.client.host", "localhost:" + embeddedZooKeeperService.port()));
    }

    @AfterClass public void stopZooKeeper() {
        if (embeddedZooKeeperService != null) {
            embeddedZooKeeperService.stop();
            embeddedZooKeeperService.close();
            embeddedZooKeeperService = null;
        }
    }

    @AfterMethod public void stopZooKeeperClients() {
        for (ZookeeperClient zookeeperClient : zookeeperClients) {
            logger.info("Closing {}" + zookeeperClient);
            zookeeperClient.stop();
            zookeeperClient.close();
        }
        zookeeperClients.clear();
    }

    public ZookeeperClient buildZookeeper() {
        return buildZookeeper(ImmutableSettings.Builder.EMPTY_SETTINGS);
    }

    public ZookeeperClient buildZookeeper(Settings settings) {
        String settingsSource = getClass().getName().replace('.', '/') + ".yml";
        Settings finalSettings = settingsBuilder()
                .loadFromClasspath(settingsSource)
                .put(defaultSettings)
                .put(settings)
                .build();

        ZookeeperClient zookeeperClient = new ZookeeperClientService(finalSettings, ClusterName.clusterNameFromSettings(finalSettings));
        zookeeperClient.start();
        zookeeperClients.add(zookeeperClient);
        return zookeeperClient;
    }

    private boolean deleteDirectory(File path) {
        if (path.exists()) {
            File[] files = path.listFiles();
            for (File file : files) {
                if (file.isDirectory()) {
                    if (!deleteDirectory(file)) {
                        return false;
                    }
                } else {
                    if (!file.delete()) {
                        return false;
                    }
                }
            }
            return path.delete();
        }
        return false;
    }
}
