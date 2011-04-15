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

package org.elasticsearch.discovery.zookeeper.client;

import org.elasticsearch.ElasticSearchException;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.common.component.LifecycleComponent;

import java.util.Set;

/**
 * @author imotov
 */
public interface ZooKeeperClient extends LifecycleComponent<ZooKeeperClient> {

    public String electMaster(String id, NodeDeletedListener masterDeletedListener) throws ElasticSearchException, InterruptedException;

    public String findMaster(NodeCreatedListener masterCreatedListener, NodeDeletedListener masterDeletedListener) throws ElasticSearchException, InterruptedException;

    public void registerNode(DiscoveryNode nodeInfo, NodeDeletedListener listener) throws ElasticSearchException, InterruptedException;

    public void unregisterNode(String id) throws ElasticSearchException, InterruptedException;

    public Set<String> listNodes(NodeListChangedListener listener) throws ElasticSearchException, InterruptedException;

    public DiscoveryNode nodeInfo(String id) throws ElasticSearchException, InterruptedException;

    public void localNode(DiscoveryNode localNode);

    public void syncClusterState() throws ElasticSearchException, InterruptedException;

    public void publishClusterState(ClusterState state) throws ElasticSearchException, InterruptedException;

    public ClusterState retrieveClusterState(NewClusterStateListener newClusterStateListener) throws ElasticSearchException, InterruptedException;

    public long sessionId();

    public boolean connected();

}
