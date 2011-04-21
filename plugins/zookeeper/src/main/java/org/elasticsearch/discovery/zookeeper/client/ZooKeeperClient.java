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

    /**
     * Tries to elect node with specified id as a master, returns the elected node id
     */
    public String electMaster(String id, NodeDeletedListener masterDeletedListener) throws ElasticSearchException, InterruptedException;

    /**
     * Returns currently elected master or null if no master is present
     * @param masterCreatedListener triggered when new master is elected
     * @param masterDeletedListener triggered when elected master disappears
     * @return elected master or null
     * @throws ElasticSearchException
     * @throws InterruptedException
     */
    public String findMaster(NodeCreatedListener masterCreatedListener, NodeDeletedListener masterDeletedListener) throws ElasticSearchException, InterruptedException;

    /**
     * Registers the node in the list of live nodes
     * @param nodeInfo node to register
     * @throws ElasticSearchException
     * @throws InterruptedException
     */
    public void registerNode(DiscoveryNode nodeInfo) throws ElasticSearchException, InterruptedException;

    /**
     * Unregister the node
     * @param id
     * @throws ElasticSearchException
     * @throws InterruptedException
     */
    public void unregisterNode(String id) throws ElasticSearchException, InterruptedException;

    /**
     * Lists live nodes
     * @param listener triggered when list of live nodes changes
     * @return
     * @throws ElasticSearchException
     * @throws InterruptedException
     */
    public Set<String> listNodes(NodeListChangedListener listener) throws ElasticSearchException, InterruptedException;

    /**
     * Returns info for live node
     * @param id
     * @return
     * @throws ElasticSearchException
     * @throws InterruptedException
     */
    public DiscoveryNode nodeInfo(String id) throws ElasticSearchException, InterruptedException;

    /**
     * Sets the local node
     * @param localNode
     */
    public void localNode(DiscoveryNode localNode);

    /**
     * Synchronizes cluster state. It needs to be called when node is switching between retrieving cluster state
     * and publishing cluster state
     * @throws ElasticSearchException
     * @throws InterruptedException
     */
    public void syncClusterState() throws ElasticSearchException, InterruptedException;

    /**
     * Publish new cluster state
     * @param state
     * @throws ElasticSearchException
     * @throws InterruptedException
     */
    public void publishClusterState(ClusterState state) throws ElasticSearchException, InterruptedException;

    /**
     * Retrieves cluster state
     * @param newClusterStateListener triggered when cluster state changes
     * @return
     * @throws ElasticSearchException
     * @throws InterruptedException
     */
    public ClusterState retrieveClusterState(NewClusterStateListener newClusterStateListener) throws ElasticSearchException, InterruptedException;

    /**
     * Returns zookeeper session id. Used for debugging.
     * @return
     */
    public long sessionId();

    /**
     * Checks if zookeeper is connected.
     * @return
     */
    public boolean connected();

    interface NewClusterStateListener {
        public void onNewClusterState(ClusterState clusterState);
    }

    interface NodeCreatedListener {
        public void onNodeCreated(String id);

    }

    interface NodeDeletedListener {
        public void onNodeDeleted(String id);
    }

    interface NodeListChangedListener {
        public void onNodeListChanged();
    }
}
