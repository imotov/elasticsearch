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

import org.elasticsearch.ElasticSearchException;
import org.elasticsearch.ElasticSearchIllegalStateException;
import org.elasticsearch.cluster.*;
import org.elasticsearch.cluster.block.ClusterBlocks;
import org.elasticsearch.cluster.metadata.MetaData;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.cluster.routing.RoutingTable;
import org.elasticsearch.common.UUID;
import org.elasticsearch.common.component.AbstractLifecycleComponent;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.discovery.Discovery;
import org.elasticsearch.discovery.InitialStateDiscoveryListener;
import org.elasticsearch.discovery.zen.DiscoveryNodesProvider;
import org.elasticsearch.discovery.zen.publish.PublishClusterStateAction;
import org.elasticsearch.discovery.zookeeper.client.*;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import static org.elasticsearch.cluster.ClusterState.newClusterStateBuilder;
import static org.elasticsearch.cluster.node.DiscoveryNode.buildCommonNodesAttributes;
import static org.elasticsearch.cluster.node.DiscoveryNodes.newNodesBuilder;

/**
 * @author imotov
 */
public class ZooKeeperDiscovery extends AbstractLifecycleComponent<Discovery> implements Discovery, DiscoveryNodesProvider {
    private final TransportService transportService;

    private final ClusterService clusterService;

    private final ClusterName clusterName;

    private final ThreadPool threadPool;

    private final AtomicBoolean initialStateSent = new AtomicBoolean();

    private final CopyOnWriteArrayList<InitialStateDiscoveryListener> initialStateListeners = new CopyOnWriteArrayList<InitialStateDiscoveryListener>();

    private ZooKeeperClient zooKeeperClient;

    private DiscoveryNode localNode;

    private boolean publishClusterStateToZooKeeper;

    private volatile boolean master = false;

    private volatile DiscoveryNodes latestDiscoNodes;

    private volatile Thread currentJoinThread;

    private final Lock updateNodeListLock = new ReentrantLock();

    private final Lock discoveryRestartLock = new ReentrantLock();

    private final PublishClusterStateAction publishClusterState;

    private final MasterGoneListener masterGoneListener = new MasterGoneListener();

    private final MasterAppearedListener masterAppearedListener = new MasterAppearedListener(false);

    private final MasterAppearedListener initialMasterAppearedListener = new MasterAppearedListener(true);

    private final NodeUnregisteredListener nodeUnregisteredListener = new NodeUnregisteredListener();

    private final MasterNodeListChangedListener masterNodeListChangedListener = new MasterNodeListChangedListener();


    @Inject public ZooKeeperDiscovery(Settings settings, ClusterName clusterName, ThreadPool threadPool,
                                      TransportService transportService, ClusterService clusterService,
                                      ZooKeeperClient zooKeeperClient) {
        super(settings);
        this.clusterName = clusterName;
        this.clusterService = clusterService;
        this.transportService = transportService;
        this.zooKeeperClient = zooKeeperClient;
        this.threadPool = threadPool;
        this.publishClusterStateToZooKeeper = componentSettings.getAsBoolean("state_publishing.enabled", false);
        if (!publishClusterStateToZooKeeper) {
            this.publishClusterState = new PublishClusterStateAction(settings, transportService, this, new NewClusterStateListener());
        } else {
            this.publishClusterState = null;
        }
    }

    @Override protected void doStart() throws ElasticSearchException {
        Map<String, String> nodeAttributes = buildCommonNodesAttributes(settings);
        // note, we rely on the fact that its a new id each time we start, see FD and "kill -9" handling
        String nodeId = UUID.randomBase64UUID();
        localNode = new DiscoveryNode(settings.get("name"), nodeId, transportService.boundAddress().publishAddress(), nodeAttributes);
        latestDiscoNodes = new DiscoveryNodes.Builder().put(localNode).localNodeId(localNode.id()).build();
        initialStateSent.set(false);
        zooKeeperClient.localNode(localNode);
        zooKeeperClient.start();
        // do the join on a different thread, the DiscoveryService waits for 30s anyhow till it is discovered
        asyncJoinCluster(true);
    }

    @Override protected void doStop() throws ElasticSearchException {
        zooKeeperClient.stop();
        master = false;
        if (currentJoinThread != null) {
            try {
                currentJoinThread.interrupt();
            } catch (Exception e) {
                // ignore
            }
        }
    }

    @Override protected void doClose() throws ElasticSearchException {
        zooKeeperClient.close();
    }

    @Override public DiscoveryNode localNode() {
        return localNode;
    }

    @Override public void addListener(InitialStateDiscoveryListener listener) {
        this.initialStateListeners.add(listener);
    }

    @Override public void removeListener(InitialStateDiscoveryListener listener) {
        this.initialStateListeners.remove(listener);
    }

    @Override public String nodeDescription() {
        return clusterName.value() + "/" + localNode.id();
    }


    @Override public void publish(ClusterState clusterState) {
        if (!master) {
            throw new ElasticSearchIllegalStateException("Shouldn't publish state when not master");
        }
        latestDiscoNodes = clusterState.nodes();
        if (publishClusterStateToZooKeeper) {
            try {
                zooKeeperClient.publishClusterState(clusterState);
            } catch (InterruptedException ex) {
                // Ignore
            }
        } else {
            publishClusterState.publish(clusterState);
        }
    }

    @Override public DiscoveryNodes nodes() {
        DiscoveryNodes latestNodes = this.latestDiscoNodes;
        if (latestNodes != null) {
            return latestNodes;
        }
        // have not decided yet, just send the local node
        return newNodesBuilder().put(localNode).localNodeId(localNode.id()).build();
    }

    private void asyncJoinCluster(final boolean initial) {
        threadPool.cached().execute(new Runnable() {
            @Override public void run() {
                currentJoinThread = Thread.currentThread();
                try {
                    innerJoinCluster(initial);
                } finally {
                    currentJoinThread = null;
                }
            }
        });
    }

    private void innerJoinCluster(boolean initial) {
        try {
            if (!initial || register()) {
                // Check if node should propose itself as a master
                if (localNode.isMasterNode()) {
                    electMaster(initial);
                } else {
                    findMaster(initial);
                }
            }
        } catch (InterruptedException ex) {
            // Ignore
        }
    }

    private boolean register() throws InterruptedException {
        if (lifecycle.stoppedOrClosed()) {
            return false;
        }
        try {
            zooKeeperClient.registerNode(localNode, nodeUnregisteredListener);
            return true;
        } catch (Exception ex) {
            restartDiscovery();
            return false;
        }
    }

    private void findMaster(boolean initial) throws InterruptedException {
        MasterAppearedListener currentMasterAppearedListener;
        if (initial) {
            currentMasterAppearedListener = initialMasterAppearedListener;
        } else {
            currentMasterAppearedListener = masterAppearedListener;
        }
        String masterId = zooKeeperClient.findMaster(currentMasterAppearedListener, masterGoneListener);
        if (masterId == null) {
            if (!initial) {
                removeMaster();
            }
        } else {
            addMaster(masterId);
        }
    }

    private void electMaster(boolean initial) throws InterruptedException {
        boolean retry = true;
        while (retry) {
            if (lifecycle.stoppedOrClosed()) {
                return;
            }
            retry = false;
            try {
                String masterNodeId = zooKeeperClient.electMaster(localNode.id(), masterGoneListener);
                if (masterNodeId != null) {
                    if (localNode.id().equals(masterNodeId)) {
                        becomeMaster(initial);
                    } else {
                        addMaster(masterNodeId);
                    }
                } else {
                    // This node can become master - retrying
                    retry = true;
                }
            } catch (Exception ex) {
                logger.error("Couldn't elect master. Restarting discovery.", ex);
                restartDiscovery();
                return;
            }
        }
    }

    private void addMaster(String masterNodeId) throws InterruptedException {
        master = false;
        if (publishClusterStateToZooKeeper) {
            ClusterState state = zooKeeperClient.retrieveClusterState(new NewZooKeeperClusterStateListener());
            if (state != null && masterNodeId.equals(state.nodes().masterNodeId())) {
                // Check that this state was published by elected master
                handleNewClusterStateFromMaster(state);
            }
        } else {
            // We just wait until cluster state is published by the master
        }

    }

    private void removeMaster() {
        clusterService.submitStateUpdateTask("zoo-keeper-disco-no-master (no_master_found)", new ProcessedClusterStateUpdateTask() {
            @Override public ClusterState execute(ClusterState currentState) {
                MetaData metaData = currentState.metaData();
                RoutingTable routingTable = currentState.routingTable();
                ClusterBlocks clusterBlocks = ClusterBlocks.builder().blocks(currentState.blocks()).addGlobalBlock(NO_MASTER_BLOCK).build();
                // if this is a data node, clean the metadata and routing, since we want to recreate the indices and shards
                if (currentState.nodes().localNode()!= null && currentState.nodes().localNode().dataNode()) {
                    metaData = MetaData.newMetaDataBuilder().build();
                    routingTable = RoutingTable.newRoutingTableBuilder().build();
                }
                DiscoveryNodes.Builder builder = DiscoveryNodes.newNodesBuilder()
                        .putAll(currentState.nodes());
                DiscoveryNode masterNode = currentState.nodes().masterNode();
                if (masterNode != null) {
                    builder = builder.remove(masterNode.id());
                }
                latestDiscoNodes = builder.build();
                return newClusterStateBuilder().state(currentState)
                        .blocks(clusterBlocks)
                        .nodes(latestDiscoNodes)
                        .metaData(metaData)
                        .routingTable(routingTable)
                        .build();
            }

            @Override public void clusterStateProcessed(ClusterState clusterState) {
                sendInitialStateEventIfNeeded();
            }
        });
    }

    private void becomeMaster(final boolean initial) throws InterruptedException {
        this.master = true;
        if (publishClusterStateToZooKeeper) {
            zooKeeperClient.syncClusterState();
        }
        clusterService.submitStateUpdateTask("zen-disco-join (elected_as_master)", new ProcessedClusterStateUpdateTask() {
            @Override public ClusterState execute(ClusterState currentState) {
                DiscoveryNodes.Builder builder = new DiscoveryNodes.Builder();
                if (initial) {
                    builder.put(localNode);
                } else {
                    builder.putAll(currentState.nodes())
                            // remove the previous master
                            .remove(currentState.nodes().masterNodeId());
                }
                // update the fact that we are the master...
                builder.localNodeId(localNode.id()).masterNodeId(localNode.id());
                latestDiscoNodes = builder.build();
                ClusterBlocks clusterBlocks = ClusterBlocks.builder().blocks(currentState.blocks()).removeGlobalBlock(NO_MASTER_BLOCK).build();
                return newClusterStateBuilder().state(currentState).nodes(builder).blocks(clusterBlocks).build();
            }

            @Override public void clusterStateProcessed(ClusterState clusterState) {
                sendInitialStateEventIfNeeded();
            }
        });

        handleUpdateNodeList();
    }


    private void restartDiscovery() {
        discoveryRestartLock.lock();
        try {
            if (lifecycle.started()) {
                if (!zooKeeperClient.connected()) {
                    logger.info("Restarting ZooKeeper discovery");
                    // We are no longer a master
                    master = false;
                    try {
                        logger.trace("Stopping ZooKeeper");
                        zooKeeperClient.stop();
                    } catch (Exception ex) {
                        logger.error("Error stopping ZooKeeper", ex);
                    }
                    while (lifecycle.started()) {
                        try {
                            logger.trace("Starting ZooKeeper");
                            zooKeeperClient.start();
                            logger.trace("Started ZooKeeper");
                            asyncJoinCluster(true);
                            return;
                        } catch (ZooKeeperClientException ex) {
                            if (ex.getCause() != null && ex.getCause() instanceof InterruptedException) {
                                logger.info("ZooKeeper was interrupted", ex);
                                return;
                            }
                            logger.warn("Error starting ZooKeeper ", ex);
                        }
                    }
                } else {
                    logger.trace("ZooKeeper is already restarted. Ignoring");
                }

            }
        } finally {
            discoveryRestartLock.unlock();
        }
    }

    private void processDeletedNode(final String nodeId) {
        clusterService.submitStateUpdateTask("zoo-keeper-disco-node_left(" + nodeId + ")", new ClusterStateUpdateTask() {
            @Override public ClusterState execute(ClusterState currentState) {
                if (currentState.nodes().nodeExists(nodeId)) {
                    DiscoveryNodes.Builder builder = new DiscoveryNodes.Builder()
                            .putAll(currentState.nodes())
                            .remove(nodeId);
                    latestDiscoNodes = builder.build();
                    return newClusterStateBuilder().state(currentState).nodes(latestDiscoNodes).build();
                } else {
                    logger.warn("Trying to deleted a node that doesn't exist {}", nodeId);
                    return currentState;
                }
            }
        });

    }

    private void processAddedNode(final DiscoveryNode node) {
        if (!master) {
            throw new ElasticSearchIllegalStateException("Node [" + localNode + "] not master for join request from [" + node + "]");
        }

        if (!transportService.addressSupported(node.address().getClass())) {
            logger.warn("received a wrong address type from [{}], ignoring...", node);
        } else {
            clusterService.submitStateUpdateTask("zoo-keeper-disco-receive(join from node[" + node + "])", new ClusterStateUpdateTask() {
                @Override public ClusterState execute(ClusterState currentState) {
                    if (currentState.nodes().nodeExists(node.id())) {
                        // the node already exists in the cluster
                        logger.warn("received a join request for an existing node [{}]", node);
                        // still send a new cluster state, so it will be re published and possibly update the other node
                        return ClusterState.builder().state(currentState).build();
                    }
                    return newClusterStateBuilder().state(currentState).nodes(currentState.nodes().newNode(node)).build();
                }
            });
        }
    }

    private void sendInitialStateEventIfNeeded() {
        if (initialStateSent.compareAndSet(false, true)) {
            for (InitialStateDiscoveryListener listener : initialStateListeners) {
                listener.initialStateProcessed();
            }
        }
    }

    private void handleNewClusterStateFromMaster(final ClusterState clusterState) {
        if (!lifecycle.started()) {
            return;
        }
        if (master) {
            logger.warn("master should not receive new cluster state from [{}]", clusterState.nodes().masterNode());
        } else {
            // Make sure that we are part of the state
            if (clusterState.nodes().localNode() != null) {
                clusterService.submitStateUpdateTask("zoo-keeper-disco-receive(from master [" + clusterState.nodes().masterNode() + "])", new ProcessedClusterStateUpdateTask() {
                    @Override public ClusterState execute(ClusterState currentState) {
                        latestDiscoNodes = clusterState.nodes();
                        return clusterState;
                    }

                    @Override public void clusterStateProcessed(ClusterState clusterState) {
                        sendInitialStateEventIfNeeded();
                    }
                });
            }
        }
    }

    private void handleUpdateNodeList() {
        if (!lifecycle.started()) {
            return;
        }
        if (!master) {
            return;
        }
        logger.trace("Updating node list");
        boolean restart = false;
        updateNodeListLock.lock();
        try {
            Set<String> currentNodes = latestDiscoNodes.nodes().keySet();
            Set<String> nodes = zooKeeperClient.listNodes(masterNodeListChangedListener);
            Set<String> deleted = new HashSet<String>(currentNodes);
            deleted.removeAll(nodes);
            Set<String> added = new HashSet<String>(nodes);
            added.removeAll(currentNodes);
            for (String nodeId : deleted) {
                processDeletedNode(nodeId);
            }
            for (String nodeId : added) {
                if (!nodeId.equals(localNode.id())) {
                    DiscoveryNode node = zooKeeperClient.nodeInfo(nodeId);
                    if (node != null) {
                        processAddedNode(node);
                    }
                }
            }
        } catch (Exception ex) {
            restart = true;
            logger.error("Couldn't elect master. Restarting discovery.", ex);
        } finally {
            updateNodeListLock.unlock();
        }
        if (restart) {
            restartDiscovery();
        }
    }


    private void handleMasterGone() {
        if (!lifecycle.started()) {
            return;
        }
        logger.info("Master is gone");
        asyncJoinCluster(false);
    }

    private void handleSelfGone(String id) {
        if (!lifecycle.started()) {
            return;
        }
        logger.warn("Node registration disappeared {} ", id);
        try {
            register();
        } catch (InterruptedException ex) {
            // Ignore
        }
    }

    private void handleMasterAppeared(boolean initial) {
        if (!lifecycle.started()) {
            return;
        }
        logger.info("New master appeared");
        asyncJoinCluster(initial);
    }

    private class MasterGoneListener implements ZooKeeperClient.NodeDeletedListener {

        @Override public void onNodeDeleted(String id) {
            handleMasterGone();
        }
    }

    private class MasterAppearedListener implements ZooKeeperClient.NodeCreatedListener {
        boolean initial;

        public MasterAppearedListener(boolean initial) {
            this.initial = initial;
        }

        @Override public void onNodeCreated(String id) {
            handleMasterAppeared(initial);
        }
    }


    private class NodeUnregisteredListener implements ZooKeeperClient.NodeDeletedListener {
        @Override public void onNodeDeleted(String id) {
            handleSelfGone(id);
        }
    }

    private class MasterNodeListChangedListener implements ZooKeeperClient.NodeListChangedListener {

        @Override public void onNodeListChanged() {
            handleUpdateNodeList();
        }
    }

    private class NewClusterStateListener implements PublishClusterStateAction.NewClusterStateListener {
        @Override public void onNewClusterState(ClusterState clusterState) {
            handleNewClusterStateFromMaster(clusterState);
        }
    }

    private class NewZooKeeperClusterStateListener implements ZooKeeperClient.NewClusterStateListener {

        @Override public void onNewClusterState(ClusterState clusterState) {
            handleNewClusterStateFromMaster(clusterState);
        }
    }

}
