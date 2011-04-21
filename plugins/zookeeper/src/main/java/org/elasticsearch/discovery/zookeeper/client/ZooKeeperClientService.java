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

import org.apache.zookeeper.*;
import org.apache.zookeeper.data.Stat;
import org.elasticsearch.ElasticSearchException;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.block.ClusterBlocks;
import org.elasticsearch.cluster.metadata.MetaData;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.cluster.routing.RoutingTable;
import org.elasticsearch.cluster.routing.allocation.AllocationExplanation;
import org.elasticsearch.common.Bytes;
import org.elasticsearch.common.component.AbstractLifecycleComponent;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.io.stream.BytesStreamInput;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.zookeeper.ZooKeeperEnvironment;
import org.elasticsearch.zookeeper.ZooKeeperFactory;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.Callable;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * @author imotov
 */
public class ZooKeeperClientService extends AbstractLifecycleComponent<ZooKeeperClient> implements ZooKeeperClient {

    private ZooKeeper zooKeeper;

    private final ZooKeeperEnvironment environment;

    private final ZooKeeperFactory zooKeeperFactory;

    private static final int MAX_NODE_SIZE = 1024 * 1024;

    private final int maxNodeSize;

    private DiscoveryNode localNode;

    private final List<ClusterStatePart<?>> parts = new ArrayList<ClusterStatePart<?>>();

    private final Lock publishingLock = new ReentrantLock();

    @Inject public ZooKeeperClientService(Settings settings, ZooKeeperEnvironment environment, ZooKeeperFactory zooKeeperFactory) {
        super(settings);
        this.environment = environment;
        this.zooKeeperFactory = zooKeeperFactory;
        maxNodeSize = settings.getAsInt("zookeeper.maxnodesize", MAX_NODE_SIZE);
        initClusterStatePersistence();
    }

    @Override protected void doStart() throws ElasticSearchException {
        try {
            zooKeeper = zooKeeperFactory.newZooKeeper();
            createNode(environment.rootNodePath());
            createNode(environment.clusterNodePath());
            createNode(environment.nodesNodePath());
            createNode(environment.stateNodePath());
        } catch (InterruptedException e) {
            throw new ZooKeeperClientException("Cannot start ZooKeeper client", e);
        }
    }

    @Override protected void doStop() throws ElasticSearchException {
        if (zooKeeper != null) {
            try {
                zooKeeper.close();
            } catch (InterruptedException e) {
                // Ignore
            }
            zooKeeper = null;
        }
    }

    @Override protected void doClose() throws ElasticSearchException {
    }

    @Override public String electMaster(final String id, final NodeDeletedListener masterDeletedListener) throws ElasticSearchException, InterruptedException {
        if (!lifecycle.started()) {
            throw new ZooKeeperClientException("electMaster is called after was service stopped");
        }
        if (id == null) {
            throw new ZooKeeperClientException("electMaster is called with null id");
        }

        final Watcher watcher = (masterDeletedListener != null) ?
                new Watcher() {
                    @Override public void process(WatchedEvent event) {
                        if (event.getType() == Watcher.Event.EventType.NodeDeleted) {
                            masterDeletedListener.onNodeDeleted(extractId(event.getPath()));
                        }
                    }
                } : null;

        while (true) {
            try {
                byte[] leader = zooKeeperCall("Getting master data", new Callable<byte[]>() {
                    @Override public byte[] call() throws Exception {
                        return zooKeeper.getData(environment.masterNodePath(), watcher, null);
                    }
                });
                if (leader != null) {
                    return new String(leader);
                } else {
                    return null;
                }
            } catch (KeeperException.NoNodeException e) {
                try {
                    zooKeeperCall("Cannot create leader node", new Callable<Object>() {
                        @Override public Object call() throws Exception {
                            zooKeeper.create(environment.masterNodePath(), id.getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);
                            return null;
                        }
                    });
                } catch (KeeperException.NodeExistsException e1) {
                    // Ignore - somebody already created this node since the last time we checked
                } catch (KeeperException e1) {
                    throw new ZooKeeperClientException("Cannot create leader node", e1);
                }
            } catch (KeeperException e) {
                throw new ZooKeeperClientException("Cannot obtain leader node", e);
            }
        }
    }

    @Override public String findMaster(final NodeCreatedListener masterCreatedListener, final NodeDeletedListener masterDeletedListener) throws ElasticSearchException, InterruptedException {
        if (!lifecycle.started()) {
            throw new ZooKeeperClientException("findMaster is called after was service stopped");
        }

        final Watcher createdWatcher = (masterCreatedListener != null) ?
                new Watcher() {
                    @Override public void process(WatchedEvent event) {
                        if (event.getType() == Watcher.Event.EventType.NodeCreated) {
                            masterCreatedListener.onNodeCreated(extractId(event.getPath()));
                        }
                    }
                } : null;

        final Watcher deletedWatcher = (masterDeletedListener != null) ?
                new Watcher() {
                    @Override public void process(WatchedEvent event) {
                        if (event.getType() == Watcher.Event.EventType.NodeDeleted) {
                            masterDeletedListener.onNodeDeleted(extractId(event.getPath()));
                        }
                    }
                } : null;
        while (true) {
            try {
                Stat stat = zooKeeperCall("Checking if master exists", new Callable<Stat>() {
                    @Override public Stat call() throws Exception {
                        return zooKeeper.exists(environment.masterNodePath(), createdWatcher);
                    }
                });

                if (stat != null) {
                    byte[] leader = zooKeeperCall("Getting master data", new Callable<byte[]>() {
                        @Override public byte[] call() throws Exception {
                            return zooKeeper.getData(environment.masterNodePath(), deletedWatcher, null);
                        }
                    });
                    if (leader != null) {
                        return new String(leader);
                    } else {
                        return null;
                    }
                } else {
                    return null;
                }
            } catch (KeeperException.NoNodeException e) {
                // Node disappeared between exists() and getData() calls
                // We will try again
            } catch (KeeperException e) {
                throw new ZooKeeperClientException("Cannot obtain leader node", e);
            }
        }
    }

    @Override public void registerNode(final DiscoveryNode nodeInfo, final NodeDeletedListener listener) throws ElasticSearchException, InterruptedException {
        if (!lifecycle.started()) {
            throw new ZooKeeperClientException("registerNode is called after service was stopped");
        }
        final String id = nodeInfo.id();
        try {
            final String nodePath = nodePath(id);
            final Watcher watcher = (listener != null) ?
                    new Watcher() {
                        @Override public void process(WatchedEvent event) {
                            if (event.getType() == Watcher.Event.EventType.NodeDeleted) {
                                listener.onNodeDeleted(extractId(event.getPath()));
                            }
                        }
                    } : null;
            try {
                BytesStreamOutput streamOutput = new BytesStreamOutput();
                nodeInfo.writeTo(streamOutput);
                final byte[] buf = streamOutput.copiedByteArray();
                zooKeeperCall("Registering node " + id, new Callable<Object>() {
                    @Override public Object call() throws Exception {
                        zooKeeper.create(nodePath, buf, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);
                        return null;
                    }
                });
            } catch (KeeperException.NodeExistsException e1) {
                // Ignore
            }
            zooKeeperCall("Registering node " + id, new Callable<Stat>() {
                @Override public Stat call() throws Exception {
                    return zooKeeper.exists(nodePath, watcher);
                }
            });
        } catch (KeeperException e) {
            throw new ZooKeeperClientException("Cannot register node " + id, e);
        } catch (IOException e) {
            throw new ZooKeeperClientException("Cannot register node " + id, e);
        }
    }

    @Override public DiscoveryNode nodeInfo(final String id) throws ElasticSearchException, InterruptedException {
        if (!lifecycle.started()) {
            throw new ZooKeeperClientException("nodeInfo is called after was service stopped");
        }
        try {
            byte[] buf = zooKeeperCall("Node Info for node " + id, new Callable<byte[]>() {
                @Override public byte[] call() throws Exception {
                    return zooKeeper.getData(nodePath(id), null, null);

                }
            });
            if (buf != null) {
                return DiscoveryNode.readNode(new BytesStreamInput(buf));
            } else {
                return null;
            }
        } catch (KeeperException.NoNodeException e) {
            return null;
        } catch (KeeperException e) {
            throw new ZooKeeperClientException("Cannot get data for node " + id, e);
        } catch (IOException e) {
            throw new ZooKeeperClientException("Cannot register node " + id, e);
        }
    }

    @Override public void localNode(DiscoveryNode localNode) {
        this.localNode = localNode;
    }

    @Override public void syncClusterState() throws ElasticSearchException, InterruptedException {
        // To prepare for publishing master state, make sure that we are in sync with zooKeeper
        retrieveClusterState(null);
    }

    @Override public void unregisterNode(final String id) throws ElasticSearchException, InterruptedException {
        if (!lifecycle.started()) {
            throw new ZooKeeperClientException("unregisterNode is called after was service stopped");
        }
        try {
            final String nodePath = nodePath(id);
            zooKeeperCall("Cannot create leader node", new Callable<Object>() {
                @Override public Object call() throws Exception {
                    zooKeeper.delete(nodePath, -1);
                    return null;
                }
            });
        } catch (KeeperException e) {
            throw new ZooKeeperClientException("Cannot unregister node" + id, e);
        }
    }

    @Override public Set<String> listNodes(final NodeListChangedListener listener) throws ElasticSearchException, InterruptedException {
        if (!lifecycle.started()) {
            throw new ZooKeeperClientException("listNodes is called after was service stopped");
        }
        Set<String> res = new HashSet<String>();
        final Watcher watcher = (listener != null) ?
                new Watcher() {
                    @Override public void process(WatchedEvent event) {
                        if (event.getType() == Watcher.Event.EventType.NodeChildrenChanged) {
                            listener.onNodeListChanged();
                        }
                    }
                } : null;
        try {

            List<String> children = zooKeeperCall("Cannot list nodes", new Callable<List<String>>() {
                @Override public List<String> call() throws Exception {
                    return zooKeeper.getChildren(environment.nodesNodePath(), watcher);
                }
            });

            if (children == null) {
                return null;
            }
            for (String path : children) {
                res.add(extractId(path));
            }
            return res;
        } catch (KeeperException e) {
            throw new ZooKeeperClientException("Cannot list nodes", e);
        }
    }

    @Override public long sessionId() {
        if (!lifecycle.started()) {
            throw new ZooKeeperClientException("sessionId is called after was service stopped");
        }
        return zooKeeper.getSessionId();
    }

    @Override public boolean connected() {
        return zooKeeper != null && zooKeeper.getState().isAlive();
    }


    @Override public void publishClusterState(ClusterState state) throws ElasticSearchException, InterruptedException {
        publishingLock.lock();
        try {
            final String statePath = environment.stateNodePath() + "/" + "state";
            final BytesStreamOutput buf = new BytesStreamOutput();
            buf.writeLong(state.version());
            for (ClusterStatePart<?> part : this.parts) {
                buf.writeUTF(part.publishClusterStatePart(state));
            }
            zooKeeperCall("Cannot publish state", new Callable<Object>() {
                @Override public Object call() throws Exception {
                    if (zooKeeper.exists(statePath, null) != null) {
                        zooKeeper.setData(statePath, buf.copiedByteArray(), -1);
                    } else {
                        zooKeeper.create(statePath, buf.copiedByteArray(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
                    }
                    return null;
                }
            });

            // Cleaning cached version of cluster parts
            for (ClusterStatePart<?> part : this.parts) {
                part.purge();
            }
        } catch (KeeperException e) {
            throw new ZooKeeperClientException("Cannot publish state", e);
        } catch (IOException e) {
            throw new ZooKeeperClientException("Cannot publish state", e);
        } finally {
            publishingLock.unlock();
        }


    }

    @Override public ClusterState retrieveClusterState(final NewClusterStateListener newClusterStateListener) throws ElasticSearchException, InterruptedException {
        final Watcher watcher = (newClusterStateListener != null) ?
                new Watcher() {
                    @Override public void process(WatchedEvent event) {
                        try {
                            logger.trace("retrieveClusterState {} ", event);
                            if (event.getType() == Watcher.Event.EventType.NodeDataChanged) {
                                ClusterState clusterState = retrieveClusterState(newClusterStateListener);
                                if (clusterState != null) {
                                    newClusterStateListener.onNewClusterState(clusterState);
                                }
                            }
                        } catch (InterruptedException ex) {
                            // Ignore
                        }
                    }
                } : null;
        final Watcher createdWatcher = (newClusterStateListener != null) ?
                new Watcher() {
                    @Override public void process(WatchedEvent event) {
                        try {
                            logger.trace("retrieveClusterState {} ", event);
                            if (event.getType() == Watcher.Event.EventType.NodeCreated) {
                                ClusterState clusterState = retrieveClusterState(newClusterStateListener);
                                if (clusterState != null) {
                                    newClusterStateListener.onNewClusterState(clusterState);
                                }
                            }
                        } catch (InterruptedException ex) {
                            // Ignore
                        }
                    }
                } : null;
        publishingLock.lock();
        try {
            final String statePath = environment.stateNodePath() + "/" + "state";
            byte[] stateBuf = zooKeeperCall("Cannot read state node", new Callable<byte[]>() {
                @Override public byte[] call() throws Exception {
                    if(zooKeeper.exists(statePath, createdWatcher) != null) {
                        return zooKeeper.getData(statePath, watcher, null);
                    } else {
                        return null;
                    }
                }
            });
            if(stateBuf == null) {
                return null;
            }
            final BytesStreamInput buf = new BytesStreamInput(stateBuf);
            ClusterState.Builder builder = ClusterState.newClusterStateBuilder()
                    .version(buf.readLong());
            for (ClusterStatePart<?> part : this.parts) {
                builder = part.set(builder, buf.readUTF());
                if (builder == null) {
                    return null;
                }
            }

            return builder.build();
        } catch (KeeperException.NoNodeException ex) {
            return null;
        } catch (KeeperException e) {
            throw new ZooKeeperClientException("Cannot retrieve state", e);
        } catch (IOException e) {
            throw new ZooKeeperClientException("Cannot retrieve state", e);
        } finally {
            publishingLock.unlock();
        }
    }

    private void initClusterStatePersistence() {
        parts.add(new ClusterStatePart<RoutingTable>("routingTable") {
            @Override public void writeTo(RoutingTable statePart, StreamOutput out) throws IOException {
                RoutingTable.Builder.writeTo(statePart, out);
            }

            @Override public RoutingTable readFrom(StreamInput in) throws IOException {
                return RoutingTable.Builder.readFrom(in);
            }

            @Override public RoutingTable get(ClusterState state) {
                return state.getRoutingTable();
            }

            @Override public ClusterState.Builder set(ClusterState.Builder builder, RoutingTable val) {
                return builder.routingTable(val);
            }
        });
        parts.add(new ClusterStatePart<DiscoveryNodes>("discoveryNodes") {
            @Override public void writeTo(DiscoveryNodes statePart, StreamOutput out) throws IOException {
                DiscoveryNodes.Builder.writeTo(statePart, out);
            }

            @Override public DiscoveryNodes readFrom(StreamInput in) throws IOException {
                return DiscoveryNodes.Builder.readFrom(in, localNode);
            }

            @Override public DiscoveryNodes get(ClusterState state) {
                return state.getNodes();
            }

            @Override public ClusterState.Builder set(ClusterState.Builder builder, DiscoveryNodes val) {
                return builder.nodes(val);
            }
        });
        parts.add(new ClusterStatePart<MetaData>("metaData") {
            @Override public void writeTo(MetaData statePart, StreamOutput out) throws IOException {
                MetaData.Builder.writeTo(statePart, out);
            }

            @Override public MetaData readFrom(StreamInput in) throws IOException {
                return MetaData.Builder.readFrom(in);
            }

            @Override public MetaData get(ClusterState state) {
                return state.metaData();
            }

            @Override public ClusterState.Builder set(ClusterState.Builder builder, MetaData val) {
                return builder.metaData(val);
            }
        });
        parts.add(new ClusterStatePart<ClusterBlocks>("clusterBlocks") {
            @Override public void writeTo(ClusterBlocks statePart, StreamOutput out) throws IOException {
                ClusterBlocks.Builder.writeClusterBlocks(statePart, out);
            }

            @Override public ClusterBlocks readFrom(StreamInput in) throws IOException {
                return ClusterBlocks.Builder.readClusterBlocks(in);
            }

            @Override public ClusterBlocks get(ClusterState state) {
                return state.blocks();
            }

            @Override public ClusterState.Builder set(ClusterState.Builder builder, ClusterBlocks val) {
                return builder.blocks(val);
            }
        });
        parts.add(new ClusterStatePart<AllocationExplanation>("allocationExplanation") {
            @Override public void writeTo(AllocationExplanation statePart, StreamOutput out) throws IOException {
                statePart.writeTo(out);
            }

            @Override public AllocationExplanation readFrom(StreamInput in) throws IOException {
                return AllocationExplanation.readAllocationExplanation(in);
            }

            @Override public AllocationExplanation get(ClusterState state) {
                return state.allocationExplanation();
            }

            @Override public ClusterState.Builder set(ClusterState.Builder builder, AllocationExplanation val) {
                return builder.allocationExplanation(val);
            }
        });
    }

    private void createNode(final String path) throws ElasticSearchException, InterruptedException {
        if(!path.startsWith("/")) {
            throw new ZooKeeperClientException("Path " + path + " doesn't start with \"/\"");
        }
        try {
            zooKeeperCall("Cannot create leader node", new Callable<Object>() {
                @Override public Object call() throws Exception {
                    String[] nodes = path.split("/");
                    String currentPath = "";
                    for(int i=1; i<nodes.length; i++) {
                        currentPath = currentPath + "/" + nodes[i];
                        if (zooKeeper.exists(currentPath, null) == null) {
                            zooKeeper.create(currentPath, new byte[0], ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
                        }
                    }
                    return null;
                }
            });
        } catch (KeeperException.NodeExistsException e) {
            // Ignore - node was already created
        } catch (KeeperException e) {
            throw new ZooKeeperClientException("Cannot create node at " + path, e);
        }
    }

    private String nodePath(String id) {
        return environment.nodesNodePath() + "/" + id;
    }

    private String extractId(String path) {
        int index = path.lastIndexOf('/');
        if (index >= 0) {
            return path.substring(index + 1);
        } else {
            return path;
        }
    }

    private <T> T zooKeeperCall(String reason, Callable<T> callable) throws InterruptedException, KeeperException {
        while (true) {
            try {
                return callable.call();
            } catch (KeeperException.ConnectionLossException ex) {
                // Retry
            } catch (KeeperException.SessionExpiredException e) {
                throw new ZooKeeperClientException(reason, e);
            } catch (KeeperException e) {
                throw e;
            } catch (InterruptedException e) {
                throw e;
            } catch (Exception e) {
                throw new ZooKeeperClientException(reason, e);
            }
        }
    }

    private abstract class ClusterStatePart<T> {
        private String statePartName;

        private T cached;

        private String cachedPath;

        private String previousPath;

        public ClusterStatePart(String statePartName) {
            this.statePartName = statePartName;
        }

        public String publishClusterStatePart(ClusterState state) throws ElasticSearchException, InterruptedException {
            T statePart = get(state);
            if (statePart.equals(cached)) {
                return cachedPath;
            } else {
                String path = internalPublishClusterStatePart(statePart);
                cached = statePart;
                previousPath = cachedPath;
                cachedPath = path;
                return path;
            }
        }

        private String internalPublishClusterStatePart(T statePart) throws ElasticSearchException, InterruptedException {
            final String path = environment.stateNodePath() + "/" + statePartName + "_";
            String rootPath;
            try {
                BytesStreamOutput streamOutput = new BytesStreamOutput();
                writeTo(statePart, streamOutput);
                final int size = streamOutput.size();
                // Create Root node with version and size of the state part
                rootPath = zooKeeperCall("Cannot " + statePartName + " node", new Callable<String>() {
                    @Override public String call() throws Exception {
                        return zooKeeper.create(path, Bytes.itoa(size), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT_SEQUENTIAL);
                    }
                });
                int chunkNum = 0;
                // Store state part in chunks in case it's too big for a single node
                // It should be able to fit into a single node in most cases
                for (int i = 0; i < size; i += maxNodeSize) {
                    final String chunkPath = rootPath + "/" + chunkNum;
                    final byte[] chunk = Arrays.copyOfRange(streamOutput.unsafeByteArray(), i, Math.min(size, i + maxNodeSize));
                    zooKeeperCall("Cannot " + statePartName + " node", new Callable<String>() {
                        @Override public String call() throws Exception {
                            return zooKeeper.create(chunkPath, chunk, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
                        }
                    });
                    chunkNum++;
                }
            } catch (KeeperException e) {
                throw new ZooKeeperClientException("Cannot create node at " + path, e);
            } catch (IOException e) {
                throw new ZooKeeperClientException("Cannot read " + statePartName + " node at " + path, e);
            }
            return rootPath;
        }

        public T getClusterStatePart(String path) throws ElasticSearchException, InterruptedException {
            if (path.equals(cachedPath)) {
                return cached;
            } else {
                cached = internalGetStatePart(path);
                cachedPath = path;
                return cached;
            }

        }

        public void purge() throws ElasticSearchException, InterruptedException {
            if (previousPath != null) {
                try {
                    zooKeeperCall("Cannot delete " + statePartName + " node at " + previousPath, new Callable<Object>() {
                        @Override public Object call() throws Exception {
                            List<String> children = zooKeeper.getChildren(previousPath, null);
                            for (String child : children) {
                                zooKeeper.delete(previousPath + "/" + child, -1);
                            }
                            zooKeeper.delete(previousPath, -1);
                            return null;
                        }
                    });
                    previousPath = null;
                } catch (KeeperException e) {
                    throw new ZooKeeperClientException("Cannot purge " + statePartName + " node at " + previousPath, e);
                }
            }
        }

        public T internalGetStatePart(final String path) throws ElasticSearchException, InterruptedException {
            try {
                byte[] sizeBuf = zooKeeperCall("Cannot read " + statePartName + " node", new Callable<byte[]>() {
                    @Override public byte[] call() throws Exception {
                        return zooKeeper.getData(path, null, null);
                    }
                });
                final int size = Bytes.atoi(sizeBuf);
                int chunkNum = 0;

                BytesStreamOutput buf = new BytesStreamOutput(size);
                for (int i = 0; i < size; i += maxNodeSize) {
                    final String chunkPath = path + "/" + chunkNum;
                    byte[] chunk = zooKeeperCall("Cannot read " + statePartName + " node", new Callable<byte[]>() {
                        @Override public byte[] call() throws Exception {
                            return zooKeeper.getData(chunkPath, null, null);
                        }
                    });
                    buf.write(chunk);
                    chunkNum++;
                }
                return readFrom(new BytesStreamInput(buf.copiedByteArray(), 0, buf.size()));
            } catch (KeeperException.NoNodeException e) {
                // This means that a new version of state is already posted and this version is
                // getting deleted - exit
                return null;
            } catch (KeeperException e) {
                throw new ZooKeeperClientException("Cannot read " + statePartName + " node at " + path, e);
            } catch (IOException e) {
                throw new ZooKeeperClientException("Cannot read " + statePartName + " node at " + path, e);
            }
        }

        public ClusterState.Builder set(ClusterState.Builder builder, String path) throws ElasticSearchException, InterruptedException {
            T val = getClusterStatePart(path);
            if (val == null) {
                return null;
            } else {
                return set(builder, val);
            }

        }

        public abstract void writeTo(T statePart, StreamOutput out) throws IOException;

        public abstract T readFrom(StreamInput in) throws IOException;

        public abstract T get(ClusterState state);

        public abstract ClusterState.Builder set(ClusterState.Builder builder, T val);

    }

}
