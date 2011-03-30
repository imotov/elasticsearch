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
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.common.component.AbstractLifecycleComponent;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;

import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;

/**
 * @author imotov
 */
public class ZookeeperClientService extends AbstractLifecycleComponent<ZookeeperClient> implements ZookeeperClient {

    private ZooKeeper zooKeeper;

    private String zooKeeperHost;

    private String clusterNode;

    private String nodesNode;

    private String rootNode;

    private TimeValue sessionTimeout;

    @Inject public ZookeeperClientService(Settings settings, ClusterName clusterName) {
        super(settings);
        zooKeeperHost = componentSettings.get("host");
        if (zooKeeperHost == null) {
            throw new ElasticSearchException("Empty ZooKeeper host name");
        }
        sessionTimeout = componentSettings.getAsTime("session.timeout", new TimeValue(1, TimeUnit.MINUTES));
        rootNode = componentSettings.get("root", "/es");
        clusterNode = rootNode + "/" + componentSettings.get("cluster", clusterName.value());
        nodesNode = clusterNode + "/" + "nodes";
    }

    @Override protected void doStart() throws ElasticSearchException {
        try {
            zooKeeper = zookeeperCall("Starting zookeeper client",
                    new Callable<ZooKeeper>() {
                        @Override public ZooKeeper call() throws Exception {
                            return new ZooKeeper(zooKeeperHost, (int)sessionTimeout.millis(), new Watcher() {
                                @Override public void process(WatchedEvent event) {
                                }
                            });
                        }
                    }
            );
            createNode(rootNode);
            createNode(clusterNode);
            createNode(nodesNode);
        } catch (InterruptedException e) {
            throw new ZookeeperClientException("Cannot start zookeeper client", e);
        } catch (KeeperException e) {
            throw new ZookeeperClientException("Cannot start zookeeper client", e);
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
            throw new ZookeeperClientException("electMaster is called after was service stopped");
        }
        if (id == null) {
            throw new ZookeeperClientException("electMaster is called with null id");
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
                byte[] leader = zookeeperCall("Getting master data", new Callable<byte[]>() {
                    @Override public byte[] call() throws Exception {
                        return zooKeeper.getData(masterPath(), watcher, null);
                    }
                });
                if (leader != null) {
                    return new String(leader);
                } else {
                    return null;
                }
            } catch (KeeperException.NoNodeException e) {
                try {
                    zookeeperCall("Cannot create leader node", new Callable<Object>() {
                        @Override public Object call() throws Exception {
                            zooKeeper.create(masterPath(), id.getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);
                            return null;
                        }
                    });
                } catch (KeeperException.NodeExistsException e1) {
                    // Ignore - somebody already created this node since the last time we checked
                } catch (KeeperException e1) {
                    throw new ZookeeperClientException("Cannot create leader node", e1);
                }
            } catch (KeeperException e) {
                throw new ZookeeperClientException("Cannot obtain leader node", e);
            }
        }
    }

    @Override public String findMaster(final NodeCreatedListener masterCreatedListener, final NodeDeletedListener masterDeletedListener) throws ElasticSearchException, InterruptedException {
        if (!lifecycle.started()) {
            throw new ZookeeperClientException("findMaster is called after was service stopped");
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
                Stat stat = zookeeperCall("Checking if master exists", new Callable<Stat>() {
                    @Override public Stat call() throws Exception {
                        return zooKeeper.exists(masterPath(), createdWatcher);
                    }
                });

                if (stat != null) {
                    byte[] leader = zookeeperCall("Getting master data", new Callable<byte[]>() {
                        @Override public byte[] call() throws Exception {
                            return zooKeeper.getData(masterPath(), deletedWatcher, null);
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
                throw new ZookeeperClientException("Cannot obtain leader node", e);
            }
        }
    }

    @Override public void registerNode(String id, final byte[] nodeInfo, final NodeDeletedListener listener) throws ElasticSearchException, InterruptedException {
        if (!lifecycle.started()) {
            throw new ZookeeperClientException("registerNode is called after service was stopped");
        }
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
                zookeeperCall("Registering node " + id, new Callable<Object>() {
                    @Override public Object call() throws Exception {
                        zooKeeper.create(nodePath, nodeInfo, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);
                        return null;
                    }
                });
            } catch (KeeperException.NodeExistsException e1) {
                // Ignore
            }
            zookeeperCall("Registering node " + id, new Callable<Stat>() {
                @Override public Stat call() throws Exception {
                    return zooKeeper.exists(nodePath, watcher);
                }
            });
        } catch (KeeperException e) {
            throw new ZookeeperClientException("Cannot register node " + id, e);
        }
    }

    @Override public byte[] nodeInfo(final String id) throws ElasticSearchException, InterruptedException {
        if (!lifecycle.started()) {
            throw new ZookeeperClientException("nodeInfo is called after was service stopped");
        }
        try {
            return zookeeperCall("Node Info for node " + id, new Callable<byte[]>() {
                @Override public byte[] call() throws Exception {
                    return zooKeeper.getData(nodePath(id), null, null);
                }
            });
        } catch (KeeperException.NoNodeException e) {
            return null;
        } catch (KeeperException e) {
            throw new ZookeeperClientException("Cannot get data for node " + id, e);
        }
    }

    @Override public void unregisterNode(final String id) throws ElasticSearchException, InterruptedException {
        if (!lifecycle.started()) {
            throw new ZookeeperClientException("unregisterNode is called after was service stopped");
        }
        try {
            final String nodePath = nodePath(id);
            zookeeperCall("Cannot create leader node", new Callable<Object>() {
                @Override public Object call() throws Exception {
                    zooKeeper.delete(nodePath, -1);
                    return null;
                }
            });
        } catch (KeeperException e) {
            throw new ZookeeperClientException("Cannot unregister node" + id, e);
        }
    }

    @Override public Set<String> listNodes(final NodeListChangedListener listener) throws ElasticSearchException, InterruptedException {
        if (!lifecycle.started()) {
            throw new ZookeeperClientException("listNodes is called after was service stopped");
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

            List<String> children = zookeeperCall("Cannot list nodes", new Callable<List<String>>() {
                @Override public List<String> call() throws Exception {
                    return zooKeeper.getChildren(nodesNode, watcher);
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
            throw new ZookeeperClientException("Cannot list nodes", e);
        }
    }

    @Override public long sessionId() {
        if (!lifecycle.started()) {
            throw new ZookeeperClientException("sessionId is called after was service stopped");
        }
        return zooKeeper.getSessionId();
    }

    @Override public boolean connected() {
        return zooKeeper != null && zooKeeper.getState().isAlive();
    }

    private void createNode(final String path) throws ElasticSearchException, InterruptedException {
        try {
            zookeeperCall("Cannot create leader node", new Callable<Object>() {
                @Override public Object call() throws Exception {
                    if (zooKeeper.exists(path, null) == null) {
                        zooKeeper.create(path, new byte[0], ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
                    }
                    return null;
                }
            });
        } catch (KeeperException.NodeExistsException e) {
            // Ignore - node was already created
        } catch (KeeperException e) {
            throw new ZookeeperClientException("Cannot create node at " + path, e);
        }
    }

    private String clusterPath(String path) {
        return clusterNode + "/" + path;
    }

    private String masterPath() {
        return clusterPath("leader");
    }

    private String nodePath(String id) {
        return nodesNode + "/" + id;
    }


    private String extractId(String path) {
        int index = path.lastIndexOf('/');
        if (index >= 0) {
            return path.substring(index + 1);
        } else {
            return path;
        }
    }

    private <T> T zookeeperCall(String reason, Callable<T> callable) throws InterruptedException, KeeperException {
        while (true) {
            try {
                return callable.call();
            } catch (KeeperException.ConnectionLossException ex) {
                // Retry
            } catch (KeeperException.SessionExpiredException e) {
                throw new ZookeeperClientException(reason, e);
            } catch (KeeperException e) {
                throw e;
            } catch (InterruptedException e) {
                throw e;
            } catch (Exception e) {
                throw new ZookeeperClientException(reason, e);
            }
        }
    }

}
