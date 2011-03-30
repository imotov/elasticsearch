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
import org.elasticsearch.discovery.zookeeper.client.NodeCreatedListener;
import org.elasticsearch.discovery.zookeeper.client.NodeDeletedListener;
import org.elasticsearch.discovery.zookeeper.client.NodeListChangedListener;
import org.elasticsearch.discovery.zookeeper.client.ZookeeperClient;
import org.testng.annotations.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.hamcrest.MatcherAssert.*;
import static org.hamcrest.Matchers.*;

/**
 * @author imotov
 */
public class ZookeeperClientTests extends AbstractZookeeperTests {

    @Test public void testStartStop() {

    }

    @Test public void testElectionSequence() throws Exception {
        ZookeeperClient zk1 = buildZookeeper();
        ZookeeperClient zk2 = buildZookeeper();
        final boolean[] callbackForSelf = new boolean[1];

        assertThat(zk1.electMaster("id1", new NodeDeletedListener() {
            @Override public void onNodeDeleted(String id) {
                callbackForSelf[0] = true;
            }
        }), equalTo("id1"));

        final CountDownLatch latch = new CountDownLatch(1);

        assertThat(zk2.electMaster("id2", new NodeDeletedListener() {
            @Override public void onNodeDeleted(String id) {
                latch.countDown();
            }
        }), equalTo("id1"));

        zk1.stop();

        assertThat(latch.await(10, TimeUnit.SECONDS), equalTo(true));

        assertThat(zk2.electMaster("id2", null), equalTo("id2"));
        assertThat(callbackForSelf[0], equalTo(true));

    }

    @Test public void testThreeNodeElection() throws Exception {
        ZookeeperClient zk1 = buildZookeeper();
        final ZookeeperClient zk2 = buildZookeeper();
        final ZookeeperClient zk3 = buildZookeeper();
        final String[] masters = new String[2];

        assertThat(zk1.electMaster("id1", null), equalTo("id1"));

        final CountDownLatch latch = new CountDownLatch(2);

        assertThat(zk2.electMaster("id2", new NodeDeletedListener() {
            @Override public void onNodeDeleted(String id) {
                try {
                    masters[0] = zk2.electMaster("id2", null);
                } catch (InterruptedException ex) {
                    throw new ElasticSearchException("Thread interrupted", ex);
                }
                latch.countDown();
            }
        }), equalTo("id1"));

        assertThat(zk3.electMaster("id3", new NodeDeletedListener() {
            @Override public void onNodeDeleted(String id) {
                try {
                    masters[1] = zk2.electMaster("id2", null);
                } catch (InterruptedException ex) {
                    throw new ElasticSearchException("Thread interrupted", ex);
                }
                latch.countDown();
            }
        }), equalTo("id1"));

        zk1.stop();

        assertThat(latch.await(1, TimeUnit.SECONDS), equalTo(true));

        assertThat(masters[0], anyOf(equalTo("id2"), equalTo("id3")));
        assertThat(masters[0], equalTo(masters[1]));
        logger.error("New Master is " + masters[0]);
    }

    @Test public void testDontElect() throws Exception {
    }

    @Test public void testRegisterNode() throws Exception {
        ZookeeperClient zk1 = buildZookeeper();

        final CountDownLatch latch = new CountDownLatch(1);

        zk1.registerNode("node1", "Node 1".getBytes(), new NodeDeletedListener() {
            @Override public void onNodeDeleted(String id) {
                assertThat(id, equalTo("node1"));
                latch.countDown();
            }
        });

        zk1.unregisterNode("node1");

        assertThat(latch.await(1, TimeUnit.SECONDS), equalTo(true));

    }

    @Test public void testNodeInfo() throws Exception {
        ZookeeperClient zk1 = buildZookeeper();

        zk1.registerNode("node1", "Node 1".getBytes(), null);

        assertThat(zk1.nodeInfo("node1"), equalTo("Node 1".getBytes()));
        assertThat(zk1.nodeInfo("node2"), nullValue());

    }


    private class RelistListener implements NodeListChangedListener {

        private ZookeeperClient zk;
        private List<List<String>> lists;
        private CountDownLatch latch;

        public RelistListener(ZookeeperClient zk, List<List<String>> lists, CountDownLatch latch) {
            this.zk = zk;
            this.lists = lists;
            this.latch = latch;
        }

        @Override public synchronized void onNodeListChanged() {
            NodeListChangedListener listener = null;
            if (latch.getCount() > 1) {
                listener = this;
            }
            try {
                Set<String> res = zk.listNodes(listener);
                List<String> resList = new ArrayList<String>(res);
                Collections.sort(resList);
                lists.add(resList);
                latch.countDown();
            } catch (InterruptedException ex) {
                throw new ElasticSearchException("Thread interrupted", ex);
            }
        }

    }

    @Test public void testListNodes() throws Exception {
        List<List<String>> lists = new ArrayList<List<String>>();
        ZookeeperClient zk1 = buildZookeeper();
        CountDownLatch latch = new CountDownLatch(4);
        RelistListener listener = new RelistListener(zk1, lists, latch);
        assertThat(zk1.listNodes(listener).size(), equalTo(0));
        zk1.registerNode("id1", null, null);
        zk1.registerNode("id2", null, null);
        zk1.registerNode("id3", null, null);
        zk1.unregisterNode("id2");

        assertThat(latch.await(1, TimeUnit.SECONDS), equalTo(true));

        assertThat(lists.get(0).toArray(), equalTo(new Object[]{"id1"}));
        assertThat(lists.get(1).toArray(), equalTo(new Object[]{"id1", "id2"}));
        assertThat(lists.get(2).toArray(), equalTo(new Object[]{"id1", "id2", "id3"}));
        assertThat(lists.get(3).toArray(), equalTo(new Object[]{"id1", "id3"}));
        assertThat(lists.size(), equalTo(4));
    }

    @Test public void testFindMasterWithNoInitialMaster() throws Exception {
        ZookeeperClient zk1 = buildZookeeper();
        ZookeeperClient zk2 = buildZookeeper();
        final AtomicBoolean deletedCalled = new AtomicBoolean();
        final CountDownLatch latch = new CountDownLatch(1);

        assertThat(zk1.findMaster(new NodeCreatedListener() {
            @Override public void onNodeCreated(String id) {
                latch.countDown();
            }
        }, new NodeDeletedListener() {
            @Override public void onNodeDeleted(String id) {
                deletedCalled.set(true);
            }
        }), nullValue());

        assertThat(zk2.electMaster("node1", null), equalTo("node1"));
        assertThat(latch.await(1, TimeUnit.SECONDS), equalTo(true));
        assertThat(deletedCalled.get(), equalTo(false));

    }

    @Test public void testFindMasterWithInitialMaster() throws Exception {
        ZookeeperClient zk1 = buildZookeeper();
        ZookeeperClient zk2 = buildZookeeper();
        final AtomicBoolean createdCalled = new AtomicBoolean();
        final AtomicBoolean deletedCalled = new AtomicBoolean();
        final CountDownLatch latch = new CountDownLatch(1);
        assertThat(zk1.electMaster("node1", null), equalTo("node1"));
        assertThat(zk2.findMaster(new NodeCreatedListener() {
            @Override public void onNodeCreated(String id) {
                createdCalled.set(true);
            }
        }, new NodeDeletedListener() {
            @Override public void onNodeDeleted(String id) {
                latch.countDown();
            }
        }), equalTo("node1"));

        assertThat(deletedCalled.get(), equalTo(false));
        zk1.stop();
        assertThat(latch.await(1, TimeUnit.SECONDS), equalTo(true));
        assertThat(createdCalled.get(), equalTo(false));


    }


}
