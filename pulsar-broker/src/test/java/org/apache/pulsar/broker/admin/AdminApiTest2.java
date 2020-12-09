/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.pulsar.broker.admin;

import static org.apache.commons.lang3.StringUtils.isBlank;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import javax.ws.rs.core.Response.Status;
import lombok.Cleanup;
import lombok.extern.slf4j.Slf4j;
import org.apache.bookkeeper.mledger.impl.ManagedCursorImpl;
import org.apache.bookkeeper.mledger.impl.ManagedLedgerImpl;
import org.apache.pulsar.broker.PulsarService;
import org.apache.pulsar.broker.admin.AdminApiTest.MockedPulsarService;
import org.apache.pulsar.broker.auth.MockedPulsarServiceBaseTest;
import org.apache.pulsar.broker.loadbalance.impl.ModularLoadManagerImpl;
import org.apache.pulsar.broker.loadbalance.impl.SimpleLoadManagerImpl;
import org.apache.pulsar.broker.service.Topic;
import org.apache.pulsar.broker.service.persistent.PersistentSubscription;
import org.apache.pulsar.broker.service.persistent.PersistentTopic;
import org.apache.pulsar.client.admin.PulsarAdmin;
import org.apache.pulsar.client.admin.PulsarAdminException;
import org.apache.pulsar.client.admin.PulsarAdminException.PreconditionFailedException;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.MessageRoutingMode;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.ProxyProtocol;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.SubscriptionInitialPosition;
import org.apache.pulsar.client.api.SubscriptionType;
import org.apache.pulsar.client.impl.MessageIdImpl;
import org.apache.pulsar.common.naming.TopicDomain;
import org.apache.pulsar.common.naming.TopicName;
import org.apache.pulsar.common.policies.data.AutoFailoverPolicyData;
import org.apache.pulsar.common.policies.data.AutoFailoverPolicyType;
import org.apache.pulsar.common.policies.data.BrokerNamespaceIsolationData;
import org.apache.pulsar.common.policies.data.ClusterData;
import org.apache.pulsar.common.policies.data.ConsumerStats;
import org.apache.pulsar.common.policies.data.FailureDomain;
import org.apache.pulsar.common.policies.data.NamespaceIsolationData;
import org.apache.pulsar.common.policies.data.PartitionedTopicStats;
import org.apache.pulsar.common.policies.data.PersistencePolicies;
import org.apache.pulsar.common.policies.data.PersistentTopicInternalStats;
import org.apache.pulsar.common.policies.data.RetentionPolicies;
import org.apache.pulsar.common.policies.data.SubscriptionStats;
import org.apache.pulsar.common.policies.data.TenantInfo;
import org.apache.pulsar.common.policies.data.TopicStats;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

@Slf4j
public class AdminApiTest2 extends MockedPulsarServiceBaseTest {

    private MockedPulsarService mockPulsarSetup;

    @BeforeMethod
    @Override
    public void setup() throws Exception {
        conf.setLoadBalancerEnabled(true);
        super.internalSetup();

        // create otherbroker to test redirect on calls that need
        // namespace ownership
        mockPulsarSetup = new MockedPulsarService(this.conf);
        mockPulsarSetup.setup();

        // Setup namespaces
        admin.clusters().createCluster("test", new ClusterData(pulsar.getWebServiceAddress()));
        TenantInfo tenantInfo = new TenantInfo(Sets.newHashSet("role1", "role2"), Sets.newHashSet("test"));
        admin.tenants().createTenant("prop-xyz", tenantInfo);
        admin.namespaces().createNamespace("prop-xyz/ns1", Sets.newHashSet("test"));
    }

    @AfterMethod(alwaysRun = true)
    @Override
    public void cleanup() throws Exception {
        super.internalCleanup();
        mockPulsarSetup.cleanup();
    }

    @DataProvider(name = "topicType")
    public Object[][] topicTypeProvider() {
        return new Object[][] { { TopicDomain.persistent.value() }, { TopicDomain.non_persistent.value() } };
    }

    @DataProvider(name = "namespaceNames")
    public Object[][] namespaceNameProvider() {
        return new Object[][] { { "ns1" }, { "global" } };
    }

    /**
     * <pre>
     * It verifies increasing partitions for partitioned-topic.
     * 1. create a partitioned-topic
     * 2. update partitions with larger number of partitions
     * 3. verify: getPartitionedMetadata and check number of partitions
     * 4. verify: this api creates existing subscription to new partitioned-topics
     *            so, message will not be lost in new partitions
     *  a. start producer and produce messages
     *  b. check existing subscription for new topics and it should have backlog msgs
     *
     * </pre>
     *
     * @throws Exception
     */
    @Test
    public void testIncrementPartitionsOfTopic() throws Exception {
        final String topicName = "increment-partitionedTopic";
        final String subName1 = topicName + "-my-sub-1/encode";
        final String subName2 = topicName + "-my-sub-2/encode";
        final int startPartitions = 4;
        final int newPartitions = 8;
        final String partitionedTopicName = "persistent://prop-xyz/ns1/" + topicName;

        URL pulsarUrl = new URL(pulsar.getWebServiceAddress());

        admin.topics().createPartitionedTopic(partitionedTopicName, startPartitions);
        // validate partition topic is created
        assertEquals(admin.topics().getPartitionedTopicMetadata(partitionedTopicName).partitions,
                startPartitions);

        // create consumer and subscriptions : check subscriptions
        @Cleanup
        PulsarClient client = PulsarClient.builder().serviceUrl(pulsarUrl.toString()).build();
        Consumer<byte[]> consumer1 = client.newConsumer().topic(partitionedTopicName).subscriptionName(subName1)
                .subscriptionType(SubscriptionType.Shared).subscribe();
        assertEquals(admin.topics().getSubscriptions(partitionedTopicName), Lists.newArrayList(subName1));
        Consumer<byte[]> consumer2 = client.newConsumer().topic(partitionedTopicName).subscriptionName(subName2)
                .subscriptionType(SubscriptionType.Shared).subscribe();
        assertEquals(Sets.newHashSet(admin.topics().getSubscriptions(partitionedTopicName)),
                Sets.newHashSet(subName1, subName2));

        // (1) update partitions
        admin.topics().updatePartitionedTopic(partitionedTopicName, newPartitions);
        // invalidate global-cache to make sure that mock-zk-cache reds fresh data
        pulsar.getGlobalZkCache().invalidateAll();
        // verify new partitions have been created
        assertEquals(admin.topics().getPartitionedTopicMetadata(partitionedTopicName).partitions,
                newPartitions);
        // (2) No Msg loss: verify new partitions have the same existing subscription names
        final String newPartitionTopicName = TopicName.get(partitionedTopicName).getPartition(startPartitions + 1)
                .toString();

        // (3) produce messages to all partitions including newly created partitions (RoundRobin)
        Producer<byte[]> producer = client.newProducer()
            .topic(partitionedTopicName)
            .enableBatching(false)
            .messageRoutingMode(MessageRoutingMode.RoundRobinPartition)
            .create();
        final int totalMessages = newPartitions * 2;
        for (int i = 0; i < totalMessages; i++) {
            String message = "message-" + i;
            producer.send(message.getBytes());
        }

        // (4) verify existing subscription has not lost any message: create new consumer with sub-2: it will load all
        // newly created partition topics
        consumer2.close();
        consumer2 = client.newConsumer().topic(partitionedTopicName).subscriptionName(subName2)
                .subscriptionType(SubscriptionType.Shared).subscribe();
        // sometime: mockZk fails to refresh ml-cache: so, invalidate the cache to get fresh data
        pulsar.getLocalZkCacheService().managedLedgerListCache().clearTree();
        assertEquals(Sets.newHashSet(admin.topics().getSubscriptions(newPartitionTopicName)),
                Sets.newHashSet(subName1, subName2));

        assertEquals(Sets.newHashSet(admin.topics().getList("prop-xyz/ns1")).size(), newPartitions);

        // test cumulative stats for partitioned topic
        PartitionedTopicStats topicStats = admin.topics().getPartitionedStats(partitionedTopicName, false);
        assertEquals(topicStats.subscriptions.keySet(), Sets.newTreeSet(Lists.newArrayList(subName1, subName2)));
        assertEquals(topicStats.subscriptions.get(subName2).consumers.size(), 1);
        assertEquals(topicStats.subscriptions.get(subName2).msgBacklog, totalMessages);
        assertEquals(topicStats.publishers.size(), 1);
        assertEquals(topicStats.partitions, Maps.newHashMap());

        // (5) verify: each partition should have backlog
        topicStats = admin.topics().getPartitionedStats(partitionedTopicName, true);
        assertEquals(topicStats.metadata.partitions, newPartitions);
        Set<String> partitionSet = Sets.newHashSet();
        for (int i = 0; i < newPartitions; i++) {
            partitionSet.add(partitionedTopicName + "-partition-" + i);
        }
        assertEquals(topicStats.partitions.keySet(), partitionSet);
        for (int i = 0; i < newPartitions; i++) {
            TopicStats partitionStats = topicStats.partitions
                    .get(TopicName.get(partitionedTopicName).getPartition(i).toString());
            assertEquals(partitionStats.publishers.size(), 1);
            assertEquals(partitionStats.subscriptions.get(subName2).consumers.size(), 1);
            assertEquals(partitionStats.subscriptions.get(subName2).msgBacklog, 2, 1);
        }

        producer.close();
        consumer1.close();
        consumer2.close();
        consumer2.close();
    }

    /**
     * verifies admin api command for non-persistent topic. It verifies: partitioned-topic, stats
     *
     * @throws Exception
     */
    @Test
    public void nonPersistentTopics() throws Exception {
        final String topicName = "nonPersistentTopic";

        final String persistentTopicName = "non-persistent://prop-xyz/ns1/" + topicName;
        // Force to create a topic
        publishMessagesOnTopic("non-persistent://prop-xyz/ns1/" + topicName, 0, 0);

        // create consumer and subscription
        @Cleanup
        PulsarClient client = PulsarClient.builder()
                .serviceUrl(pulsar.getWebServiceAddress())
                .statsInterval(0, TimeUnit.SECONDS)
                .build();
        Consumer<byte[]> consumer = client.newConsumer().topic(persistentTopicName).subscriptionName("my-sub")
                .subscribe();

        publishMessagesOnTopic("non-persistent://prop-xyz/ns1/" + topicName, 10, 0);

        TopicStats topicStats = admin.topics().getStats(persistentTopicName);
        assertEquals(topicStats.subscriptions.keySet(), Sets.newTreeSet(Lists.newArrayList("my-sub")));
        assertEquals(topicStats.subscriptions.get("my-sub").consumers.size(), 1);
        assertEquals(topicStats.publishers.size(), 0);

        PersistentTopicInternalStats internalStats = admin.topics().getInternalStats(persistentTopicName, false);
        assertEquals(internalStats.cursors.keySet(), Sets.newTreeSet(Lists.newArrayList("my-sub")));

        consumer.close();

        topicStats = admin.topics().getStats(persistentTopicName);
        assertTrue(topicStats.subscriptions.keySet().contains("my-sub"));
        assertEquals(topicStats.publishers.size(), 0);

        // test partitioned-topic
        final String partitionedTopicName = "non-persistent://prop-xyz/ns1/paritioned";
        assertEquals(admin.topics().getPartitionedTopicMetadata(partitionedTopicName).partitions, 0);
        admin.topics().createPartitionedTopic(partitionedTopicName, 5);
        assertEquals(admin.topics().getPartitionedTopicMetadata(partitionedTopicName).partitions, 5);
    }

    private void publishMessagesOnTopic(String topicName, int messages, int startIdx) throws Exception {
        Producer<byte[]> producer = pulsarClient.newProducer()
            .topic(topicName)
            .enableBatching(false)
            .messageRoutingMode(MessageRoutingMode.SinglePartition)
            .create();

        for (int i = startIdx; i < (messages + startIdx); i++) {
            String message = "message-" + i;
            producer.send(message.getBytes());
        }

        producer.close();
    }

    /**
     * verifies validation on persistent-policies
     *
     * @throws Exception
     */
    @Test
    public void testSetPersistencepolicies() throws Exception {

        final String namespace = "prop-xyz/ns2";
        admin.namespaces().createNamespace(namespace, Sets.newHashSet("test"));

        assertEquals(admin.namespaces().getPersistence(namespace), new PersistencePolicies(2, 2, 2, 0.0));
        admin.namespaces().setPersistence(namespace, new PersistencePolicies(3, 3, 3, 10.0));
        assertEquals(admin.namespaces().getPersistence(namespace), new PersistencePolicies(3, 3, 3, 10.0));

        try {
            admin.namespaces().setPersistence(namespace, new PersistencePolicies(3, 4, 3, 10.0));
            fail("should have failed");
        } catch (PulsarAdminException e) {
            assertEquals(e.getStatusCode(), 400);
        }
        try {
            admin.namespaces().setPersistence(namespace, new PersistencePolicies(3, 3, 4, 10.0));
            fail("should have failed");
        } catch (PulsarAdminException e) {
            assertEquals(e.getStatusCode(), 400);
        }
        try {
            admin.namespaces().setPersistence(namespace, new PersistencePolicies(6, 3, 1, 10.0));
            fail("should have failed");
        } catch (PulsarAdminException e) {
            assertEquals(e.getStatusCode(), 400);
        }

        // make sure policies has not been changed
        assertEquals(admin.namespaces().getPersistence(namespace), new PersistencePolicies(3, 3, 3, 10.0));
    }

    /**
     * validates update of persistent-policies reflects on managed-ledger and managed-cursor
     *
     * @throws Exception
     */
    @Test
    public void testUpdatePersistencePolicyUpdateManagedCursor() throws Exception {

        final String namespace = "prop-xyz/ns2";
        final String topicName = "persistent://" + namespace + "/topic1";
        admin.namespaces().createNamespace(namespace, Sets.newHashSet("test"));

        admin.namespaces().setPersistence(namespace, new PersistencePolicies(3, 3, 3, 50.0));
        assertEquals(admin.namespaces().getPersistence(namespace), new PersistencePolicies(3, 3, 3, 50.0));

        Producer<byte[]> producer = pulsarClient.newProducer()
            .topic(topicName)
            .enableBatching(false)
            .messageRoutingMode(MessageRoutingMode.SinglePartition)
            .create();
        Consumer<byte[]> consumer = pulsarClient.newConsumer().topic(topicName).subscriptionName("my-sub").subscribe();

        PersistentTopic topic = (PersistentTopic) pulsar.getBrokerService().getOrCreateTopic(topicName).get();
        ManagedLedgerImpl managedLedger = (ManagedLedgerImpl) topic.getManagedLedger();
        ManagedCursorImpl cursor = (ManagedCursorImpl) managedLedger.getCursors().iterator().next();

        final double newThrottleRate = 100;
        final int newEnsembleSize = 5;
        admin.namespaces().setPersistence(namespace, new PersistencePolicies(newEnsembleSize, 3, 3, newThrottleRate));

        retryStrategically((test) -> managedLedger.getConfig().getEnsembleSize() == newEnsembleSize
                && cursor.getThrottleMarkDelete() != newThrottleRate, 5, 200);

        // (1) verify cursor.markDelete has been updated
        assertEquals(cursor.getThrottleMarkDelete(), newThrottleRate);

        // (2) verify new ledger creation takes new config

        producer.close();
        consumer.close();

    }

    /**
     * Verify unloading topic
     *
     * @throws Exception
     */
    @Test(dataProvider = "topicType")
    public void testUnloadTopic(final String topicType) throws Exception {

        final String namespace = "prop-xyz/ns2";
        final String topicName = topicType + "://" + namespace + "/topic1";
        admin.namespaces().createNamespace(namespace, Sets.newHashSet("test"));

        // create a topic by creating a producer
        Producer<byte[]> producer = pulsarClient.newProducer().topic(topicName).create();
        producer.close();

        Topic topic = pulsar.getBrokerService().getTopicIfExists(topicName).join().get();
        final boolean isPersistentTopic = topic instanceof PersistentTopic;

        // (1) unload the topic
        unloadTopic(topicName, isPersistentTopic);

        // topic must be removed from map
        assertFalse(pulsar.getBrokerService().getTopicReference(topicName).isPresent());

        // recreation of producer will load the topic again
        pulsarClient.newProducer().topic(topicName).create();
        topic = pulsar.getBrokerService().getTopicReference(topicName).get();
        assertNotNull(topic);
        // unload the topic
        unloadTopic(topicName, isPersistentTopic);
        // producer will retry and recreate the topic
        for (int i = 0; i < 5; i++) {
            if (!pulsar.getBrokerService().getTopicReference(topicName).isPresent() || i != 4) {
                Thread.sleep(200);
            }
        }
        // topic should be loaded by this time
        topic = pulsar.getBrokerService().getTopicReference(topicName).get();
        assertNotNull(topic);
    }

    private void unloadTopic(String topicName, boolean isPersistentTopic) throws Exception {
        admin.topics().unload(topicName);
    }

    /**
     * Verifies reset-cursor at specific position using admin-api.
     *
     * <pre>
     * 1. Publish 50 messages
     * 2. Consume 20 messages
     * 3. reset cursor position on 10th message
     * 4. consume 40 messages from reset position
     * </pre>
     *
     * @param namespaceName
     * @throws Exception
     */
    @Test(dataProvider = "namespaceNames", timeOut = 10000)
    public void testResetCursorOnPosition(String namespaceName) throws Exception {
        final String topicName = "persistent://prop-xyz/use/" + namespaceName + "/resetPosition";
        final int totalProducedMessages = 50;

        // set retention
        admin.namespaces().setRetention("prop-xyz/ns1", new RetentionPolicies(10, 10));

        // create consumer and subscription
        Consumer<byte[]> consumer = pulsarClient.newConsumer().topic(topicName).subscriptionName("my-sub")
                .subscriptionType(SubscriptionType.Shared).subscribe();

        assertEquals(admin.topics().getSubscriptions(topicName), Lists.newArrayList("my-sub"));

        publishMessagesOnPersistentTopic(topicName, totalProducedMessages, 0);

        List<Message<byte[]>> messages = admin.topics().peekMessages(topicName, "my-sub", 10);
        assertEquals(messages.size(), 10);

        Message<byte[]> message = null;
        MessageIdImpl resetMessageId = null;
        int resetPositionId = 10;
        for (int i = 0; i < 20; i++) {
            message = consumer.receive(1, TimeUnit.SECONDS);
            consumer.acknowledge(message);
            if (i == resetPositionId) {
                resetMessageId = (MessageIdImpl) message.getMessageId();
            }
        }

        // close consumer which will clean up internal-receive-queue
        consumer.close();

        // messages should still be available due to retention
        MessageIdImpl messageId = new MessageIdImpl(resetMessageId.getLedgerId(), resetMessageId.getEntryId(), -1);
        // reset position at resetMessageId
        admin.topics().resetCursor(topicName, "my-sub", messageId);

        consumer = pulsarClient.newConsumer().topic(topicName).subscriptionName("my-sub")
                .subscriptionType(SubscriptionType.Shared).subscribe();
        MessageIdImpl msgId2 = (MessageIdImpl) consumer.receive(1, TimeUnit.SECONDS).getMessageId();
        assertEquals(resetMessageId, msgId2);

        int receivedAfterReset = 1; // start with 1 because we have already received 1 msg

        for (int i = 0; i < totalProducedMessages; i++) {
            message = consumer.receive(500, TimeUnit.MILLISECONDS);
            if (message == null) {
                break;
            }
            consumer.acknowledge(message);
            ++receivedAfterReset;
        }
        assertEquals(receivedAfterReset, totalProducedMessages - resetPositionId);

        // invalid topic name
        try {
            admin.topics().resetCursor(topicName + "invalid", "my-sub", messageId);
            fail("It should have failed due to invalid topic name");
        } catch (PulsarAdminException.NotFoundException e) {
            // Ok
        }

        // invalid cursor name
        try {
            admin.topics().resetCursor(topicName, "invalid-sub", messageId);
            fail("It should have failed due to invalid subscription name");
        } catch (PulsarAdminException.NotFoundException e) {
            // Ok
        }

        // invalid position
        try {
            messageId = new MessageIdImpl(0, 0, -1);
            admin.topics().resetCursor(topicName, "my-sub", messageId);
        } catch (PulsarAdminException.PreconditionFailedException e) {
            fail("It shouldn't fail for a invalid position");
        }

        consumer.close();
    }

    private void publishMessagesOnPersistentTopic(String topicName, int messages, int startIdx) throws Exception {
        Producer<byte[]> producer = pulsarClient.newProducer()
            .topic(topicName)
            .enableBatching(false)
            .messageRoutingMode(MessageRoutingMode.SinglePartition)
            .create();

        for (int i = startIdx; i < (messages + startIdx); i++) {
            String message = "message-" + i;
            producer.send(message.getBytes());
        }

        producer.close();
    }

    /**
     * It verifies that pulsar with different load-manager generates different load-report and returned by admin-api
     *
     * @throws Exception
     */
    @Test
    public void testLoadReportApi() throws Exception {

        this.conf.setLoadManagerClassName(SimpleLoadManagerImpl.class.getName());
        MockedPulsarService mockPulsarSetup1 = new MockedPulsarService(this.conf);
        mockPulsarSetup1.setup();
        PulsarService simpleLoadManager = mockPulsarSetup1.getPulsar();
        PulsarAdmin simpleLoadManagerAdmin = mockPulsarSetup1.getAdmin();
        assertNotNull(simpleLoadManagerAdmin.brokerStats().getLoadReport());

        this.conf.setLoadManagerClassName(ModularLoadManagerImpl.class.getName());
        MockedPulsarService mockPulsarSetup2 = new MockedPulsarService(this.conf);
        mockPulsarSetup2.setup();
        PulsarAdmin modularLoadManagerAdmin = mockPulsarSetup2.getAdmin();
        assertNotNull(modularLoadManagerAdmin.brokerStats().getLoadReport());

        mockPulsarSetup1.cleanup();
        mockPulsarSetup2.cleanup();
    }

    @Test
    public void testPeerCluster() throws Exception {
        admin.clusters().createCluster("us-west1",
                new ClusterData("http://broker.messaging.west1.example.com:8080"));
        admin.clusters().createCluster("us-west2",
                new ClusterData("http://broker.messaging.west2.example.com:8080"));
        admin.clusters().createCluster("us-east1",
                new ClusterData("http://broker.messaging.east1.example.com:8080"));
        admin.clusters().createCluster("us-east2",
                new ClusterData("http://broker.messaging.east2.example.com:8080"));

        admin.clusters().updatePeerClusterNames("us-west1", Sets.newLinkedHashSet(Lists.newArrayList("us-west2")));
        assertEquals(admin.clusters().getCluster("us-west1").getPeerClusterNames(), Lists.newArrayList("us-west2"));
        assertNull(admin.clusters().getCluster("us-west2").getPeerClusterNames());
        // update cluster with duplicate peer-clusters in the list
        admin.clusters().updatePeerClusterNames("us-west1", Sets.newLinkedHashSet(
                Lists.newArrayList("us-west2", "us-east1", "us-west2", "us-east1", "us-west2", "us-east1")));
        assertEquals(admin.clusters().getCluster("us-west1").getPeerClusterNames(),
                Lists.newArrayList("us-west2", "us-east1"));
        admin.clusters().updatePeerClusterNames("us-west1", null);
        assertNull(admin.clusters().getCluster("us-west1").getPeerClusterNames());

        // Check name validation
        try {
            admin.clusters().updatePeerClusterNames("us-west1",
                    Sets.newLinkedHashSet(Lists.newArrayList("invalid-cluster")));
            fail("should have failed");
        } catch (PulsarAdminException e) {
            assertTrue(e instanceof PreconditionFailedException);
        }

        // Cluster itself can't be part of peer-list
        try {
            admin.clusters().updatePeerClusterNames("us-west1", Sets.newLinkedHashSet(Lists.newArrayList("us-west1")));
            fail("should have failed");
        } catch (PulsarAdminException e) {
            assertTrue(e instanceof PreconditionFailedException);
        }
    }

    /**
     * It validates that peer-cluster can't coexist in replication-cluster list
     *
     * @throws Exception
     */
    @Test
    public void testReplicationPeerCluster() throws Exception {
        admin.clusters().createCluster("us-west1",
                new ClusterData("http://broker.messaging.west1.example.com:8080"));
        admin.clusters().createCluster("us-west2",
                new ClusterData("http://broker.messaging.west2.example.com:8080"));
        admin.clusters().createCluster("us-west3",
                new ClusterData("http://broker.messaging.west2.example.com:8080"));
        admin.clusters().createCluster("us-west4",
                new ClusterData("http://broker.messaging.west2.example.com:8080"));
        admin.clusters().createCluster("us-east1",
                new ClusterData("http://broker.messaging.east1.example.com:8080"));
        admin.clusters().createCluster("us-east2",
                new ClusterData("http://broker.messaging.east2.example.com:8080"));
        admin.clusters().createCluster("global", new ClusterData());

        List<String> allClusters = admin.clusters().getClusters();
        Collections.sort(allClusters);
        assertEquals(allClusters,
                Lists.newArrayList("test", "us-east1", "us-east2", "us-west1", "us-west2", "us-west3", "us-west4"));

        final String property = "peer-prop";
        Set<String> allowedClusters = Sets.newHashSet("us-west1", "us-west2", "us-west3", "us-west4", "us-east1",
                "us-east2", "global");
        TenantInfo propConfig = new TenantInfo(Sets.newHashSet("test"), allowedClusters);
        admin.tenants().createTenant(property, propConfig);

        final String namespace = property + "/global/conflictPeer";
        admin.namespaces().createNamespace(namespace);

        admin.clusters().updatePeerClusterNames("us-west1",
                Sets.newLinkedHashSet(Lists.newArrayList("us-west2", "us-west3")));
        assertEquals(admin.clusters().getCluster("us-west1").getPeerClusterNames(),
                Lists.newArrayList("us-west2", "us-west3"));

        // (1) no conflicting peer
        Set<String> clusterIds = Sets.newHashSet("us-east1", "us-east2");
        admin.namespaces().setNamespaceReplicationClusters(namespace, clusterIds);

        // (2) conflicting peer
        clusterIds = Sets.newHashSet("us-west2", "us-west3", "us-west1");
        try {
            admin.namespaces().setNamespaceReplicationClusters(namespace, clusterIds);
            fail("Peer-cluster can't coexist in replication cluster list");
        } catch (PulsarAdminException.ConflictException e) {
            // Ok
        }

        clusterIds = Sets.newHashSet("us-west2", "us-west3");
        // no peer coexist in replication clusters
        admin.namespaces().setNamespaceReplicationClusters(namespace, clusterIds);

        clusterIds = Sets.newHashSet("us-west1", "us-west4");
        // no peer coexist in replication clusters
        admin.namespaces().setNamespaceReplicationClusters(namespace, clusterIds);
    }

    @Test
    public void clusterFailureDomain() throws PulsarAdminException {

        final String cluster = pulsar.getConfiguration().getClusterName();
        // create
        FailureDomain domain = new FailureDomain();
        domain.setBrokers(Sets.newHashSet("b1", "b2", "b3"));
        admin.clusters().createFailureDomain(cluster, "domain-1", domain);
        admin.clusters().updateFailureDomain(cluster, "domain-1", domain);

        assertEquals(admin.clusters().getFailureDomain(cluster, "domain-1"), domain);

        Map<String, FailureDomain> domains = admin.clusters().getFailureDomains(cluster);
        assertEquals(domains.size(), 1);
        assertTrue(domains.containsKey("domain-1"));

        try {
            // try to create domain with already registered brokers
            admin.clusters().createFailureDomain(cluster, "domain-2", domain);
            fail("should have failed because of brokers are already registered");
        } catch (PulsarAdminException.ConflictException e) {
            // Ok
        }

        admin.clusters().deleteFailureDomain(cluster, "domain-1");
        assertTrue(admin.clusters().getFailureDomains(cluster).isEmpty());

        admin.clusters().createFailureDomain(cluster, "domain-2", domain);
        domains = admin.clusters().getFailureDomains(cluster);
        assertEquals(domains.size(), 1);
        assertTrue(domains.containsKey("domain-2"));
    }

    @Test
    public void namespaceAntiAffinity() throws PulsarAdminException {
        final String namespace = "prop-xyz/ns1";
        final String antiAffinityGroup = "group";
        assertTrue(isBlank(admin.namespaces().getNamespaceAntiAffinityGroup(namespace)));
        admin.namespaces().setNamespaceAntiAffinityGroup(namespace, antiAffinityGroup);
        assertEquals(admin.namespaces().getNamespaceAntiAffinityGroup(namespace), antiAffinityGroup);
        admin.namespaces().deleteNamespaceAntiAffinityGroup(namespace);
        assertTrue(isBlank(admin.namespaces().getNamespaceAntiAffinityGroup(namespace)));

        final String ns1 = "prop-xyz/antiAG1";
        final String ns2 = "prop-xyz/antiAG2";
        final String ns3 = "prop-xyz/antiAG3";
        admin.namespaces().createNamespace(ns1, Sets.newHashSet("test"));
        admin.namespaces().createNamespace(ns2, Sets.newHashSet("test"));
        admin.namespaces().createNamespace(ns3, Sets.newHashSet("test"));
        admin.namespaces().setNamespaceAntiAffinityGroup(ns1, antiAffinityGroup);
        admin.namespaces().setNamespaceAntiAffinityGroup(ns2, antiAffinityGroup);
        admin.namespaces().setNamespaceAntiAffinityGroup(ns3, antiAffinityGroup);

        Set<String> namespaces = new HashSet<>(
                admin.namespaces().getAntiAffinityNamespaces("prop-xyz", "test", antiAffinityGroup));
        assertEquals(namespaces.size(), 3);
        assertTrue(namespaces.contains(ns1));
        assertTrue(namespaces.contains(ns2));
        assertTrue(namespaces.contains(ns3));

        List<String> namespaces2 = admin.namespaces().getAntiAffinityNamespaces("prop-xyz", "test", "invalid-group");
        assertEquals(namespaces2.size(), 0);
    }

    @Test
    public void testNonPersistentTopics() throws Exception {
        final String namespace = "prop-xyz/ns2";
        final String topicName = "non-persistent://" + namespace + "/topic";
        admin.namespaces().createNamespace(namespace, 20);
        admin.namespaces().setNamespaceReplicationClusters(namespace, Sets.newHashSet("test"));
        int totalTopics = 100;

        Set<String> topicNames = Sets.newHashSet();
        for (int i = 0; i < totalTopics; i++) {
            topicNames.add(topicName + i);
            Producer<byte[]> producer = pulsarClient.newProducer().topic(topicName + i).create();
            producer.close();
        }

        for (int i = 0; i < totalTopics; i++) {
            Topic topic = pulsar.getBrokerService().getTopicReference(topicName + i).get();
            assertNotNull(topic);
        }

        Set<String> topicsInNs = Sets.newHashSet(admin.topics().getList(namespace));
        assertEquals(topicsInNs.size(), totalTopics);
        topicsInNs.removeAll(topicNames);
        assertEquals(topicsInNs.size(), 0);
    }

    @Test
    public void testPublishConsumerStats() throws Exception {
        final String topicName = "statTopic";
        final String subscriberName = topicName + "-my-sub-1";
        final String topic = "persistent://prop-xyz/ns1/" + topicName;
        final String producerName = "myProducer";

        @Cleanup
        PulsarClient client = PulsarClient.builder().serviceUrl(pulsar.getWebServiceAddress()).build();
        Consumer<byte[]> consumer = client.newConsumer().topic(topic).subscriptionName(subscriberName)
                .subscriptionType(SubscriptionType.Shared).subscribe();
        Producer<byte[]> producer = client.newProducer()
            .topic(topic)
            .enableBatching(false)
            .messageRoutingMode(MessageRoutingMode.SinglePartition)
            .producerName(producerName)
            .create();

        retryStrategically((test) -> {
            TopicStats stats;
            try {
                stats = admin.topics().getStats(topic);
                return stats.publishers.size() > 0 && stats.subscriptions.get(subscriberName) != null
                        && stats.subscriptions.get(subscriberName).consumers.size() > 0;
            } catch (PulsarAdminException e) {
                return false;
            }
        }, 5, 200);

        TopicStats topicStats = admin.topics().getStats(topic);
        assertEquals(topicStats.publishers.size(), 1);
        assertNotNull(topicStats.publishers.get(0).getAddress());
        assertNotNull(topicStats.publishers.get(0).getClientVersion());
        assertNotNull(topicStats.publishers.get(0).getConnectedSince());
        assertNotNull(topicStats.publishers.get(0).getProducerName());
        assertEquals(topicStats.publishers.get(0).getProducerName(), producerName);

        SubscriptionStats subscriber = topicStats.subscriptions.get(subscriberName);
        assertNotNull(subscriber);
        assertEquals(subscriber.consumers.size(), 1);
        ConsumerStats consumerStats = subscriber.consumers.get(0);
        assertNotNull(consumerStats.getAddress());
        assertNotNull(consumerStats.getClientVersion());
        assertNotNull(consumerStats.getConnectedSince());

        producer.close();
        consumer.close();
    }

    @Test
    public void testTenantNameWithUnderscore() throws Exception {
        TenantInfo tenantInfo = new TenantInfo(Sets.newHashSet("role1", "role2"), Sets.newHashSet("test"));
        admin.tenants().createTenant("prop_xyz", tenantInfo);

        admin.namespaces().createNamespace("prop_xyz/my-namespace", Sets.newHashSet("test"));

        String topic = "persistent://prop_xyz/use/my-namespace/my-topic";

        Producer<byte[]> producer = pulsarClient.newProducer().topic(topic)
                .create();

        TopicStats stats = admin.topics().getStats(topic);
        assertEquals(stats.publishers.size(), 1);
        producer.close();
    }

    @Test
    public void testTenantNameWithInvalidCharacters() throws Exception {
        TenantInfo tenantInfo = new TenantInfo(Sets.newHashSet("role1", "role2"), Sets.newHashSet("test"));

        // If we try to create property with invalid characters, it should fail immediately
        try {
            admin.tenants().createTenant("prop xyz", tenantInfo);
            fail("Should have failed");
        } catch (PulsarAdminException e) {
            assertEquals(e.getStatusCode(), Status.PRECONDITION_FAILED.getStatusCode());
        }

        try {
            admin.tenants().createTenant("prop&xyz", tenantInfo);
            fail("Should have failed");
        } catch (PulsarAdminException e) {
            assertEquals(e.getStatusCode(), Status.PRECONDITION_FAILED.getStatusCode());
        }
    }

    @Test
    public void testTenantWithNonexistentClusters() throws Exception {
        // Check non-existing cluster
        assertFalse(admin.clusters().getClusters().contains("cluster-non-existing"));

        Set<String> allowedClusters = Sets.newHashSet("cluster-non-existing");
        TenantInfo tenantInfo = new TenantInfo(Sets.newHashSet("role1", "role2"), allowedClusters);

        // If we try to create tenant with nonexistent clusters, it should fail immediately
        try {
            admin.tenants().createTenant("test-tenant", tenantInfo);
            fail("Should have failed");
        } catch (PulsarAdminException e) {
            assertEquals(e.getStatusCode(), Status.PRECONDITION_FAILED.getStatusCode());
        }

        assertFalse(admin.tenants().getTenants().contains("test-tenant"));

        // Check existing tenant
        assertTrue(admin.tenants().getTenants().contains("prop-xyz"));

        // If we try to update existing tenant with nonexistent clusters, it should fail immediately
        try {
            admin.tenants().updateTenant("prop-xyz", tenantInfo);
            fail("Should have failed");
        } catch (PulsarAdminException e) {
            assertEquals(e.getStatusCode(), Status.PRECONDITION_FAILED.getStatusCode());
        }
    }

    @Test
    public void brokerNamespaceIsolationPolicies() throws Exception {

        // create
        String policyName1 = "policy-1";
        String cluster = pulsar.getConfiguration().getClusterName();
        String namespaceRegex = "other/" + cluster + "/other.*";
        String brokerName = pulsar.getAdvertisedAddress();
        String brokerAddress = brokerName + ":" + pulsar.getConfiguration().getWebServicePort().get();
        NamespaceIsolationData nsPolicyData1 = new NamespaceIsolationData();
        nsPolicyData1.namespaces = new ArrayList<String>();
        nsPolicyData1.namespaces.add(namespaceRegex);
        nsPolicyData1.primary = new ArrayList<String>();
        nsPolicyData1.primary.add(brokerName + ":[0-9]*");
        nsPolicyData1.secondary = new ArrayList<String>();
        nsPolicyData1.secondary.add(brokerName + ".*");
        nsPolicyData1.auto_failover_policy = new AutoFailoverPolicyData();
        nsPolicyData1.auto_failover_policy.policy_type = AutoFailoverPolicyType.min_available;
        nsPolicyData1.auto_failover_policy.parameters = new HashMap<String, String>();
        nsPolicyData1.auto_failover_policy.parameters.put("min_limit", "1");
        nsPolicyData1.auto_failover_policy.parameters.put("usage_threshold", "100");
        admin.clusters().createNamespaceIsolationPolicy(cluster, policyName1, nsPolicyData1);

        List<BrokerNamespaceIsolationData> brokerIsolationDataList = admin.clusters()
                .getBrokersWithNamespaceIsolationPolicy(cluster);
        assertEquals(brokerIsolationDataList.size(), 1);
        assertEquals(brokerIsolationDataList.get(0).brokerName, brokerAddress);
        assertEquals(brokerIsolationDataList.get(0).namespaceRegex.size(), 1);
        assertEquals(brokerIsolationDataList.get(0).namespaceRegex.get(0), namespaceRegex);

        BrokerNamespaceIsolationData brokerIsolationData = admin.clusters()
                .getBrokerWithNamespaceIsolationPolicy(cluster, brokerAddress);
        assertEquals(brokerIsolationData.brokerName, brokerAddress);
        assertEquals(brokerIsolationData.namespaceRegex.size(), 1);
        assertEquals(brokerIsolationData.namespaceRegex.get(0), namespaceRegex);

        BrokerNamespaceIsolationData isolationData = admin.clusters().getBrokerWithNamespaceIsolationPolicy(cluster, "invalid-broker");
        assertFalse(isolationData.isPrimary);
    }

    @Test
    public void clustersList() throws PulsarAdminException {
        final String cluster = pulsar.getConfiguration().getClusterName();
        admin.clusters().createCluster("global", new ClusterData("http://localhost:6650"));

        // Global cluster, if there, should be omitted from the results
        assertEquals(admin.clusters().getClusters(), Lists.newArrayList(cluster));
    }
    /**
     * verifies cluster has been set before create topic
     *
     * @throws Exception
     */
    @Test
    public void testClusterIsReadyBeforeCreateTopic() throws PulsarAdminException {
        final String topicName = "partitionedTopic";
        final int partitions = 4;
        final String persistentPartitionedTopicName = "persistent://prop-xyz/ns2/" + topicName;
        final String NonPersistentPartitionedTopicName = "non-persistent://prop-xyz/ns2/" + topicName;

        admin.namespaces().createNamespace("prop-xyz/ns2");
        // By default the cluster will configure as configuration file. So the create topic operation
        // will never throw exception except there is no cluster.
        admin.namespaces().setNamespaceReplicationClusters("prop-xyz/ns2", new HashSet<String>());

        try {
            admin.topics().createPartitionedTopic(persistentPartitionedTopicName, partitions);
            Assert.fail("should have failed due to Namespace does not have any clusters configured");
        } catch (PulsarAdminException.PreconditionFailedException e) {
        }
        try {
            admin.topics().createPartitionedTopic(NonPersistentPartitionedTopicName, partitions);
            Assert.fail("should have failed due to Namespace does not have any clusters configured");
        } catch (PulsarAdminException.PreconditionFailedException e) {
        }
    }

    @Test
    public void testCreateNamespaceWithNoClusters() throws PulsarAdminException {
        String localCluster = pulsar.getConfiguration().getClusterName();
        String namespace = "prop-xyz/test-ns-with-no-clusters";
        admin.namespaces().createNamespace(namespace);

        // Global cluster, if there, should be omitted from the results
        assertEquals(admin.namespaces().getNamespaceReplicationClusters(namespace),
                Collections.singletonList(localCluster));
    }

    @Test(timeOut = 30000)
    public void testConsumerStatsLastTimestamp() throws PulsarClientException, PulsarAdminException, InterruptedException {
        long timestamp = System.currentTimeMillis();
        final String topicName = "consumer-stats-" + timestamp;
        final String subscribeName = topicName + "-test-stats-sub";
        final String topic = "persistent://prop-xyz/ns1/" + topicName;
        final String producerName = "producer-" + topicName;

        @Cleanup
        PulsarClient client = PulsarClient.builder().serviceUrl(pulsar.getWebServiceAddress()).build();
        Producer<byte[]> producer = client.newProducer().topic(topic)
            .enableBatching(false)
            .producerName(producerName)
            .create();

        // a. Send a message to the topic.
        producer.send("message-1".getBytes(StandardCharsets.UTF_8));

        // b. Create a consumer, because there was a message in the topic, the consumer will receive the message pushed
        // by the broker, the lastConsumedTimestamp will as the consume subscribe time.
        Consumer<byte[]> consumer = client.newConsumer().topic(topic)
            .subscriptionName(subscribeName)
            .subscriptionInitialPosition(SubscriptionInitialPosition.Earliest)
            .acknowledgmentGroupTime(0, TimeUnit.SECONDS)
            .subscribe();
        Message<byte[]> message = consumer.receive();

        // Get the consumer stats.
        TopicStats topicStats = admin.topics().getStats(topic);
        SubscriptionStats subscriptionStats = topicStats.subscriptions.get(subscribeName);
        long startConsumedFlowTimestamp = subscriptionStats.lastConsumedFlowTimestamp;
        long startAckedTimestampInSubStats = subscriptionStats.lastAckedTimestamp;
        ConsumerStats consumerStats = subscriptionStats.consumers.get(0);
        long startConsumedTimestampInConsumerStats = consumerStats.lastConsumedTimestamp;
        long startAckedTimestampInConsumerStats = consumerStats.lastAckedTimestamp;

        // Because the message was pushed by the broker, the consumedTimestamp should not as 0.
        assertNotEquals(0, startConsumedTimestampInConsumerStats);
        // There is no consumer ack the message, so the lastAckedTimestamp still as 0.
        assertEquals(0, startAckedTimestampInConsumerStats);
        assertNotEquals(0, startConsumedFlowTimestamp);
        assertEquals(0, startAckedTimestampInSubStats);

        // c. The Consumer receives the message and acks the message.
        consumer.acknowledge(message);
        // Waiting for the ack command send to the broker.
        while (true) {
            topicStats = admin.topics().getStats(topic);
            if (topicStats.subscriptions.get(subscribeName).lastAckedTimestamp != 0) {
                break;
            }
            TimeUnit.MILLISECONDS.sleep(100);
        }

        // Get the consumer stats.
        topicStats = admin.topics().getStats(topic);
        subscriptionStats = topicStats.subscriptions.get(subscribeName);
        long consumedFlowTimestamp = subscriptionStats.lastConsumedFlowTimestamp;
        long ackedTimestampInSubStats = subscriptionStats.lastAckedTimestamp;
        consumerStats = subscriptionStats.consumers.get(0);
        long consumedTimestamp = consumerStats.lastConsumedTimestamp;
        long ackedTimestamp = consumerStats.lastAckedTimestamp;

        // The lastConsumedTimestamp should same as the last time because the broker does not push any messages and the
        // consumer does not pull any messages.
        assertEquals(startConsumedTimestampInConsumerStats, consumedTimestamp);
        assertTrue(startAckedTimestampInConsumerStats < ackedTimestamp);
        assertNotEquals(0, consumedFlowTimestamp);
        assertTrue(startAckedTimestampInSubStats < ackedTimestampInSubStats);

        // d. Send another messages. The lastConsumedTimestamp should be updated.
        producer.send("message-2".getBytes(StandardCharsets.UTF_8));

        // e. Receive the message and ack it.
        message = consumer.receive();
        consumer.acknowledge(message);
        // Waiting for the ack command send to the broker.
        while (true) {
            topicStats = admin.topics().getStats(topic);
            if (topicStats.subscriptions.get(subscribeName).lastAckedTimestamp != ackedTimestampInSubStats) {
                break;
            }
            TimeUnit.MILLISECONDS.sleep(100);
        }

        // Get the consumer stats again.
        topicStats = admin.topics().getStats(topic);
        subscriptionStats = topicStats.subscriptions.get(subscribeName);
        long lastConsumedFlowTimestamp = subscriptionStats.lastConsumedFlowTimestamp;
        long lastConsumedTimestampInSubStats = subscriptionStats.lastConsumedTimestamp;
        long lastAckedTimestampInSubStats = subscriptionStats.lastAckedTimestamp;
        consumerStats = subscriptionStats.consumers.get(0);
        long lastConsumedTimestamp = consumerStats.lastConsumedTimestamp;
        long lastAckedTimestamp = consumerStats.lastAckedTimestamp;

        assertTrue(consumedTimestamp < lastConsumedTimestamp);
        assertTrue(ackedTimestamp < lastAckedTimestamp);
        assertTrue(startConsumedTimestampInConsumerStats < lastConsumedTimestamp);
        assertTrue(startAckedTimestampInConsumerStats < lastAckedTimestamp);
        assertTrue(consumedFlowTimestamp == lastConsumedFlowTimestamp);
        assertTrue(ackedTimestampInSubStats < lastAckedTimestampInSubStats);
        assertEquals(lastConsumedTimestamp, lastConsumedTimestampInSubStats);

        consumer.close();
        producer.close();
    }

    @Test(timeOut = 30000)
    public void testPreciseBacklog() throws PulsarClientException, PulsarAdminException, InterruptedException {
        final String topic = "persistent://prop-xyz/ns1/precise-back-log";
        final String subName = "sub-name";

        @Cleanup
        PulsarClient client = PulsarClient.builder().serviceUrl(pulsar.getWebServiceAddress()).build();

        @Cleanup
        Consumer<byte[]> consumer = client.newConsumer()
            .topic(topic)
            .subscriptionName(subName)
            .acknowledgmentGroupTime(0, TimeUnit.SECONDS)
            .subscribe();

        @Cleanup
        Producer<byte[]> producer = client.newProducer()
            .topic(topic)
            .enableBatching(false)
            .create();

        producer.send("message-1".getBytes(StandardCharsets.UTF_8));
        Message<byte[]> message = consumer.receive();
        assertNotNull(message);

        // Mock the entries added count. Default is disable the precise backlog, so the backlog is entries added count - consumed count
        // Since message have not acked, so the backlog is 10
        PersistentSubscription subscription = (PersistentSubscription)pulsar.getBrokerService().getTopicReference(topic).get().getSubscription(subName);
        assertNotNull(subscription);
        ((ManagedLedgerImpl)subscription.getCursor().getManagedLedger()).setEntriesAddedCounter(10L);
        TopicStats topicStats = admin.topics().getStats(topic);
        assertEquals(topicStats.subscriptions.get(subName).msgBacklog, 10);

        topicStats = admin.topics().getStats(topic, true);
        assertEquals(topicStats.subscriptions.get(subName).msgBacklog, 1);
        consumer.acknowledge(message);

        // wait for ack send
        Thread.sleep(500);

        // Consumer acks the message, so the precise backlog is 0
        topicStats = admin.topics().getStats(topic, true);
        assertEquals(topicStats.subscriptions.get(subName).msgBacklog, 0);

        topicStats = admin.topics().getStats(topic);
        assertEquals(topicStats.subscriptions.get(subName).msgBacklog, 9);
    }

    @Test(timeOut = 30000)
    public void testBacklogNoDelayed() throws PulsarClientException, PulsarAdminException, InterruptedException {
        final String topic = "persistent://prop-xyz/ns1/precise-back-log-no-delayed-" + UUID.randomUUID().toString();
        final String subName = "sub-name";

        @Cleanup
        PulsarClient client = PulsarClient.builder().serviceUrl(pulsar.getWebServiceAddress()).build();

        @Cleanup
        Consumer<byte[]> consumer = client.newConsumer()
            .topic(topic)
            .subscriptionName(subName)
            .subscriptionType(SubscriptionType.Shared)
            .acknowledgmentGroupTime(0, TimeUnit.SECONDS)
            .subscribe();

        @Cleanup
        Producer<byte[]> producer = client.newProducer()
            .topic(topic)
            .enableBatching(false)
            .create();

        for (int i = 0; i < 10; i++) {
            if (i > 4) {
                producer.newMessage()
                    .value("message-1".getBytes(StandardCharsets.UTF_8))
                    .deliverAfter(10, TimeUnit.SECONDS)
                    .send();
            } else {
                producer.send("message-1".getBytes(StandardCharsets.UTF_8));
            }
        }

        // Wait for messages to be tracked for delayed delivery. This happens
        // on the consumer dispatch side, so when the send() is complete we're
        // not yet guaranteed to see the stats updated.
        Thread.sleep(500);

        TopicStats topicStats = admin.topics().getStats(topic, true);
        assertEquals(topicStats.subscriptions.get(subName).msgBacklog, 10);
        assertEquals(topicStats.subscriptions.get(subName).msgBacklogNoDelayed, 5);

        for (int i = 0; i < 5; i++) {
            consumer.acknowledge(consumer.receive());
        }
        // Wait the ack send.
        Thread.sleep(500);
        topicStats = admin.topics().getStats(topic, true);
        assertEquals(topicStats.subscriptions.get(subName).msgBacklog, 5);
        assertEquals(topicStats.subscriptions.get(subName).msgBacklogNoDelayed, 0);
    }

    @Test
    public void testPreciseBacklogForPartitionedTopic() throws PulsarClientException, PulsarAdminException, InterruptedException {
        final String topic = "persistent://prop-xyz/ns1/precise-back-log-for-partitioned-topic";
        admin.topics().createPartitionedTopic(topic, 2);
        final String subName = "sub-name";

        @Cleanup
        PulsarClient client = PulsarClient.builder().serviceUrl(pulsar.getWebServiceAddress()).build();

        @Cleanup
        Consumer<byte[]> consumer = client.newConsumer()
            .topic(topic)
            .subscriptionName(subName)
            .acknowledgmentGroupTime(0, TimeUnit.SECONDS)
            .subscribe();

        @Cleanup
        Producer<byte[]> producer = client.newProducer()
            .topic(topic)
            .enableBatching(false)
            .create();

        producer.send("message-1".getBytes(StandardCharsets.UTF_8));
        Message<byte[]> message = consumer.receive();
        assertNotNull(message);

        // Mock the entries added count. Default is disable the precise backlog, so the backlog is entries added count - consumed count
        // Since message have not acked, so the backlog is 10
        for (int i = 0; i < 2; i++) {
            PersistentSubscription subscription = (PersistentSubscription)pulsar.getBrokerService().getTopicReference(topic + "-partition-" + i).get().getSubscription(subName);
            assertNotNull(subscription);
            ((ManagedLedgerImpl)subscription.getCursor().getManagedLedger()).setEntriesAddedCounter(10L);
        }

        TopicStats topicStats = admin.topics().getPartitionedStats(topic, false);
        assertEquals(topicStats.subscriptions.get(subName).msgBacklog, 20);

        topicStats = admin.topics().getPartitionedStats(topic, false, true);
        assertEquals(topicStats.subscriptions.get(subName).msgBacklog, 1);
    }

    @Test(timeOut = 30000)
    public void testBacklogNoDelayedForPartitionedTopic() throws PulsarClientException, PulsarAdminException, InterruptedException {
        final String topic = "persistent://prop-xyz/ns1/precise-back-log-no-delayed-partitioned-topic";
        admin.topics().createPartitionedTopic(topic, 2);
        final String subName = "sub-name";

        @Cleanup
        PulsarClient client = PulsarClient.builder().serviceUrl(pulsar.getWebServiceAddress()).build();

        @Cleanup
        Consumer<byte[]> consumer = client.newConsumer()
            .topic(topic)
            .subscriptionName(subName)
            .subscriptionType(SubscriptionType.Shared)
            .acknowledgmentGroupTime(0, TimeUnit.SECONDS)
            .subscribe();

        @Cleanup
        Producer<byte[]> producer = client.newProducer()
            .topic(topic)
            .enableBatching(false)
            .create();

        for (int i = 0; i < 10; i++) {
            if (i > 4) {
                producer.newMessage()
                    .value("message-1".getBytes(StandardCharsets.UTF_8))
                    .deliverAfter(10, TimeUnit.SECONDS)
                    .send();
            } else {
                producer.send("message-1".getBytes(StandardCharsets.UTF_8));
            }
        }

        TopicStats topicStats = admin.topics().getPartitionedStats(topic, false, true);
        assertEquals(topicStats.subscriptions.get(subName).msgBacklog, 10);
        assertEquals(topicStats.subscriptions.get(subName).msgBacklogNoDelayed, 5);

        for (int i = 0; i < 5; i++) {
            consumer.acknowledge(consumer.receive());
        }
        // Wait the ack send.
        Thread.sleep(500);
        topicStats = admin.topics().getPartitionedStats(topic, false, true);
        assertEquals(topicStats.subscriptions.get(subName).msgBacklog, 5);
        assertEquals(topicStats.subscriptions.get(subName).msgBacklogNoDelayed, 0);
    }

    @Test
    public void testMaxNumPartitionsPerPartitionedTopicSuccess() {
        final String topic = "persistent://prop-xyz/ns1/max-num-partitions-per-partitioned-topic-success";
        pulsar.getConfiguration().setMaxNumPartitionsPerPartitionedTopic(3);

        try {
            admin.topics().createPartitionedTopic(topic, 2);
        } catch (Exception e) {
            fail("should not throw any exceptions");
        }
    }

    @Test
    public void testMaxNumPartitionsPerPartitionedTopicFailure() {
        final String topic = "persistent://prop-xyz/ns1/max-num-partitions-per-partitioned-topic-failure";
        pulsar.getConfiguration().setMaxNumPartitionsPerPartitionedTopic(2);

        try {
            admin.topics().createPartitionedTopic(topic, 3);
            fail("should throw exception when number of partitions exceed than max partitions");
        } catch (Exception e) {
            assertTrue(e instanceof PulsarAdminException);
        }
    }

    @Test
    public void testListOfNamespaceBundles() throws Exception {
        TenantInfo tenantInfo = new TenantInfo(Sets.newHashSet("role1", "role2"), Sets.newHashSet("test"));
        admin.tenants().createTenant("prop-xyz2", tenantInfo);
        admin.namespaces().createNamespace("prop-xyz2/ns1", 10);
        admin.namespaces().setNamespaceReplicationClusters("prop-xyz2/ns1", Sets.newHashSet("test"));
        admin.namespaces().createNamespace("prop-xyz2/test/ns2", 10);
        assertEquals(admin.namespaces().getBundles("prop-xyz2/ns1").numBundles, 10);
        assertEquals(admin.namespaces().getBundles("prop-xyz2/test/ns2").numBundles, 10);
    }

    @Test
    public void testUpdateClusterWithProxyUrl() throws Exception {
        ClusterData cluster = new ClusterData(pulsar.getWebServiceAddress());
        String clusterName = "test2";
        admin.clusters().createCluster(clusterName, cluster);
        Assert.assertEquals(admin.clusters().getCluster(clusterName), cluster);

        // update
        cluster.setProxyServiceUrl("proxy");
        cluster.setProxyProtocol(ProxyProtocol.SNI);
        admin.clusters().updateCluster(clusterName, cluster);
        Assert.assertEquals(admin.clusters().getCluster(clusterName), cluster);
    }

    @Test
    public void testMaxNamespacesPerTenant() throws Exception {
        super.internalCleanup();
        conf.setMaxNamespacesPerTenant(2);
        super.internalSetup();
        admin.clusters().createCluster("test", new ClusterData(brokerUrl.toString()));
        TenantInfo tenantInfo = new TenantInfo(Sets.newHashSet("role1", "role2"), Sets.newHashSet("test"));
        admin.tenants().createTenant("testTenant", tenantInfo);
        admin.namespaces().createNamespace("testTenant/ns1", Sets.newHashSet("test"));
        admin.namespaces().createNamespace("testTenant/ns2", Sets.newHashSet("test"));
        try {
            admin.namespaces().createNamespace("testTenant/ns3", Sets.newHashSet("test"));
        } catch (PulsarAdminException e) {
            Assert.assertEquals(e.getStatusCode(), 412);
            Assert.assertEquals(e.getHttpError(), "Exceed the maximum number of namespace in tenant :testTenant");
        }

        //unlimited
        super.internalCleanup();
        conf.setMaxNamespacesPerTenant(0);
        super.internalSetup();
        admin.clusters().createCluster("test", new ClusterData(brokerUrl.toString()));
        admin.tenants().createTenant("testTenant", tenantInfo);
        for (int i = 0; i < 10; i++) {
            admin.namespaces().createNamespace("testTenant/ns-" + i, Sets.newHashSet("test"));
        }

    }

    @Test
    public void testInvalidBundleErrorResponse() throws Exception {
        try {
            admin.namespaces().deleteNamespaceBundle("prop-xyz/ns1", "invalid-bundle");
            fail("should have failed due to invalid bundle");
        } catch (PreconditionFailedException e) {
            assertTrue(e.getMessage().startsWith("Invalid bundle range"));
        }
    }

    @Test
    public void testMaxSubscriptionsPerTopic() throws Exception {
        super.internalCleanup();
        conf.setMaxSubscriptionsPerTopic(2);
        super.internalSetup();

        admin.clusters().createCluster("test", new ClusterData(brokerUrl.toString()));
        TenantInfo tenantInfo = new TenantInfo(Sets.newHashSet("role1", "role2"), Sets.newHashSet("test"));
        admin.tenants().createTenant("testTenant", tenantInfo);
        admin.namespaces().createNamespace("testTenant/ns1", Sets.newHashSet("test"));

        final String topic = "persistent://testTenant/ns1/max-subscriptions-per-topic";

        admin.topics().createPartitionedTopic(topic, 3);
        Producer producer = pulsarClient.newProducer().topic(topic).create();
        producer.close();

        // create subscription
        admin.topics().createSubscription(topic, "test-sub1", MessageId.EARLIEST);
        admin.topics().createSubscription(topic, "test-sub2", MessageId.EARLIEST);
        try {
            admin.topics().createSubscription(topic, "test-sub3", MessageId.EARLIEST);
            Assert.fail();
        } catch (PulsarAdminException e) {
            log.info("create subscription failed. Exception: ", e);
        }

        super.internalCleanup();
        conf.setMaxSubscriptionsPerTopic(0);
        super.internalSetup();

        admin.clusters().createCluster("test", new ClusterData(brokerUrl.toString()));
        admin.tenants().createTenant("testTenant", tenantInfo);
        admin.namespaces().createNamespace("testTenant/ns1", Sets.newHashSet("test"));

        admin.topics().createPartitionedTopic(topic, 3);
        producer = pulsarClient.newProducer().topic(topic).create();
        producer.close();

        for (int i = 0; i < 10; ++i) {
            admin.topics().createSubscription(topic, "test-sub" + i, MessageId.EARLIEST);
        }

        super.internalCleanup();
        conf.setMaxSubscriptionsPerTopic(2);
        super.internalSetup();

        admin.clusters().createCluster("test", new ClusterData(brokerUrl.toString()));
        admin.tenants().createTenant("testTenant", tenantInfo);
        admin.namespaces().createNamespace("testTenant/ns1", Sets.newHashSet("test"));

        admin.topics().createPartitionedTopic(topic, 3);
        producer = pulsarClient.newProducer().topic(topic).create();
        producer.close();

        Consumer consumer1 = null;
        Consumer consumer2 = null;
        Consumer consumer3 = null;

        try {
            consumer1 = pulsarClient.newConsumer().subscriptionName("test-sub1").topic(topic).subscribe();
            Assert.assertNotNull(consumer1);
        } catch (PulsarClientException e) {
            Assert.fail();
        }

        try {
            consumer2 = pulsarClient.newConsumer().subscriptionName("test-sub2").topic(topic).subscribe();
            Assert.assertNotNull(consumer2);
        } catch (PulsarClientException e) {
            Assert.fail();
        }

        try {
            consumer3 = pulsarClient.newConsumer().subscriptionName("test-sub3").topic(topic).subscribe();
            Assert.fail();
        } catch (PulsarClientException e) {
            log.info("subscription reached max subscriptions per topic");
        }

        consumer1.close();
        consumer2.close();
        admin.topics().deletePartitionedTopic(topic);
    }
}
