package dev.solace.autoscale;

import com.solacesystems.jms.SolConnectionFactory;
import com.solacesystems.jms.SolJmsUtility;
import com.solacesystems.jms.SolXAConnectionFactory;

import javax.jms.Connection;
import javax.jms.DeliveryMode;
import javax.jms.Destination;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageProducer;
import javax.jms.Queue;
import javax.jms.Session;
import javax.jms.TextMessage;
import javax.jms.XAConnection;
import javax.jms.XASession;
import javax.transaction.xa.XAResource;
import javax.transaction.xa.Xid;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

public final class NativeFeatureQualification {
    private static final int RECEIVE_TIMEOUT_MS = 3000;
    private static final int ABSENCE_TIMEOUT_MS = 700;

    private NativeFeatureQualification() {}

    public static void main(String[] args) throws Exception {
        if (args.length == 0) {
            throw new IllegalArgumentException("mode required");
        }
        switch (args[0]) {
            case "local":
                runLocalTransactions();
                break;
            case "open-tx-crash":
                publishOpenTransactionAndCrash(args[1]);
                break;
            case "assert-empty":
                assertQueueEmpty(args[1]);
                break;
            case "xa-basic":
                runXaBasic();
                break;
            case "xa-prepare":
                prepareXaForRecovery(args[1]);
                break;
            case "xa-recover":
                recoverAndCommitXa(args[1]);
                break;
            case "publish-topic":
                publishTopic(args[1], args[2], Integer.parseInt(args[3]));
                break;
            case "trace-workload":
                traceWorkload(args[1], args[2], Integer.parseInt(args[3]));
                break;
            case "queue-publish":
                publishQueue(args[1], args[2], Integer.parseInt(args[3]));
                break;
            case "queue-consume":
                consumeQueue(args[1], args[2], Integer.parseInt(args[3]));
                break;
            case "replay-assert":
                assertReplay(args[1], args[2], Integer.parseInt(args[3]), args[4], Integer.parseInt(args[5]));
                break;
            default:
                throw new IllegalArgumentException("unknown mode: " + args[0]);
        }
    }

    private static SolConnectionFactory connectionFactory() throws Exception {
        SolConnectionFactory factory = SolJmsUtility.createConnectionFactory();
        configure(factory);
        return factory;
    }

    private static SolXAConnectionFactory xaConnectionFactory() throws Exception {
        SolXAConnectionFactory factory = SolJmsUtility.createXAConnectionFactory();
        configure(factory);
        return factory;
    }

    private static void configure(SolConnectionFactory factory) throws JMSException {
        factory.setHost(required("SOLACE_HOST"));
        factory.setVPN(env("SOLACE_VPN", "default"));
        factory.setUsername(required("SOLACE_USERNAME"));
        factory.setPassword(required("SOLACE_PASSWORD"));
        factory.setDirectTransport(false);
    }

    private static void runLocalTransactions() throws Exception {
        String prefix = env("QUAL_PREFIX", "native-qual");
        long start = System.nanoTime();
        Map<String, Object> result = new LinkedHashMap<>();
        result.put("test", "local_jms_transactions");

        try (Connection txConnection = connectionFactory().createConnection();
             Connection observerConnection = connectionFactory().createConnection()) {
            txConnection.start();
            observerConnection.start();
            Session tx = txConnection.createSession(true, Session.SESSION_TRANSACTED);
            Session observer = observerConnection.createSession(false, Session.AUTO_ACKNOWLEDGE);

            Queue commitQueue = SolJmsUtility.createQueue(prefix + ".tx.commit");
            List<String> committed = ids(prefix + "-commit", 5);
            send(tx, commitQueue, committed);
            require(receiveOne(observer, commitQueue, ABSENCE_TIMEOUT_MS) == null,
                    "uncommitted publication became visible");
            tx.commit();
            List<Received> commitReceived = receive(observer, commitQueue, committed.size(), RECEIVE_TIMEOUT_MS);
            assertIds("commit", committed, commitReceived);

            Queue rollbackQueue = SolJmsUtility.createQueue(prefix + ".tx.rollback");
            send(tx, rollbackQueue, ids(prefix + "-rollback", 4));
            tx.rollback();
            require(receiveOne(observer, rollbackQueue, ABSENCE_TIMEOUT_MS) == null,
                    "rolled-back publications became visible");

            Queue redeliveryQueue = SolJmsUtility.createQueue(prefix + ".tx.redelivery");
            List<String> redeliveryIds = ids(prefix + "-redelivery", 3);
            send(tx, redeliveryQueue, redeliveryIds);
            tx.commit();
            List<Received> firstDelivery = receive(tx, redeliveryQueue, redeliveryIds.size(), RECEIVE_TIMEOUT_MS);
            assertIds("first delivery", redeliveryIds, firstDelivery);
            tx.rollback();
            List<Received> secondDelivery = receive(tx, redeliveryQueue, redeliveryIds.size(), RECEIVE_TIMEOUT_MS);
            assertIds("redelivery", redeliveryIds, secondDelivery);
            for (Received message : secondDelivery) {
                require(message.redelivered || message.deliveryCount > 1,
                        "rolled-back message was not marked redelivered: " + message.id);
            }
            tx.commit();

            result.put("status", "PASS");
            result.put("commit_count", committed.size());
            result.put("rollback_visible_count", 0);
            result.put("redelivery_count", secondDelivery.size());
            result.put("redelivery_ids", receivedIds(secondDelivery));
        }
        result.put("latency_ms", elapsedMs(start));
        printJson(result);
    }

    private static void publishOpenTransactionAndCrash(String queueName) throws Exception {
        Connection connection = connectionFactory().createConnection();
        connection.start();
        Session session = connection.createSession(true, Session.SESSION_TRANSACTED);
        send(session, SolJmsUtility.createQueue(queueName), ids(env("QUAL_PREFIX", "native-qual") + "-crash", 4));
        System.out.println("OPEN_TX_READY count=4");
        System.out.flush();
        Runtime.getRuntime().halt(73);
    }

    private static void assertQueueEmpty(String queueName) throws Exception {
        long start = System.nanoTime();
        try (Connection connection = connectionFactory().createConnection()) {
            connection.start();
            Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
            require(receiveOne(session, SolJmsUtility.createQueue(queueName), ABSENCE_TIMEOUT_MS) == null,
                    "crashed open transaction produced visible publications");
        }
        Map<String, Object> result = new LinkedHashMap<>();
        result.put("test", "open_transaction_client_crash");
        result.put("status", "PASS");
        result.put("visible_count", 0);
        result.put("latency_ms", elapsedMs(start));
        printJson(result);
    }

    private static void runXaBasic() throws Exception {
        String prefix = env("QUAL_PREFIX", "native-qual");
        long start = System.nanoTime();
        Map<String, Object> result = new LinkedHashMap<>();
        result.put("test", "xa_prepare_commit_rollback");
        try (XAConnection connection = xaConnectionFactory().createXAConnection()) {
            connection.start();
            XASession xaSession = connection.createXASession();
            XAResource resource = xaSession.getXAResource();
            Session session = xaSession.getSession();

            Queue commitQueue = SolJmsUtility.createQueue(prefix + ".xa.commit");
            List<String> commitIds = ids(prefix + "-xa-commit", 3);
            Xid commitXid = xid(prefix + "-xa-commit");
            resource.start(commitXid, XAResource.TMNOFLAGS);
            send(session, commitQueue, commitIds);
            resource.end(commitXid, XAResource.TMSUCCESS);
            int vote = resource.prepare(commitXid);
            require(vote == XAResource.XA_OK, "XA commit branch prepare vote was " + vote);
            resource.commit(commitXid, false);

            Queue rollbackQueue = SolJmsUtility.createQueue(prefix + ".xa.rollback");
            Xid rollbackXid = xid(prefix + "-xa-rollback");
            resource.start(rollbackXid, XAResource.TMNOFLAGS);
            send(session, rollbackQueue, ids(prefix + "-xa-rollback", 3));
            resource.end(rollbackXid, XAResource.TMSUCCESS);
            int rollbackVote = resource.prepare(rollbackXid);
            require(rollbackVote == XAResource.XA_OK, "XA rollback branch prepare vote was " + rollbackVote);
            resource.rollback(rollbackXid);

            Session observer = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
            List<Received> committed = receive(observer, commitQueue, commitIds.size(), RECEIVE_TIMEOUT_MS);
            assertIds("XA commit", commitIds, committed);
            require(receiveOne(observer, rollbackQueue, ABSENCE_TIMEOUT_MS) == null,
                    "XA rolled-back publications became visible");
            result.put("status", "PASS");
            result.put("prepare_vote", vote);
            result.put("commit_count", committed.size());
            result.put("rollback_visible_count", 0);
        }
        result.put("latency_ms", elapsedMs(start));
        printJson(result);
    }

    private static void prepareXaForRecovery(String xidFile) throws Exception {
        String prefix = env("QUAL_PREFIX", "native-qual");
        XidImpl xid = (XidImpl) xid(prefix + "-xa-recovery");
        try (XAConnection connection = xaConnectionFactory().createXAConnection()) {
            connection.start();
            XASession xaSession = connection.createXASession();
            XAResource resource = xaSession.getXAResource();
            resource.start(xid, XAResource.TMNOFLAGS);
            send(xaSession.getSession(), SolJmsUtility.createQueue(prefix + ".xa.recovery"),
                    ids(prefix + "-xa-recovery", 2));
            resource.end(xid, XAResource.TMSUCCESS);
            int vote = resource.prepare(xid);
            require(vote == XAResource.XA_OK, "XA recovery branch prepare vote was " + vote);
        }
        Files.write(Paths.get(xidFile), xid.serialize().getBytes(StandardCharsets.UTF_8));
        Map<String, Object> result = new LinkedHashMap<>();
        result.put("test", "xa_restart_prepare");
        result.put("status", "PASS");
        result.put("prepared_count", 2);
        printJson(result);
    }

    private static void recoverAndCommitXa(String xidFile) throws Exception {
        long start = System.nanoTime();
        String prefix = env("QUAL_PREFIX", "native-qual");
        XidImpl expected = XidImpl.parse(new String(Files.readAllBytes(Paths.get(xidFile)), StandardCharsets.UTF_8).trim());
        int recoveredCount = 0;
        try (XAConnection connection = xaConnectionFactory().createXAConnection()) {
            connection.start();
            XASession xaSession = connection.createXASession();
            XAResource resource = xaSession.getXAResource();
            List<Xid> recovered = new ArrayList<>();
            recovered.addAll(Arrays.asList(resource.recover(XAResource.TMSTARTRSCAN)));
            recovered.addAll(Arrays.asList(resource.recover(XAResource.TMENDRSCAN)));
            for (Xid candidate : recovered) {
                if (sameXid(expected, candidate)) {
                    resource.commit(candidate, false);
                    recoveredCount++;
                }
            }
            require(recoveredCount == 1, "expected one matching recovered XA branch, got " + recoveredCount);
            Session observer = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
            List<String> expectedIds = ids(prefix + "-xa-recovery", 2);
            List<Received> received = receive(observer, SolJmsUtility.createQueue(prefix + ".xa.recovery"),
                    expectedIds.size(), RECEIVE_TIMEOUT_MS);
            assertIds("XA restart recovery", expectedIds, received);
        }
        Map<String, Object> result = new LinkedHashMap<>();
        result.put("test", "xa_restart_recovery");
        result.put("status", "PASS");
        result.put("recovered_branches", recoveredCount);
        result.put("committed_count", 2);
        result.put("latency_ms", elapsedMs(start));
        printJson(result);
    }

    private static void publishTopic(String topicName, String idPrefix, int count) throws Exception {
        long start = System.nanoTime();
        try (Connection connection = connectionFactory().createConnection()) {
            connection.start();
            Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
            send(session, SolJmsUtility.createTopic(topicName), ids(idPrefix, count));
        }
        Map<String, Object> result = new LinkedHashMap<>();
        result.put("test", "replay_topic_publish");
        result.put("status", "PASS");
        result.put("published_count", count);
        result.put("latency_ms", elapsedMs(start));
        printJson(result);
    }

    private static void traceWorkload(String topicName, String idPrefix, int count) throws Exception {
        long start = System.nanoTime();
        List<String> expected = ids(idPrefix, count);
        try (Connection connection = connectionFactory().createConnection()) {
            connection.start();
            Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
            javax.jms.Topic subscription = SolJmsUtility.createTopic(topicName + "/>");
            try (MessageConsumer consumer = session.createConsumer(subscription)) {
                for (String id : expected) {
                    send(session, SolJmsUtility.createTopic(topicName + "/" + id), Arrays.asList(id));
                }
                List<Received> received = new ArrayList<>();
                long deadline = System.currentTimeMillis() + RECEIVE_TIMEOUT_MS * 2L;
                while (received.size() < count) {
                    Message message = consumer.receive(Math.max(1L, deadline - System.currentTimeMillis()));
                    if (message == null) break;
                    received.add(received(message));
                }
                assertIds("trace workload", expected, received);
            }
        }
        Map<String, Object> result = new LinkedHashMap<>();
        result.put("test", "trace_workload");
        result.put("status", "PASS");
        result.put("message_count", count);
        result.put("message_ids", expected);
        result.put("latency_ms", elapsedMs(start));
        printJson(result);
    }

    private static void publishQueue(String queueName, String idPrefix, int count) throws Exception {
        long start = System.nanoTime();
        try (Connection connection = connectionFactory().createConnection()) {
            connection.start();
            Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
            send(session, SolJmsUtility.createQueue(queueName), ids(idPrefix, count));
        }
        Map<String, Object> result = new LinkedHashMap<>();
        result.put("test", "queue_publish");
        result.put("status", "PASS");
        result.put("message_count", count);
        result.put("message_ids", ids(idPrefix, count));
        result.put("latency_ms", elapsedMs(start));
        printJson(result);
    }

    private static void consumeQueue(String queueName, String idPrefix, int count) throws Exception {
        long start = System.nanoTime();
        List<String> expected = ids(idPrefix, count);
        try (Connection connection = connectionFactory().createConnection()) {
            connection.start();
            Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
            List<Received> actual = receive(session, SolJmsUtility.createQueue(queueName), count, RECEIVE_TIMEOUT_MS * 2L);
            assertIds("queue consume", expected, actual);
        }
        Map<String, Object> result = new LinkedHashMap<>();
        result.put("test", "queue_consume");
        result.put("status", "PASS");
        result.put("message_count", count);
        result.put("message_ids", expected);
        result.put("latency_ms", elapsedMs(start));
        printJson(result);
    }

    private static void assertReplay(String queueName, String replayPrefix, int replayCount,
                                     String livePrefix, int liveCount) throws Exception {
        long start = System.nanoTime();
        try (Connection connection = connectionFactory().createConnection()) {
            connection.start();
            Session producerSession = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
            Session consumerSession = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
            Queue queue = SolJmsUtility.createQueue(queueName);
            List<Received> all = new ArrayList<>();
            try (MessageConsumer consumer = consumerSession.createConsumer(queue)) {
                Message first = consumer.receive(RECEIVE_TIMEOUT_MS * 2L);
                require(first != null, "replay delivered no first message");
                Received firstReceived = received(first);
                require(firstReceived.id.startsWith(replayPrefix + "-"),
                        "first delivery was not replayed: " + firstReceived.id);
                all.add(firstReceived);

                // Publish live traffic after replay delivery has begun and before its backlog is drained.
                send(producerSession, queue, ids(livePrefix, liveCount));
                long deadline = System.currentTimeMillis() + RECEIVE_TIMEOUT_MS * 2L;
                while (all.size() < (replayCount + liveCount) * 3) {
                    long timeout = all.size() < replayCount + liveCount
                            ? Math.max(1L, deadline - System.currentTimeMillis()) : ABSENCE_TIMEOUT_MS;
                    if (timeout <= 0) break;
                    Message message = consumer.receive(timeout);
                    if (message == null) break;
                    all.add(received(message));
                }
            }
            List<String> expected = new ArrayList<>();
            expected.addAll(ids(replayPrefix, replayCount));
            expected.addAll(ids(livePrefix, liveCount));
            assertCompleteKnownSet("replay plus live", expected, all);
            List<String> actual = receivedIds(all);
            int lastReplay = -1;
            int firstLive = Integer.MAX_VALUE;
            for (int i = 0; i < actual.size(); i++) {
                if (actual.get(i).startsWith(replayPrefix + "-")) lastReplay = i;
                if (actual.get(i).startsWith(livePrefix + "-")) firstLive = Math.min(firstLive, i);
            }
            Map<String, Object> result = new LinkedHashMap<>();
            result.put("test", "ordinary_queue_replay_with_live_traffic");
            result.put("status", "PASS");
            result.put("replayed_count", replayCount);
            result.put("live_count", liveCount);
            result.put("duplicate_count", all.size() - new java.util.HashSet<>(actual).size());
            result.put("observed_order", actual);
            result.put("replay_before_live", lastReplay < firstLive);
            result.put("latency_ms", elapsedMs(start));
            printJson(result);
        }
    }

    private static void send(Session session, Destination destination, List<String> ids) throws JMSException {
        try (MessageProducer producer = session.createProducer(destination)) {
            producer.setDeliveryMode(DeliveryMode.PERSISTENT);
            for (String id : ids) {
                TextMessage message = session.createTextMessage(id);
                message.setStringProperty("qualificationId", id);
                producer.send(message);
            }
        }
    }

    private static Received receiveOne(Session session, Queue queue, long timeoutMs) throws JMSException {
        try (MessageConsumer consumer = session.createConsumer(queue)) {
            Message message = consumer.receive(timeoutMs);
            if (message == null) return null;
            return received(message);
        }
    }

    private static List<Received> receive(Session session, Queue queue, int count, long timeoutMs) throws JMSException {
        List<Received> received = new ArrayList<>();
        long deadline = System.currentTimeMillis() + timeoutMs;
        try (MessageConsumer consumer = session.createConsumer(queue)) {
            while (received.size() < count) {
                long remaining = deadline - System.currentTimeMillis();
                if (remaining <= 0) break;
                Message message = consumer.receive(remaining);
                if (message == null) break;
                received.add(received(message));
            }
        }
        return received;
    }

    private static Received received(Message message) throws JMSException {
        String id = message.getStringProperty("qualificationId");
        int deliveryCount = message.propertyExists("JMSXDeliveryCount") ? message.getIntProperty("JMSXDeliveryCount") : 0;
        return new Received(id, message.getJMSRedelivered(), deliveryCount);
    }

    private static List<String> ids(String prefix, int count) {
        List<String> result = new ArrayList<>();
        for (int i = 0; i < count; i++) result.add(prefix + "-" + i);
        return result;
    }

    private static List<String> receivedIds(List<Received> messages) {
        List<String> ids = new ArrayList<>();
        for (Received message : messages) ids.add(message.id);
        return ids;
    }

    private static void assertIds(String label, List<String> expected, List<Received> actual) {
        List<String> actualIds = receivedIds(actual);
        require(expected.equals(actualIds), label + " IDs differ: expected=" + expected + " actual=" + actualIds);
    }

    private static void assertCompleteKnownSet(String label, List<String> expected, List<Received> actual) {
        List<String> actualIds = receivedIds(actual);
        java.util.HashSet<String> expectedSet = new java.util.HashSet<>(expected);
        java.util.HashSet<String> actualSet = new java.util.HashSet<>(actualIds);
        require(expectedSet.equals(actualSet), label + " ID set differs: expected=" + expected + " actual=" + actualIds);
        for (String id : actualIds) require(expectedSet.contains(id), label + " contained unknown ID: " + id);
    }

    private static Xid xid(String seed) {
        byte[] global = (seed + "-" + UUID.randomUUID()).getBytes(StandardCharsets.UTF_8);
        byte[] branch = "branch-0".getBytes(StandardCharsets.UTF_8);
        return new XidImpl(0x534f4c, Arrays.copyOf(global, Math.min(global.length, Xid.MAXGTRIDSIZE)), branch);
    }

    private static boolean sameXid(Xid left, Xid right) {
        return left.getFormatId() == right.getFormatId()
                && Arrays.equals(left.getGlobalTransactionId(), right.getGlobalTransactionId())
                && Arrays.equals(left.getBranchQualifier(), right.getBranchQualifier());
    }

    private static String required(String name) {
        String value = System.getenv(name);
        if (value == null || value.isEmpty()) throw new IllegalStateException(name + " is required");
        return value;
    }

    private static String env(String name, String fallback) {
        String value = System.getenv(name);
        return value == null || value.isEmpty() ? fallback : value;
    }

    private static long elapsedMs(long start) {
        return (System.nanoTime() - start) / 1_000_000L;
    }

    private static void require(boolean condition, String message) {
        if (!condition) throw new AssertionError(message);
    }

    private static void printJson(Map<String, Object> values) {
        StringBuilder out = new StringBuilder("{");
        boolean first = true;
        for (Map.Entry<String, Object> entry : values.entrySet()) {
            if (!first) out.append(',');
            first = false;
            out.append('"').append(escape(entry.getKey())).append("\":");
            appendJson(out, entry.getValue());
        }
        System.out.println(out.append('}'));
    }

    private static void appendJson(StringBuilder out, Object value) {
        if (value == null) out.append("null");
        else if (value instanceof Number || value instanceof Boolean) out.append(value);
        else if (value instanceof Iterable) {
            out.append('[');
            boolean first = true;
            for (Object item : (Iterable<?>) value) {
                if (!first) out.append(',');
                first = false;
                appendJson(out, item);
            }
            out.append(']');
        } else out.append('"').append(escape(String.valueOf(value))).append('"');
    }

    private static String escape(String text) {
        return text.replace("\\", "\\\\").replace("\"", "\\\"").replace("\n", "\\n").replace("\r", "\\r");
    }

    private static final class Received {
        private final String id;
        private final boolean redelivered;
        private final int deliveryCount;

        private Received(String id, boolean redelivered, int deliveryCount) {
            this.id = id;
            this.redelivered = redelivered;
            this.deliveryCount = deliveryCount;
        }
    }

    private static final class XidImpl implements Xid {
        private final int formatId;
        private final byte[] globalId;
        private final byte[] branchId;

        private XidImpl(int formatId, byte[] globalId, byte[] branchId) {
            this.formatId = formatId;
            this.globalId = globalId.clone();
            this.branchId = branchId.clone();
        }

        public int getFormatId() { return formatId; }
        public byte[] getGlobalTransactionId() { return globalId.clone(); }
        public byte[] getBranchQualifier() { return branchId.clone(); }

        private String serialize() {
            return formatId + ":" + hex(globalId) + ":" + hex(branchId);
        }

        private static XidImpl parse(String value) {
            String[] parts = value.split(":", -1);
            require(parts.length == 3, "invalid XID serialization");
            return new XidImpl(Integer.parseInt(parts[0]), unhex(parts[1]), unhex(parts[2]));
        }

        private static String hex(byte[] bytes) {
            StringBuilder value = new StringBuilder();
            for (byte b : bytes) value.append(String.format("%02x", b & 0xff));
            return value.toString();
        }

        private static byte[] unhex(String value) {
            require(value.length() % 2 == 0, "invalid hex length");
            byte[] bytes = new byte[value.length() / 2];
            for (int i = 0; i < value.length(); i += 2) {
                bytes[i / 2] = (byte) Integer.parseInt(value.substring(i, i + 2), 16);
            }
            return bytes;
        }
    }
}
