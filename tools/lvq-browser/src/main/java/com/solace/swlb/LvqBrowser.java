package com.solace.swlb;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.solacesystems.jcsmp.Browser;
import com.solacesystems.jcsmp.BrowserProperties;
import com.solacesystems.jcsmp.BytesXMLMessage;
import com.solacesystems.jcsmp.JCSMPException;
import com.solacesystems.jcsmp.JCSMPFactory;
import com.solacesystems.jcsmp.JCSMPProperties;
import com.solacesystems.jcsmp.JCSMPSession;
import com.solacesystems.jcsmp.Queue;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.nio.charset.CodingErrorAction;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Base64;
import java.util.List;
import java.util.Objects;

/** A narrow, non-destructive queue browser used only for Broker 0 membership LVQs. */
public final class LvqBrowser implements AutoCloseable {
    static final int MAX_REQUEST_BYTES = 16 * 1024;
    static final int MAX_PAYLOAD_BYTES = 1024 * 1024;
    static final int DEFAULT_TIMEOUT_MS = 10_000;
    static final int MAX_MESSAGES = 16;

    private final String namespace;
    private final JCSMPSession session;

    static final class Request {
        public String operation;
        public String queue;
        public Integer timeoutMs;
    }

    @JsonInclude(JsonInclude.Include.NON_NULL)
    static final class BrowsedMessage {
        public final String payloadBase64;
        public final String applicationMessageId;
        public final String timestamp;

        BrowsedMessage(String payloadBase64, String applicationMessageId, String timestamp) {
            this.payloadBase64 = payloadBase64;
            this.applicationMessageId = applicationMessageId;
            this.timestamp = timestamp;
        }
    }

    @JsonInclude(JsonInclude.Include.NON_NULL)
    static final class Response {
        public final boolean ok;
        public final List<BrowsedMessage> messages;
        public final String error;

        Response(boolean ok, List<BrowsedMessage> messages, String error) {
            this.ok = ok;
            this.messages = messages;
            this.error = error;
        }

        static Response failure(String message) {
            return new Response(false, null, message);
        }
    }

    LvqBrowser(String namespace, JCSMPSession session) {
        this.namespace = validateNamespace(namespace);
        this.session = Objects.requireNonNull(session, "session");
    }

    static String validateNamespace(String value) {
        if (value == null || value.isEmpty() || value.length() > 253) {
            throw new IllegalArgumentException("invalid browser namespace");
        }
        for (String label : value.split("\\.", -1)) {
            if (label.length() > 63 || !label.matches("[a-z0-9](?:[a-z0-9-]*[a-z0-9])?")) {
                throw new IllegalArgumentException("invalid browser namespace");
            }
        }
        return value;
    }

    static String validateQueue(String namespace, String queue) {
        String prefix = validateNamespace(namespace) + ".";
        if (queue == null || queue.length() > 200 || !queue.startsWith(prefix)
                || !queue.matches("[A-Za-z0-9_.-]+")) {
            throw new IllegalArgumentException("queue is outside the configured namespace");
        }
        return queue;
    }

    Response browseLatest(Request request) throws JCSMPException {
        if (request == null || !"browse_latest".equals(request.operation)) {
            throw new IllegalArgumentException("only browse_latest is supported");
        }
        int timeout = request.timeoutMs == null ? DEFAULT_TIMEOUT_MS : request.timeoutMs;
        if (timeout < 1 || timeout > 60_000) {
            throw new IllegalArgumentException("timeoutMs must be between 1 and 60000");
        }
        Queue queue = JCSMPFactory.onlyInstance().createQueue(validateQueue(namespace, request.queue));
        BrowserProperties properties = new BrowserProperties();
        properties.setEndpoint(queue);
        properties.setTransportWindowSize(32);
        Browser browser = session.createBrowser(properties);
        try {
            List<BrowsedMessage> messages = new ArrayList<>();
            for (int index = 0; index < MAX_MESSAGES; index++) {
                BytesXMLMessage message = browser.getNext(index == 0 ? timeout : 1);
                if (message == null) break;
                byte[] payload = payloadBytes(message);
                if (payload == null) return Response.failure("browsed message has no binary payload");
                if (payload.length > MAX_PAYLOAD_BYTES) return Response.failure("browsed payload exceeds one MiB");
                Long senderTimestamp = message.getSenderTimestamp();
                String timestamp = senderTimestamp == null ? null : Instant.ofEpochMilli(senderTimestamp).toString();
                messages.add(new BrowsedMessage(Base64.getEncoder().encodeToString(payload),
                        message.getApplicationMessageId(), timestamp));
                if (!browser.hasMore()) break;
            }
            if (messages.isEmpty()) return Response.failure("membership LVQ is empty");
            if (messages.size() == MAX_MESSAGES && browser.hasMore()) {
                return Response.failure("membership LVQ exceeds message limit");
            }
            return new Response(true, new ArrayList<>(messages), null);
        } finally {
            // Browser.close() releases the browser flow. Browser.remove() is deliberately never called.
            browser.close();
        }
    }

    static byte[] payloadBytes(BytesXMLMessage message) {
        if (message.hasAttachment()) {
            ByteBuffer attachment = message.getAttachmentByteBuffer().asReadOnlyBuffer();
            byte[] payload = new byte[attachment.remaining()];
            attachment.get(payload);
            return payload;
        }
        return message.getBytes();
    }

    @Override
    public void close() {
        session.closeSession();
    }

    private static String requiredEnv(String name) {
        String value = System.getenv(name);
        if (value == null || value.trim().isEmpty()) throw new IllegalStateException(name + " is required");
        return value;
    }

    static JCSMPSession connect() throws JCSMPException {
        JCSMPProperties properties = new JCSMPProperties();
        properties.setProperty(JCSMPProperties.HOST, requiredEnv("SWLB_CONTROL_HOST"));
        properties.setProperty(JCSMPProperties.VPN_NAME, requiredEnv("SWLB_CONTROL_VPN"));
        properties.setProperty(JCSMPProperties.USERNAME, requiredEnv("SWLB_CONTROL_BROWSER_USERNAME"));
        properties.setProperty(JCSMPProperties.PASSWORD, requiredEnv("SWLB_CONTROL_BROWSER_PASSWORD"));
        String trustStore = System.getenv("SWLB_CONTROL_TRUST_STORE");
        if (trustStore != null && !trustStore.trim().isEmpty()) {
            properties.setProperty(JCSMPProperties.SSL_TRUST_STORE, trustStore);
        }
        properties.setProperty(JCSMPProperties.CLIENT_NAME, "swlb-lvq-browser-" + System.nanoTime());
        JCSMPSession session = JCSMPFactory.onlyInstance().createSession(properties);
        session.connect();
        return session;
    }

    public static void main(String[] args) throws Exception {
        if (args.length != 1) {
            System.err.println("usage: java -jar lvq-browser.jar <namespace>");
            System.exit(2);
        }
        ObjectMapper mapper = new ObjectMapper()
                .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, true);
        try (LvqBrowser service = new LvqBrowser(args[0], connect())) {
            while (true) {
                byte[] line;
                try {
                    line = readLine(System.in, MAX_REQUEST_BYTES);
                    if (line == null) break;
                } catch (IllegalArgumentException error) {
                    System.out.println(mapper.writeValueAsString(Response.failure(error.getMessage())));
                    System.out.flush();
                    // The oversized request was consumed through its newline, so the
                    // stream remains synchronized for the next request.
                    continue;
                }
                Response response;
                try {
                    response = service.browseLatest(mapper.readValue(line, Request.class));
                } catch (Exception error) {
                    response = Response.failure(safeError(error));
                }
                System.out.println(mapper.writeValueAsString(response));
                System.out.flush();
            }
        }
    }

    static byte[] readLine(InputStream input, int maximum) throws IOException {
        ByteArrayOutputStream line = new ByteArrayOutputStream(Math.min(maximum, 1024));
        boolean oversized = false;
        while (true) {
            int value = input.read();
            if (value == -1) {
                if (line.size() == 0 && !oversized) return null;
                break;
            }
            if (value == '\n') break;
            if (line.size() < maximum) {
                line.write(value);
            } else {
                oversized = true;
            }
        }
        if (oversized) throw new IllegalArgumentException("request exceeds limit");
        byte[] bytes = line.toByteArray();
        int length = bytes.length;
        if (length > 0 && bytes[length - 1] == '\r') {
            bytes = java.util.Arrays.copyOf(bytes, length - 1);
        }
        try {
            StandardCharsets.UTF_8.newDecoder()
                    .onMalformedInput(CodingErrorAction.REPORT)
                    .onUnmappableCharacter(CodingErrorAction.REPORT)
                    .decode(ByteBuffer.wrap(bytes));
        } catch (java.nio.charset.CharacterCodingException error) {
            throw new IllegalArgumentException("request is not valid UTF-8");
        }
        return bytes;
    }

    static String safeError(Throwable error) {
        if (error instanceof IllegalArgumentException) return error.getMessage();
        return "browse failed: " + error.getClass().getSimpleName();
    }
}
