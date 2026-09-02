package com.solace.autoscale.client;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Tier-1 resolver (§9). Calls the assignment service, caches the result, and FAILS OPEN to the
 * cache when the service is unreachable - the control plane being down must never take the
 * application down (§9.4). Returns a location only; never a credential.
 *
 * This is a reference skeleton: JSON parsing is intentionally minimal (swap in Jackson/Gson in a
 * real build). It mirrors the Python resolver's behaviour.
 */
public final class Resolver {

    /** Per-protocol endpoint map plus placement metadata. */
    public static final class Assignment {
        public final String brokerId;
        public final String msgVpn;
        public final String state;
        public final int leaseSeconds;
        public final Map<String, String> endpoints;

        public Assignment(String brokerId, String msgVpn, String state, int leaseSeconds,
                          Map<String, String> endpoints) {
            this.brokerId = brokerId;
            this.msgVpn = msgVpn;
            this.state = state;
            this.leaseSeconds = leaseSeconds;
            this.endpoints = endpoints;
        }

        public String endpoint(String protocol) {
            String uri = endpoints.get(protocol);
            if (uri == null) {
                throw new IllegalArgumentException("protocol not offered: " + protocol);
            }
            return uri;
        }
    }

    private final String baseUrl;
    private final HttpClient http = HttpClient.newBuilder()
            .connectTimeout(Duration.ofSeconds(5)).build();
    private final Map<String, Assignment> cache = new ConcurrentHashMap<>();

    public Resolver(String baseUrl) {
        this.baseUrl = baseUrl.replaceAll("/+$", "");
    }

    /**
     * Resolve (shard, clientId, mode) to an Assignment. On a service error, returns the cached
     * assignment if present (fail open); otherwise throws.
     */
    public Assignment resolve(String shard, String clientId, String mode, String protocol) {
        String key = shard + "|" + clientId + "|" + mode;
        try {
            String q = "shard=" + shard + "&client_id=" + clientId + "&mode=" + mode
                    + (protocol != null ? "&protocol=" + protocol : "");
            HttpRequest req = HttpRequest.newBuilder(URI.create(baseUrl + "/assignment?" + q))
                    .timeout(Duration.ofSeconds(5)).GET().build();
            HttpResponse<String> resp = http.send(req, HttpResponse.BodyHandlers.ofString());
            if (resp.statusCode() / 100 != 2) {
                throw new RuntimeException("assignment service returned " + resp.statusCode());
            }
            Assignment a = Json.parseAssignment(resp.body());
            cache.put(key, a);
            return a;
        } catch (Exception e) {
            Assignment cached = cache.get(key);
            if (cached != null) {
                return cached; // fail open
            }
            throw new RuntimeException(
                "assignment service unreachable and no cached assignment for " + key, e);
        }
    }

    // Minimal placeholder; replace with Jackson/Gson in a real build.
    static final class Json {
        static Assignment parseAssignment(String body) {
            throw new UnsupportedOperationException(
                "wire up a JSON library (Jackson/Gson) here; see Python resolver for the shape");
        }
    }
}
