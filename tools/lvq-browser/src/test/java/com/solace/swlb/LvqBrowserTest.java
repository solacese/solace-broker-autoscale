package com.solace.swlb;

import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.nio.charset.StandardCharsets;
import java.util.Collections;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

final class LvqBrowserTest {
    @Test
    void requestAndResponseAreJava8CompatibleBeans() {
        LvqBrowser.Request request = new LvqBrowser.Request();
        request.operation = "browse_latest";
        request.queue = "acme.ctl.group.membership";
        request.timeoutMs = 1000;
        assertEquals("browse_latest", request.operation);

        LvqBrowser.BrowsedMessage message = new LvqBrowser.BrowsedMessage("e30=", "id", null);
        LvqBrowser.Response response = new LvqBrowser.Response(true, Collections.singletonList(message), null);
        assertEquals("e30=", response.messages.get(0).payloadBase64);
    }

    @Test
    void acceptsBoundedNamespace() {
        assertEquals("qualification-01", LvqBrowser.validateNamespace("qualification-01"));
        assertEquals("acme.prod", LvqBrowser.validateNamespace("acme.prod"));
    }

    @Test
    void rejectsUnsafeNamespace() {
        assertThrows(IllegalArgumentException.class, () -> LvqBrowser.validateNamespace("../other"));
        assertThrows(IllegalArgumentException.class, () -> LvqBrowser.validateNamespace("UPPER"));
        assertThrows(IllegalArgumentException.class, () -> LvqBrowser.validateNamespace("acme..prod"));
        assertThrows(IllegalArgumentException.class, () -> LvqBrowser.validateNamespace("-acme"));
    }

    @Test
    void scopesQueueToExactConfiguredNamespace() {
        assertEquals("acme.ctl.group.membership", LvqBrowser.validateQueue("acme", "acme.ctl.group.membership"));
        assertThrows(IllegalArgumentException.class,
                () -> LvqBrowser.validateQueue("acme", "other.ctl.group.membership"));
        assertThrows(IllegalArgumentException.class,
                () -> LvqBrowser.validateQueue("acme", "acme2.ctl.group.membership"));
    }

    @Test
    void redactsUnexpectedFailures() {
        assertEquals("browse failed: RuntimeException", LvqBrowser.safeError(new RuntimeException("secret")));
        assertEquals("bad request", LvqBrowser.safeError(new IllegalArgumentException("bad request")));
    }

    @Test
    void readsBoundedUtf8Lines() throws Exception {
        ByteArrayInputStream input = new ByteArrayInputStream("first\r\nsecond\n".getBytes(StandardCharsets.UTF_8));
        assertArrayEquals("first".getBytes(StandardCharsets.UTF_8), LvqBrowser.readLine(input, 16));
        assertArrayEquals("second".getBytes(StandardCharsets.UTF_8), LvqBrowser.readLine(input, 16));
        assertNull(LvqBrowser.readLine(input, 16));
    }

    @Test
    void rejectsOversizedAndMalformedLines() {
        assertThrows(IllegalArgumentException.class,
                () -> LvqBrowser.readLine(new ByteArrayInputStream("too-long\n".getBytes(StandardCharsets.UTF_8)), 3));
        assertThrows(IllegalArgumentException.class,
                () -> LvqBrowser.readLine(new ByteArrayInputStream(new byte[]{(byte) 0xc3, (byte) 0x28, '\n'}), 16));
    }
}
