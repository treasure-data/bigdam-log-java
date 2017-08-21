package com.treasuredata.bigdam.log;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

import java.time.Instant;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

import io.sentry.SentryClient;
import io.sentry.event.EventBuilder;
import org.komamitsu.fluency.EventTime;
import org.komamitsu.fluency.Fluency;
import org.slf4j.Logger;

import org.junit.After;
import org.junit.Test;

import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.*;
import static org.mockito.Mockito.*;

public class LogTest
{
    @After
    public void teardown()
    {
        Log.reset();
    }

    @Test
    public void initLoggerInDefault()
            throws Exception
    {
        Logger underlying = mock(Logger.class);
        SentryClient sentry = mock(SentryClient.class);
        Fluency fluency = mock(Fluency.class);
        Log.setup(false, null, null, false, null, 0, clazz -> underlying, () -> sentry, (s, i) -> fluency);
        Log log = new Log(LogTest.class);

        log.error("message", ImmutableMap.of());
        verify(underlying).error("message");
        verify(sentry, never()).sendEvent(any(EventBuilder.class));
        verify(fluency, never()).emit(any(), any(), any());
    }

    @Test
    public void initLoggerWithSentry()
            throws Exception
    {
        Logger underlying = mock(Logger.class);
        SentryClient sentry = mock(SentryClient.class);
        Fluency fluency = mock(Fluency.class);
        Log.setup(true, "https://public:private@host:443/1", Optional.empty(), true, "localhost", 24224, clazz -> underlying, () -> sentry, (s, i) -> fluency);
        Log log = new Log(LogTest.class);

        String message = "yaaaaaaaaaaay";
        Exception e = null;
        try {
            throw new RuntimeException(message);
        }
        catch (Exception ex) {
            e = ex;
            log.exception(ex);
        }

        assertThat(e, is(instanceOf(RuntimeException.class)));
        assertThat(e.getMessage(), is(message));
        verify(underlying).error(message, e);
        verify(sentry, times(1)).sendEvent(any(EventBuilder.class));
        verify(fluency, times(1)).emit(eq("bigdam.log.error"), any(EventTime.class), eq(ImmutableMap.of("message", "java.lang.RuntimeException:" + message)));
    }

    @Test
    public void initLoggerWithFluency()
            throws Exception
    {
        Logger underlying = mock(Logger.class);
        SentryClient sentry = mock(SentryClient.class);
        Fluency fluency = mock(Fluency.class);
        Log.setup(false, null, null, true, "localhost", 24224, clazz -> underlying, () -> sentry, (s, i) -> fluency);
        Log log = new Log(LogTest.class);

        log.error("message 1", ImmutableMap.of());
        log.warn("message 2", ImmutableMap.of());
        log.info("message 3", ImmutableMap.of());
        log.debug("message 4", ImmutableMap.of());
        log.trace("message 5", ImmutableMap.of());
        verify(underlying).error("message 1");
        verify(underlying).warn("message 2");
        verify(underlying).info("message 3");
        verify(underlying).debug("message 4");
        verify(underlying).trace("message 5");
        verify(fluency).emit(eq("bigdam.log.error"), any(EventTime.class), eq(ImmutableMap.of("message", "message 1")));
        verify(fluency).emit(eq("bigdam.log.warn"), any(EventTime.class), eq(ImmutableMap.of("message", "message 2")));
        verify(fluency).emit(eq("bigdam.log.info"), any(EventTime.class), eq(ImmutableMap.of("message", "message 3")));
        verify(fluency).emit(eq("bigdam.log.debug"), any(EventTime.class), eq(ImmutableMap.of("message", "message 4")));
        verify(fluency).emit(eq("bigdam.log.trace"), any(EventTime.class), eq(ImmutableMap.of("message", "message 5")));

        verify(sentry, never()).sendEvent(any(EventBuilder.class));
    }

    @Test
    public void logWithAttributes()
            throws Exception
    {
        Logger underlying = mock(Logger.class);
        SentryClient sentry = mock(SentryClient.class);
        Fluency fluency = mock(Fluency.class);
        Log.setup(false, null, null, true, "localhost", 24224, clazz -> underlying, () -> sentry, (s, i) -> fluency);
        Log log = new Log(LogTest.class);

        log.error("message", ImmutableMap.of("k", "vvvvvvvvvvvvvv", "k1", "v1", "k2", "v2"));
        verify(underlying).error("message");
        verify(fluency).emit(eq("bigdam.log.error"), any(EventTime.class), eq(ImmutableMap.of("message", "message", "k", "vvvvvvvvvvvvvv", "k1", "v1", "k2", "v2")));

        verify(sentry, never()).sendEvent(any(EventBuilder.class));
    }

    @Test
    public void defaultAttributes()
            throws Exception
    {
        Logger underlying = mock(Logger.class);
        SentryClient sentry = mock(SentryClient.class);
        Fluency fluency = mock(Fluency.class);
        Log.setup(false, null, null, true, "localhost", 24224, clazz -> underlying, () -> sentry, (s, i) -> fluency);
        Log.setDefaultAttributes(ImmutableMap.of("mykey", "myvalue", "myname", "tagomoris"));
        Log log = new Log(LogTest.class);

        log.error("message", ImmutableMap.of());
        verify(underlying).error("message");
        verify(fluency).emit(eq("bigdam.log.error"), any(EventTime.class), eq(ImmutableMap.of("message", "message", "mykey", "myvalue", "myname", "tagomoris")));

        verify(sentry, never()).sendEvent(any(EventBuilder.class));
    }

    @Test
    public void attributes()
            throws Exception
    {
        Logger underlying = mock(Logger.class);
        SentryClient sentry = mock(SentryClient.class);
        Fluency fluency = mock(Fluency.class);
        Log.setup(false, null, null, true, "localhost", 24224, clazz -> underlying, () -> sentry, (s, i) -> fluency);
        Map<String, Object> defaults = ImmutableMap.of("mykey", "myvalue", "myname", "tagomoris");
        Log.setDefaultAttributes(defaults);
        Log log = new Log(LogTest.class);
        Map<String, Object> attrs = ImmutableMap.of("k", "vvvvvvvvvvvvvv", "k1", "v1", "k2", "v2");
        log.error("message", attrs);

        Map<String, Object> expected = ImmutableMap.<String, Object>builder()
                .put("message", "message")
                .putAll(attrs)
                .putAll(defaults)
                .build();

        verify(underlying).error("message");
        verify(fluency).emit(eq("bigdam.log.error"), any(EventTime.class), eq(expected));

        verify(sentry, never()).sendEvent(any(EventBuilder.class));
    }

    private static class JustPojo
    {
        private final String v1;
        private final Boolean v2;
        private final Integer v3;

        public JustPojo(final String v1, final Boolean v2, final Integer v3)
        {
            this.v1 = v1;
            this.v2 = v2;
            this.v3 = v3;
        }

        @Override
        public String toString()
        {
            return String.format("JustPojo{%s,%s,%s}", v1, v2, v3);
        }
    }

    @Test
    public void attributesWithJavaObjects()
            throws Exception
    {
        Logger underlying = mock(Logger.class);
        SentryClient sentry = mock(SentryClient.class);
        Fluency fluency = mock(Fluency.class);
        Log.setup(false, null, null, true, "localhost", 24224, clazz -> underlying, () -> sentry, (s, i) -> fluency);
        Map<String, Object> defaults = ImmutableMap.of("mykey", "myvalue", "myname", "tagomoris");
        Log.setDefaultAttributes(defaults);
        Log log = new Log(LogTest.class);

        Instant t = Instant.now();
        JustPojo p1 = new JustPojo("p1", true, 1);
        JustPojo p2 = new JustPojo("p2", false, null);

        Map<String, Object> attrs = new HashMap<>();
        attrs.put("t", t);
        attrs.put("p1", p1);
        attrs.put("p2", p2);
        attrs.put("n", null);
        log.error("message", attrs);

        Map<String, Object> expected = new HashMap<>();
        expected.put("message", "message");
        expected.put("t", t.toString());
        expected.put("p1", "JustPojo{p1,true,1}");
        expected.put("p2", "JustPojo{p2,false,null}");
        expected.put("n", null);
        expected.putAll(defaults);

        verify(underlying).error("message");
        verify(fluency).emit(eq("bigdam.log.error"), any(EventTime.class), eq(expected));

        verify(sentry, never()).sendEvent(any(EventBuilder.class));
    }

    @Test
    public void tagPrefixModified()
            throws Exception
    {
        Logger underlying = mock(Logger.class);
        SentryClient sentry = mock(SentryClient.class);
        Fluency fluency = mock(Fluency.class);
        Log.setup(false, null, null, true, "localhost", 24224, clazz -> underlying, () -> sentry, (s, i) -> fluency);
        Log.setTagPrefix("bigdam.test.log.");
        Log log = new Log(LogTest.class);

        log.error("message", ImmutableMap.of());
        verify(underlying).error("message");
        verify(fluency).emit(eq("bigdam.test.log.error"), any(EventTime.class), eq(ImmutableMap.of("message", "message")));

        verify(sentry, never()).sendEvent(any(EventBuilder.class));
    }

    @Test
    public void hideAndMaskAttributes()
            throws Exception
    {
        Logger underlying = mock(Logger.class);
        SentryClient sentry = mock(SentryClient.class);
        Fluency fluency = mock(Fluency.class);
        Log.setup(false, null, null, true, "localhost", 24224, clazz -> underlying, () -> sentry, (s, i) -> fluency);
        Map<String, Object> defaults = ImmutableMap.of("mykey", "myvalue", "myname", "tagomoris");
        Log.setDefaultAttributes(defaults);
        Log.setAttributeKeysHidden(ImmutableList.of("k1", "k2"));
        Log.setAttributeKeysMasked(ImmutableList.of("k"));
        Log.setMaskedValueLength(4);
        Log log = new Log(LogTest.class);
        Map<String, Object> attrs = ImmutableMap.of("k", "vvvvvvvvvvvvvv", "k1", "v1", "k2", "v2");
        log.error("message", attrs);

        Map<String, Object> expected = ImmutableMap.<String, Object>builder()
                .put("message", "message")
                .put("mykey", "myvalue")
                .put("myname", "tagomoris")
                .put("k", "vvvv")
                .build();

        verify(underlying).error("message");
        verify(fluency).emit(eq("bigdam.log.error"), any(EventTime.class), eq(expected));

        verify(sentry, never()).sendEvent(any(EventBuilder.class));
    }
}

