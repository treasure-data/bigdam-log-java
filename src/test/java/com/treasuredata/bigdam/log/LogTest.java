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

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.spi.LoggingEvent;
import ch.qos.logback.core.Appender;

import org.junit.After;
import org.junit.Test;

import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.notNullValue;
import static org.junit.Assert.*;
import static org.junit.Assume.*;
import static org.mockito.Mockito.*;

import org.mockito.ArgumentCaptor;

public class LogTest
{
    @After
    public void teardown()
    {
        Log.reset();
    }

    @Test
    public void getLevel()
    {
        assertThat(Log.getLevel("error"), is(Level.ERROR));
        assertThat(Log.getLevel("warn"), is(Level.WARN));
        assertThat(Log.getLevel("info"), is(Level.INFO));
        assertThat(Log.getLevel("debug"), is(Level.DEBUG));
        assertThat(Log.getLevel("trace"), is(Level.TRACE));
    }

    @Test
    public void getRemoteLevel()
    {
        /*
        private static final int REMOTE_LEVEL_THRESHOLD_NEVER = 5;
        private static final int REMOTE_LEVEL_THRESHOLD_ERROR = 4;
        private static final int REMOTE_LEVEL_THRESHOLD_WARN = 3;
        private static final int REMOTE_LEVEL_THRESHOLD_INFO = 2;
        private static final int REMOTE_LEVEL_THRESHOLD_DEBUG = 1;
        private static final int REMOTE_LEVEL_THRESHOLD_TRACE = 0;
        */
        assertThat(Log.getRemoteLevel("error"), is(4));
        assertThat(Log.getRemoteLevel("warn"), is(3));
        assertThat(Log.getRemoteLevel("info"), is(2));
        assertThat(Log.getRemoteLevel("debug"), is(1));
        assertThat(Log.getRemoteLevel("trace"), is(0));
    }

    @Test
    public void isEnabled()
    {
        int configured1 = Log.getRemoteLevel("info");
        assertThat(Log.isEnabled(configured1, Log.getRemoteLevel("error")), is(true));
        assertThat(Log.isEnabled(configured1, Log.getRemoteLevel("warn")), is(true));
        assertThat(Log.isEnabled(configured1, Log.getRemoteLevel("info")), is(true));
        assertThat(Log.isEnabled(configured1, Log.getRemoteLevel("debug")), is(false));
        assertThat(Log.isEnabled(configured1, Log.getRemoteLevel("trace")), is(false));

        int configured2 = Log.getRemoteLevel("trace");
        assertThat(Log.isEnabled(configured2, Log.getRemoteLevel("error")), is(true));
        assertThat(Log.isEnabled(configured2, Log.getRemoteLevel("warn")), is(true));
        assertThat(Log.isEnabled(configured2, Log.getRemoteLevel("info")), is(true));
        assertThat(Log.isEnabled(configured2, Log.getRemoteLevel("debug")), is(true));
        assertThat(Log.isEnabled(configured2, Log.getRemoteLevel("trace")), is(true));

        int configured3 = Log.getRemoteLevel("error");
        assertThat(Log.isEnabled(configured3, Log.getRemoteLevel("error")), is(true));
        assertThat(Log.isEnabled(configured3, Log.getRemoteLevel("warn")), is(false));
        assertThat(Log.isEnabled(configured3, Log.getRemoteLevel("info")), is(false));
        assertThat(Log.isEnabled(configured3, Log.getRemoteLevel("debug")), is(false));
        assertThat(Log.isEnabled(configured3, Log.getRemoteLevel("trace")), is(false));

        int configured4 = 5; // NEVER
        assertThat(Log.isEnabled(configured4, Log.getRemoteLevel("error")), is(false));
        assertThat(Log.isEnabled(configured4, Log.getRemoteLevel("warn")), is(false));
        assertThat(Log.isEnabled(configured4, Log.getRemoteLevel("info")), is(false));
        assertThat(Log.isEnabled(configured4, Log.getRemoteLevel("debug")), is(false));
        assertThat(Log.isEnabled(configured4, Log.getRemoteLevel("trace")), is(false));
    }

    @Test
    public void initLoggerInDefault()
            throws Exception
    {
        Logger underlying = mock(Logger.class);
        SentryClient sentry = mock(SentryClient.class);
        Fluency fluency = mock(Fluency.class);
        Log.setup(false, null, null, null, false, null, null, 0, clazz -> underlying, (s) -> sentry, (s, i) -> fluency);
        Log log = new Log(LogTest.class);

        log.error("message");
        verify(underlying).error("message");
        verify(sentry, never()).sendEvent(any(EventBuilder.class));
        verify(fluency, never()).emit(any(), any(), any());
    }

    // TODO: data driven test for log levels of set / actual

    @SuppressWarnings({"unchecked", "rawtypes"})
    @Test
    public void setLogLevelWarnAndPutWarn()
            throws Exception
    {
        SentryClient sentry = mock(SentryClient.class);
        Fluency fluency = mock(Fluency.class);
        Log.setup(false, null, null, null, false, null, null, 0);
        Log.setLogLevel("warn");
        Log log = new Log(LogTest.class);
        ch.qos.logback.classic.Logger underlying = (ch.qos.logback.classic.Logger) log.getUnderlying();

        Appender mockAppender = mock(Appender.class);
        underlying.addAppender(mockAppender);

        log.warn("yay", ImmutableMap.of("k", "v"));

        ArgumentCaptor<LoggingEvent> captorLoggingEvent = ArgumentCaptor.forClass(LoggingEvent.class);

        verify(mockAppender).doAppend(captorLoggingEvent.capture());
        LoggingEvent loggingEvent = captorLoggingEvent.getValue();
        assertThat(loggingEvent.getLevel(), is(Level.WARN));
        assertThat(loggingEvent.getFormattedMessage(), is("yay {k=v}"));
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    @Test
    public void setLogLevelWarnAndPutInfo()
            throws Exception
    {
        SentryClient sentry = mock(SentryClient.class);
        Fluency fluency = mock(Fluency.class);
        Log.setup(false, null, null, null, false, null, null, 0);
        Log.setLogLevel("warn");
        Log log = new Log(LogTest.class);
        ch.qos.logback.classic.Logger underlying = (ch.qos.logback.classic.Logger) log.getUnderlying();

        Appender mockAppender = mock(Appender.class);
        underlying.addAppender(mockAppender);

        log.info("yay", ImmutableMap.of("k", "v"));

        verify(mockAppender, never()).doAppend(any());
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    @Test
    public void setLogLevelDebugAndPutTrace()
            throws Exception
    {
        SentryClient sentry = mock(SentryClient.class);
        Fluency fluency = mock(Fluency.class);
        Log.setup(false, null, null, null, false, null, null, 0);
        Log.setLogLevel("DEBUG");
        Log log = new Log(LogTest.class);
        ch.qos.logback.classic.Logger underlying = (ch.qos.logback.classic.Logger) log.getUnderlying();

        Appender mockAppender = mock(Appender.class);
        underlying.addAppender(mockAppender);

        log.trace("yay", ImmutableMap.of("k", "v"));

        verify(mockAppender, never()).doAppend(any());
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    @Test
    public void setLogLevelTraceAndPutTrace()
            throws Exception
    {
        SentryClient sentry = mock(SentryClient.class);
        Fluency fluency = mock(Fluency.class);
        Log.setup(false, null, null, null, false, null, null, 0);
        Log.setLogLevel("TRACE");
        Log log = new Log(LogTest.class);
        ch.qos.logback.classic.Logger underlying = (ch.qos.logback.classic.Logger) log.getUnderlying();

        Appender mockAppender = mock(Appender.class);
        underlying.addAppender(mockAppender);

        log.trace("yay", ImmutableMap.of("k", "v"));

        ArgumentCaptor<LoggingEvent> captorLoggingEvent = ArgumentCaptor.forClass(LoggingEvent.class);

        verify(mockAppender).doAppend(captorLoggingEvent.capture());
        LoggingEvent loggingEvent = captorLoggingEvent.getValue();
        assertThat(loggingEvent.getLevel(), is(Level.TRACE));
        assertThat(loggingEvent.getFormattedMessage(), is("yay {k=v}"));
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    @Test
    public void setLogLevelWithPackageName()
            throws Exception
    {
        SentryClient sentry = mock(SentryClient.class);
        Fluency fluency = mock(Fluency.class);
        Log.setup(false, null, null, null, false, null, null, 0);
        Log.setLogLevel("TRACE");
        Log.setLogLevel("com.treasuredata.bigdam.log", "INFO");
        Log log = new Log(LogTest.class);
        ch.qos.logback.classic.Logger underlying = (ch.qos.logback.classic.Logger) log.getUnderlying();

        Appender mockAppender = mock(Appender.class);
        underlying.addAppender(mockAppender);

        log.trace("yay", ImmutableMap.of("k", "v"));

        verify(mockAppender, never()).doAppend(any());
    }

    private long anyFluentdTimeStamp()
    {
        // return any(EventTime.class);
        return any(Long.class);
    }

    @Test
    public void initLoggerWithSentry()
            throws Exception
    {
        Logger underlying = mock(Logger.class);
        SentryClient sentry = mock(SentryClient.class);
        Fluency fluency = mock(Fluency.class);
        Log.setup(true, "error", "https://public:private@host:443/1", Optional.empty(), true, "trace", "localhost", 24224, clazz -> underlying, (s) -> sentry, (s, i) -> fluency);
        Log log = new Log(LogTest.class);

        String message = "yaaaaaaaaaaay";
        Exception e = null;
        try {
            throw new RuntimeException(message);
        }
        catch (Exception ex) {
            e = ex;
            log.error("yay", ex, Attrs.of("key", "value", "nullKey", null));
        }

        assertThat(e, is(instanceOf(RuntimeException.class)));
        assertThat(e.getMessage(), is(message));
        verify(underlying).error(eq("yay {}"), eq(Attrs.of("key", "value", "nullKey", null)), eq(e));
        verify(sentry, times(1)).sendEvent(any(EventBuilder.class));
        verify(fluency, times(1)).emit(eq("bigdam.log.error"),
                anyFluentdTimeStamp(),
                eq(Attrs.of(
                        "message", "yay",
                        "stime", log.getLastTimestamp().getNano(),
                        "errorClass", "java.lang.RuntimeException",
                        "error", message,
                        "key", "value",
                        "nullKey", null)));
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    @Test
    public void setSentryAndFluentdLogLevel()
        throws Exception
    {
        Logger underlying = mock(Logger.class);
        SentryClient sentry = mock(SentryClient.class);
        Fluency fluency = mock(Fluency.class);
        Log.setup(true, "warn", "https://public:private@host:443/1", Optional.empty(), true, "info", "localhost", 24224, clazz -> underlying, (s) -> sentry, (s, i) -> fluency);
        Log log = new Log(LogTest.class);

        String message = "yaaaaaaaaaaay";
        Exception e = null;
        try {
            throw new RuntimeException(message);
        }
        catch (Exception ex) {
            e = ex;
            log.error("yay", ex, Attrs.of("key", "value", "nullKey", null));
        }

        assertThat(e, is(instanceOf(RuntimeException.class)));
        assertThat(e.getMessage(), is(message));
        verify(underlying).error(eq("yay {}"), eq(Attrs.of("key", "value", "nullKey", null)), eq(e));
        verify(sentry, times(1)).sendEvent(any(EventBuilder.class));
        verify(fluency, times(1)).emit(eq("bigdam.log.error"),
                anyFluentdTimeStamp(),
                eq(Attrs.of(
                        "message", "yay",
                        "stime", log.getLastTimestamp().getNano(),
                        "errorClass", "java.lang.RuntimeException",
                        "error", message,
                        "key", "value",
                        "nullKey", null)));
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    @Test
    public void setSentryAndFluentdLogLevelToBeIgnored()
        throws Exception
    {
        Logger underlying = mock(Logger.class);
        SentryClient sentry = mock(SentryClient.class);
        Fluency fluency = mock(Fluency.class);
        Log.setup(true, "error", "https://public:private@host:443/1", Optional.empty(), true, "error", "localhost", 24224, clazz -> underlying, (s) -> sentry, (s, i) -> fluency);
        Log log = new Log(LogTest.class);

        String message = "yaaaaaaaaaaay";
        Exception e = null;
        try {
            throw new RuntimeException(message);
        }
        catch (Exception ex) {
            e = ex;
            log.warn("yay", ex, Attrs.of("key", "value", "nullKey", null));
        }

        assertThat(e, is(instanceOf(RuntimeException.class)));
        assertThat(e.getMessage(), is(message));
        verify(underlying).warn(eq("yay {}"), eq(Attrs.of("key", "value", "nullKey", null)), eq(e));
        verify(sentry, never()).sendEvent(any(EventBuilder.class));
        verify(fluency, never()).emit(eq("bigdam.log.warn"),
                anyFluentdTimeStamp(),
                eq(Attrs.of(
                        "message", "yay",
                        "stime", log.getLastTimestamp().getNano(),
                        "errorClass", "java.lang.RuntimeException",
                        "error", message,
                        "key", "value",
                        "nullKey", null)));
    }

    private void throwExceptionForTest()
    {
        throw new RuntimeException(String.format("This is an exception thrown by unit tests: %d", System.nanoTime()));
    }

    @Test
    public void sendErrorToSentryActuallOnlyWhenConfigured()
            throws Exception
    {
        String dsn = System.getenv("BIGDAM_LOG_TEST_SENTRY_DSN");
        assumeThat(dsn, is(notNullValue()));
        assumeThat(dsn, is(not("")));
        // if BIGDAM_LOG_TEST_SENTRY_DSN is not set, code below doesn't run
        Log.setup(true, "error", dsn, Optional.empty(), false, null, null, 24224);
        Log log = new Log(LogTest.class);
        try {
            throwExceptionForTest();
            assertThat(true, is(false));
        }
        catch (Throwable e) {
            log.error("Events from unit test", e, Attrs.of("key", "value"));
            assertThat(true, is(true));
        }
        Log.close();
        assertThat(true, is(true));
    }

    @Test
    public void initLoggerWithFluency()
            throws Exception
    {
        Logger underlying = mock(Logger.class);
        SentryClient sentry = mock(SentryClient.class);
        Fluency fluency = mock(Fluency.class);
        Log.setup(false, null, null, null, true, "trace", "localhost", 24224, clazz -> underlying, (s) -> sentry, (s, i) -> fluency);
        Log log = new Log(LogTest.class);

        log.error("message 1");
        int nano1 = log.getLastTimestamp().getNano();
        log.warn("message 2");
        int nano2 = log.getLastTimestamp().getNano();
        log.info("message 3");
        int nano3 = log.getLastTimestamp().getNano();
        log.debug("message 4");
        int nano4 = log.getLastTimestamp().getNano();
        log.trace("message 5");
        int nano5 = log.getLastTimestamp().getNano();
        verify(underlying).error("message 1");
        verify(underlying).warn("message 2");
        verify(underlying).info("message 3");
        verify(underlying).debug("message 4");
        verify(underlying).trace("message 5");
        verify(fluency).emit(eq("bigdam.log.error"), anyFluentdTimeStamp(), eq(ImmutableMap.of("message", "message 1", "stime", nano1)));
        verify(fluency).emit(eq("bigdam.log.warn"), anyFluentdTimeStamp(), eq(ImmutableMap.of("message", "message 2", "stime", nano2)));
        verify(fluency).emit(eq("bigdam.log.info"), anyFluentdTimeStamp(), eq(ImmutableMap.of("message", "message 3", "stime", nano3)));
        verify(fluency).emit(eq("bigdam.log.debug"), anyFluentdTimeStamp(), eq(ImmutableMap.of("message", "message 4", "stime", nano4)));
        verify(fluency).emit(eq("bigdam.log.trace"), anyFluentdTimeStamp(), eq(ImmutableMap.of("message", "message 5", "stime", nano5)));

        verify(sentry, never()).sendEvent(any(EventBuilder.class));
    }

    @Test
    public void logWithAttributes()
            throws Exception
    {
        Logger underlying = mock(Logger.class);
        SentryClient sentry = mock(SentryClient.class);
        Fluency fluency = mock(Fluency.class);
        Log.setup(false, null, null, null, true, "trace", "localhost", 24224, clazz -> underlying, (s) -> sentry, (s, i) -> fluency);
        Log log = new Log(LogTest.class);

        Map<String, Object> attrs = ImmutableMap.of("k", "vvvvvvvvvvvvvv", "k1", "v1", "k2", "v2");
        log.error("message", attrs);

        Map<String, Object> expected = ImmutableMap.<String, Object>builder()
                .put("message", "message")
                .put("stime", log.getLastTimestamp().getNano())
                .putAll(attrs)
                .build();

        verify(underlying).error("message {}", attrs);
        verify(fluency).emit(eq("bigdam.log.error"), anyFluentdTimeStamp(), eq(expected));
        verify(sentry, never()).sendEvent(any(EventBuilder.class));
    }

    @Test
    public void defaultAttributes()
            throws Exception
    {
        Logger underlying = mock(Logger.class);
        SentryClient sentry = mock(SentryClient.class);
        Fluency fluency = mock(Fluency.class);
        Log.setup(false, null, null, null, true, "trace", "localhost", 24224, clazz -> underlying, (s) -> sentry, (s, i) -> fluency);
        Log.setDefaultAttributes(ImmutableMap.of("mykey", "myvalue", "myname", "tagomoris"));
        Log log = new Log(LogTest.class);

        log.error("message");

        Map<String, Object> expected = ImmutableMap.<String, Object>builder()
                .put("message", "message")
                .put("stime", log.getLastTimestamp().getNano())
                .put("mykey", "myvalue")
                .put("myname", "tagomoris")
                .build();

        verify(underlying).error("message");
        verify(fluency).emit(eq("bigdam.log.error"), anyFluentdTimeStamp(), eq(expected));
        verify(sentry, never()).sendEvent(any(EventBuilder.class));
    }

    @Test
    public void attributes()
            throws Exception
    {
        Logger underlying = mock(Logger.class);
        SentryClient sentry = mock(SentryClient.class);
        Fluency fluency = mock(Fluency.class);
        Log.setup(false, null, null, null, true, "trace", "localhost", 24224, clazz -> underlying, (s) -> sentry, (s, i) -> fluency);
        Map<String, Object> defaults = ImmutableMap.of("mykey", "myvalue", "myname", "tagomoris");
        Log.setDefaultAttributes(defaults);
        Log log = new Log(LogTest.class);
        Map<String, Object> attrs = ImmutableMap.of("k", "vvvvvvvvvvvvvv", "k1", "v1", "k2", "v2");
        log.error("message", attrs);

        Map<String, Object> expected = ImmutableMap.<String, Object>builder()
                .put("message", "message")
                .put("stime", log.getLastTimestamp().getNano())
                .putAll(attrs)
                .putAll(defaults)
                .build();

        verify(underlying).error("message {}", attrs);
        verify(fluency).emit(eq("bigdam.log.error"), anyFluentdTimeStamp(), eq(expected));
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
        Log.setup(false, null, null, null, true, "trace", "localhost", 24224, clazz -> underlying, (s) -> sentry, (s, i) -> fluency);
        Map<String, Object> defaults = ImmutableMap.of("mykey", "myvalue", "myname", "tagomoris");
        Log.setDefaultAttributes(defaults);
        Log log = new Log(LogTest.class);

        Instant t = Instant.ofEpochSecond(1505367350L); // 2017-09-14 05:35:50 UTC
        JustPojo p1 = new JustPojo("p1", true, 1);
        JustPojo p2 = new JustPojo("p2", false, null);

        Map<String, Object> attrs = new HashMap<>();
        attrs.put("t", t);
        attrs.put("p1", p1);
        attrs.put("p2", p2);
        attrs.put("n", null);
        log.error("message", attrs);

        Map<String, Object> expectedAttrs = new HashMap<>();
        expectedAttrs.put("t", t.toString());
        expectedAttrs.put("p1", "JustPojo{p1,true,1}");
        expectedAttrs.put("p2", "JustPojo{p2,false,null}");
        expectedAttrs.put("n", null);
        Map<String, Object> expected = new HashMap<>();
        expected.put("message", "message");
        expected.put("stime", log.getLastTimestamp().getNano());
        expected.putAll(expectedAttrs);
        expected.putAll(defaults);

        verify(underlying).error(eq("message {}"), eq(expectedAttrs));
        verify(fluency).emit(eq("bigdam.log.error"), anyFluentdTimeStamp(), eq(expected));
        verify(sentry, never()).sendEvent(any(EventBuilder.class));
    }

    @Test
    public void tagPrefixModified()
            throws Exception
    {
        Logger underlying = mock(Logger.class);
        SentryClient sentry = mock(SentryClient.class);
        Fluency fluency = mock(Fluency.class);
        Log.setup(false, null, null, null, true, "trace", "localhost", 24224, clazz -> underlying, (s) -> sentry, (s, i) -> fluency);
        Log.setTagPrefix("bigdam.test.log.");
        Log log = new Log(LogTest.class);

        log.error("message", ImmutableMap.of());
        verify(underlying).error(eq("message {}"), eq(ImmutableMap.of()));
        verify(fluency).emit(eq("bigdam.test.log.error"), anyFluentdTimeStamp(), eq(ImmutableMap.of("message", "message", "stime", log.getLastTimestamp().getNano())));

        verify(sentry, never()).sendEvent(any(EventBuilder.class));
    }

    @Test
    public void hideAndMaskAttributes()
            throws Exception
    {
        Logger underlying = mock(Logger.class);
        SentryClient sentry = mock(SentryClient.class);
        Fluency fluency = mock(Fluency.class);
        Log.setup(false, null, null, null, true, "trace", "localhost", 24224, clazz -> underlying, (s) -> sentry, (s, i) -> fluency);
        Map<String, Object> defaults = ImmutableMap.of("mykey", "myvalue", "myname", "tagomoris");
        Log.setDefaultAttributes(defaults);
        Log.setAttributeKeysHidden(ImmutableList.of("k1", "k2"));
        Log.setAttributeKeysMasked(ImmutableList.of("k"));
        Log.setMaskedValueLength(4);
        Log log = new Log(LogTest.class);
        Map<String, Object> attrs = ImmutableMap.of("k", "vvvvvvvvvvvvvv", "k1", "v1", "k2", "v2");
        log.error("message", attrs);

        Map<String, Object> expectedAttrs = ImmutableMap.of("k", "vvvv");
        Map<String, Object> expected = ImmutableMap.<String, Object>builder()
                .put("message", "message")
                .put("stime", log.getLastTimestamp().getNano())
                .put("mykey", "myvalue")
                .put("myname", "tagomoris")
                .putAll(expectedAttrs)
                .build();

        verify(underlying).error(eq("message {}"), eq(expectedAttrs));
        verify(fluency).emit(eq("bigdam.log.error"), anyFluentdTimeStamp(), eq(expected));
        verify(sentry, never()).sendEvent(any(EventBuilder.class));
    }
}

