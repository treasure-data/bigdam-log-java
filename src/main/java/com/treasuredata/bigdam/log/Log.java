package com.treasuredata.bigdam.log;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

import java.io.IOException;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.time.Instant;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.BiFunction;
import java.util.function.Function;

import org.komamitsu.fluency.EventTime;
import org.komamitsu.fluency.Fluency;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

// ch.qos.logback.classic.Logger
import ch.qos.logback.classic.Level;

import io.sentry.Sentry;
import io.sentry.SentryClient;
import io.sentry.event.Event;
import io.sentry.event.EventBuilder;
import io.sentry.event.interfaces.ExceptionInterface;

public class Log
{
    private static final String DEFAULT_ERROR_TAG = "bigdam.log.error";
    private static final String DEFAULT_WARN_TAG = "bigdam.log.warn";
    private static final String DEFAULT_INFO_TAG = "bigdam.log.info";
    private static final String DEFAULT_DEBUG_TAG = "bigdam.log.debug";
    private static final String DEFAULT_TRACE_TAG = "bigdam.log.trace";

    private static final int LOG_SERVICE_LEVEL_THRESHOLD_NEVER = 5;
    private static final int LOG_SERVICE_LEVEL_THRESHOLD_ERROR = 4;
    private static final int LOG_SERVICE_LEVEL_THRESHOLD_WARN = 3;
    private static final int LOG_SERVICE_LEVEL_THRESHOLD_INFO = 2;
    private static final int LOG_SERVICE_LEVEL_THRESHOLD_DEBUG = 1;
    private static final int LOG_SERVICE_LEVEL_THRESHOLD_TRACE = 0;

    private static final String SUBSECOND_TIME_FIELD = "stime";

    private static Function<Class<?>, Logger> loggerGetter = Log::defaultLoggerGetter;

    private static Map<String, ? extends Object> defaultAttributes = ImmutableMap.of();
    private static List<String> attributeKeysHidden = ImmutableList.of();
    private static List<String> attributeKeysMasked = ImmutableList.of();
    private static int maskedValueLength = 8;

    private static SentryClient sentry = null;
    private static int sentryLevel = LOG_SERVICE_LEVEL_THRESHOLD_NEVER;

    private static Fluency fluency = null;
    private static int fluentdLevel = LOG_SERVICE_LEVEL_THRESHOLD_NEVER;

    private static String errorTag = DEFAULT_ERROR_TAG;
    private static String warnTag = DEFAULT_WARN_TAG;
    private static String infoTag = DEFAULT_INFO_TAG;
    private static String debugTag = DEFAULT_DEBUG_TAG;
    private static String traceTag = DEFAULT_TRACE_TAG;

    private final Class<?> clazz;
    private Logger logger;

    private Instant lastTimestamp;

    public static Level getLevel(final String str)
    {
        String upcase = str.toUpperCase();
        if (upcase.equals("ERROR")) {
            return Level.ERROR;
        }
        if (upcase.equals("WARN")) {
            return Level.WARN;
        }
        if (upcase.equals("INFO")) {
            return Level.INFO;
        }
        if (upcase.equals("DEBUG")) {
            return Level.DEBUG;
        }
        if (upcase.equals("TRACE")) {
            return Level.TRACE;
        }
        return null;
    }

    public static int getRemoteLevel(final String threshold)
    {
        int level = LOG_SERVICE_LEVEL_THRESHOLD_NEVER;
        if (threshold.equals("error")) {
            level = LOG_SERVICE_LEVEL_THRESHOLD_ERROR;
        }
        else if (threshold.equals("warn")) {
            level = LOG_SERVICE_LEVEL_THRESHOLD_WARN;
        }
        else if (threshold.equals("info")) {
            level = LOG_SERVICE_LEVEL_THRESHOLD_INFO;
        }
        else if (threshold.equals("debug")) {
            level = LOG_SERVICE_LEVEL_THRESHOLD_DEBUG;
        }
        else if (threshold.equals("trace")) {
            level = LOG_SERVICE_LEVEL_THRESHOLD_TRACE;
        }
        else {
            throw new IllegalArgumentException("Invalid log level for Fluentd/Sentry log level:" + threshold);
        }
        return level;
    }

    public static boolean isEnabled(final int configured, final int checked)
    {
        return checked >= configured;
    }

    public static Logger defaultLoggerGetter(final Class<?> clazz)
    {
        return LoggerFactory.getLogger(clazz);
    }

    public static SentryClient defaultSentryClientGetter(final String dsn)
    {
        return Sentry.init(dsn);
    }

    public static Fluency defaultFluencyGetter(final String host, final int port)
    {
        try {
            return Fluency.defaultFluency(host, port);
        }
        catch (IOException e) {
            throw new RuntimeException("Failed to initialize Fluentd client", e);
        }
    }

    // only for testing
    public static void reset()
    {
        sentry = null;
        fluency = null;
        errorTag = DEFAULT_ERROR_TAG;
        warnTag = DEFAULT_WARN_TAG;
        infoTag = DEFAULT_INFO_TAG;
        debugTag = DEFAULT_DEBUG_TAG;
        traceTag = DEFAULT_TRACE_TAG;
        defaultAttributes = ImmutableMap.of();
        attributeKeysHidden = ImmutableList.of();
        attributeKeysMasked = ImmutableList.of();
        maskedValueLength = 8;
    }

    public static void setup(
            final boolean enableSentry,
            final String sentryLevelThreshold,
            final String dsn,
            final Optional<Float> sampleRate,
            final boolean enableFluentd,
            final String fluentdLevelThreshold,
            final String host,
            final int port
    )
    {
        setup(
                enableSentry, sentryLevelThreshold, dsn, sampleRate,
                enableFluentd, fluentdLevelThreshold, host, port,
                Log::defaultLoggerGetter,
                Log::defaultSentryClientGetter,
                Log::defaultFluencyGetter
        );
    }

    public static void setup(
            final boolean enableSentry,
            final String sentryLevelThreshold,
            final String dsn,
            final Optional<Float> sampleRate,
            final boolean enableFluentd,
            final String fluentdLevelThreshold,
            final String host,
            final int port,
            final Function<Class<?>, Logger> loggerGetter,
            final Function<String, SentryClient> sentryGetter,
            final BiFunction<String, Integer, Fluency> fluencyGetter
    )
    {
        setupLogger(loggerGetter);
        if (enableSentry) {
            setupSentry(sentryGetter, sentryLevelThreshold, dsn, sampleRate);
        }
        if (enableFluentd) {
            setupFluentd(fluencyGetter, fluentdLevelThreshold, host, port);
        }
    }

    public static void setupLogger()
    {
        setupLogger(Log::defaultLoggerGetter);
    }

    public static void setupLogger(final Function<Class<?>, Logger> loggerGetterArg)
    {
        if (loggerGetterArg != null) {
            loggerGetter = loggerGetterArg;
        }
    }

    public static void setupSentry(final String sentryLevelThreshold, final String dsn, final Optional<Float> sampleRate)
    {
        setupSentry(Log::defaultSentryClientGetter, sentryLevelThreshold, dsn, sampleRate);
    }

    public static void setupSentry(final Function<String, SentryClient> sentryGetterArg, final String sentryLevelThreshold, final String dsn, final Optional<Float> sampleRate)
    {
        if (sampleRate.isPresent()) {
            setupSentry(sentryGetterArg, sentryLevelThreshold, String.format("%s?sample.rate=%f", dsn, sampleRate.get()));
        }
        else {
            setupSentry(sentryGetterArg, sentryLevelThreshold, dsn);
        }
    }

    public static void setupSentry(final Function<String, SentryClient> sentryGetterArg, final String sentryLevelThreshold, final String dsn)
    {
        if (sentryLevelThreshold == null) {
            throw new IllegalArgumentException("Sentry log level is not specified.");
        }
        sentryLevel = getRemoteLevel(sentryLevelThreshold);
        sentry = sentryGetterArg.apply(dsn);
    }

    public static void setupFluentd(final String host, final int port)
    {
        setupFluentd(Log::defaultFluencyGetter, "info", host, port);
    }

    public static void setupFluentd(final String level, final String host, final int port)
    {
        setupFluentd(Log::defaultFluencyGetter, level, host, port);
    }

    public static void setupFluentd(
            final BiFunction<String, Integer, Fluency> fluencyGetterArg,
            final String fluentdLevelThreshold,
            final String host,
            final Integer port
    )
    {
        fluentdLevel = getRemoteLevel(fluentdLevelThreshold);
        fluency = fluencyGetterArg.apply(host, port);
    }

    public static void setLogLevel(final String newLevel)
    {
        Level newer = getLevel(newLevel);
        if (newer == null) {
            throw new IllegalArgumentException("BUG: Unknown LogLevel:" + newLevel);
        }
        Logger root = LoggerFactory.getLogger(Logger.ROOT_LOGGER_NAME);
        if (root instanceof ch.qos.logback.classic.Logger) {
            ((ch.qos.logback.classic.Logger) root).setLevel(newer);
        }
    }

    public static void setLogLevel(final String packageName, final String newLevel)
    {
        Level newer = getLevel(newLevel);
        if (newer == null) {
            throw new IllegalArgumentException("BUG: Unknown LogLevel:" + newLevel);
        }
        Logger root = LoggerFactory.getLogger(packageName);
        if (root instanceof ch.qos.logback.classic.Logger) {
            ((ch.qos.logback.classic.Logger) root).setLevel(newer);
        }
    }

    public static void setDefaultAttributes(final Map<String, ? extends Object> defaultAttributesArg)
    {
        // all default attributes must be serializable as msgpack
        // TODO: add check for serializability
        defaultAttributes = ImmutableMap.copyOf(defaultAttributesArg);
    }

    public static void setTagPrefix(final String prefix)
    {
        errorTag = prefix + "error";
        warnTag = prefix + "warn";
        infoTag = prefix + "info";
        debugTag = prefix + "debug";
        traceTag = prefix + "trace";
    }

    public static void setAttributeKeysHidden(final List<String> keys)
    {
        attributeKeysHidden = keys;
    }

    public static void setAttributeKeysMasked(final List<String> keys)
    {
        attributeKeysMasked = keys;
    }

    public static void setMaskedValueLength(final int length)
    {
        maskedValueLength = length;
    }

    public static void close()
    {
        if (fluency != null) {
            try {
                fluency.close();
            }
            catch (IOException e) {
                // ignore it - this process is going down.
            }
        }
        if (sentry != null) {
            sentry.closeConnection();
        }
    }

    public Log(Class<?> clazz)
    {
        this.clazz = clazz;
        this.logger = loggerGetter.apply(clazz);
    }

    // only for testing
    Logger getUnderlying()
    {
        return logger;
    }

    protected void sendException(final Throwable e, Map<String, ? extends Object> attrs)
    {
        if (sentry == null) {
            return;
        }
        if (attrs == null) {
            attrs = ImmutableMap.of();
        }

        EventBuilder builder = new EventBuilder()
                .withMessage(e.getMessage())
                .withLevel(Event.Level.ERROR)
                .withLogger(clazz.getName());
        for (Map.Entry<String, ? extends Object> pair : defaultAttributes.entrySet()) {
            builder.withTag(pair.getKey(), pair.getValue().toString());
        }
        for (Map.Entry<String, ? extends Object> pair : attrs.entrySet()) {
            String key = pair.getKey();
            Object value = pair.getValue();
            if (attributeKeysHidden.contains(key)) {
                // ignore
            }
            else if (attributeKeysMasked.contains(key)) {
                builder.withExtra(key, value == null ? null : value.toString().substring(0, maskedValueLength));
            }
            else {
                builder.withExtra(key, value == null ? null : value.toString());
            }
        }
        builder.withSentryInterface(new ExceptionInterface(e));
        sentry.sendEvent(builder);
    }

    private EventTime eventTime(final Instant now)
    {
        return EventTime.fromEpoch((int) now.getEpochSecond(), now.getNano());
    }

    private Map<String, Object> filterAttrs(final Map<String, ? extends Object> attrs)
    {
        if (attrs == null) {
            return ImmutableMap.of();
        }

        Map<String, Object> event = new HashMap<>();
        for (Map.Entry<String, ? extends Object> pair : attrs.entrySet()) {
            String key = pair.getKey();
            Object value = pair.getValue();
            if (attributeKeysHidden.contains(key)) {
                // ignore
            }
            else if (attributeKeysMasked.contains(key)) {
                if (value instanceof String) {
                    event.put(key, ((String) value).substring(0, maskedValueLength));
                }
                else {
                    event.put(key, value.toString().substring(0, maskedValueLength));
                }
            }
            else {
                if (value == null
                        || value instanceof String
                        || value instanceof Integer
                        || value instanceof Long
                        || value instanceof BigInteger
                        || value instanceof BigDecimal
                        || value instanceof Float
                        || value instanceof Double
                        || value instanceof Boolean
                        ) {
                    event.put(key, value);
                }
                else {
                    event.put(key, value.toString());
                }
            }
        }
        return event;
    }

    // use this method only in testing
    public Instant getLastTimestamp()
    {
        if (lastTimestamp == null) {
            return Instant.now();
        }
        return lastTimestamp;
    }

    private Map<String, Object> buildEvent(final Instant now, final String messageKey, final String message, final Throwable e, final Map<String, ? extends Object> attrs)
    {
        // lastTimestamp is only for testing
        lastTimestamp = now;
        // buildEvent doesn't modify "attrs", but construct another Map object.
        // Original attrs is used for many purposes (dump it on local log, send it to Fluentd and/or Sentry),
        // so we should not modify it.
        Map<String, Object> event = new HashMap<>();
        event.put(SUBSECOND_TIME_FIELD, now.getNano());
        if (messageKey != null) {
            event.put(messageKey, message);
        }
        if (e != null) {
            event.put("errorClass", e.getClass().getName());
            event.put("error", e.getMessage());
        }
        event.putAll(defaultAttributes);
        event.putAll(filterAttrs(attrs));
        return event;
    }

    protected void sendEvent(final String tag, final Map<String, ? extends Object> attrs)
    {
        sendEvent(tag, Instant.now(), null, null, null, attrs);
    }

    protected void sendEvent(final String tag, final Instant now, final Map<String, ? extends Object> attrs)
    {
        sendEvent(tag, now, null, null, null, attrs);
    }

    protected void sendEvent(final String tag, final String message, final Map<String, ? extends Object> attrs)
    {
        sendEvent(tag, Instant.now(), "message", message, null, (attrs == null ? ImmutableMap.of() : attrs));
    }

    protected void sendEvent(final String tag, final String message, final Throwable e, final Map<String, ? extends Object> attrs)
    {
        sendEvent(tag, Instant.now(), "message", message, e, (attrs == null ? ImmutableMap.of() : attrs));
    }

    protected void sendEvent(final String tag, final Instant now, final String messageKey, final String message, final Throwable e, final Map<String, ? extends Object> attrs)
    {
        if (fluency == null) {
            return;
        }
        try {
            // Fluentd 0.12 doesn't support EventTime, so use a normal integer here
            fluency.emit(tag, now.getEpochSecond(), buildEvent(now, messageKey, message, e, attrs));
        }
        catch (IOException ex) {
            logger.error("Failed to emit event to Fluentd", ex);
        }
    }

    public void error(final String message)
    {
        logger.error(message);
        if (isEnabled(fluentdLevel, LOG_SERVICE_LEVEL_THRESHOLD_ERROR)) {
            sendEvent(errorTag, message, null);
        }
    }

    public void error(final String message, final Map<String, ? extends Object> attrs)
    {
        logger.error(message + " {}", filterAttrs(attrs));
        if (isEnabled(fluentdLevel, LOG_SERVICE_LEVEL_THRESHOLD_ERROR)) {
            sendEvent(errorTag, message, attrs);
        }
    }

    public void error(final String message, final Throwable e)
    {
        error(message, e, null);
    }

    public void error(final String message, final Throwable e, final Map<String, ? extends Object> attrs)
    {
        if (attrs == null) {
            logger.error(message, e);
        }
        else {
            logger.error(message + " {}", filterAttrs(attrs), e);
        }
        if (isEnabled(fluentdLevel, LOG_SERVICE_LEVEL_THRESHOLD_ERROR)) {
            sendEvent(errorTag, message, e, attrs);
        }
        if (isEnabled(sentryLevel, LOG_SERVICE_LEVEL_THRESHOLD_ERROR)) {
            sendException(e, attrs);
        }
    }

    public void warn(final String message)
    {
        logger.warn(message);
        if (isEnabled(fluentdLevel, LOG_SERVICE_LEVEL_THRESHOLD_WARN)) {
            sendEvent(warnTag, message, null);
        }
    }

    public void warn(final String message, final Map<String, ? extends Object> attrs)
    {
        logger.warn(message + " {}", filterAttrs(attrs));
        if (isEnabled(fluentdLevel, LOG_SERVICE_LEVEL_THRESHOLD_WARN)) {
            sendEvent(warnTag, message, attrs);
        }
    }

    public void warn(final String message, final Throwable e)
    {
        warn(message, e, null);
    }

    public void warn(final String message, final Throwable e, final Map<String, ? extends Object> attrs)
    {
        if (attrs == null) {
            logger.warn(message, e);
        }
        else {
            logger.warn(message + " {}", filterAttrs(attrs), e);
        }
        if (isEnabled(fluentdLevel, LOG_SERVICE_LEVEL_THRESHOLD_WARN)) {
            sendEvent(warnTag, message, e, attrs);
        }
        if (isEnabled(sentryLevel, LOG_SERVICE_LEVEL_THRESHOLD_WARN)) {
            sendException(e, attrs);
        }
    }

    public void info(final String message)
    {
        logger.info(message);
        if (isEnabled(fluentdLevel, LOG_SERVICE_LEVEL_THRESHOLD_INFO)) {
            sendEvent(infoTag, message, null);
        }
    }

    public void info(final String message, final Map<String, ? extends Object> attrs)
    {
        logger.info(message + " {}", filterAttrs(attrs));
        if (isEnabled(fluentdLevel, LOG_SERVICE_LEVEL_THRESHOLD_INFO)) {
            sendEvent(infoTag, message, attrs);
        }
    }

    public void info(final String message, final Throwable e)
    {
        info(message, e, null);
    }

    public void info(final String message, final Throwable e, final Map<String, ? extends Object> attrs)
    {
        if (attrs == null) {
            logger.info(message, e);
        }
        else {
            logger.info(message + " {}", filterAttrs(attrs), e);
        }
        if (isEnabled(fluentdLevel, LOG_SERVICE_LEVEL_THRESHOLD_INFO)) {
            sendEvent(infoTag, message, e, attrs);
        }
        if (isEnabled(sentryLevel, LOG_SERVICE_LEVEL_THRESHOLD_INFO)) {
            sendException(e, attrs);
        }
    }

    public void debug(final String message)
    {
        logger.debug(message);
        if (isEnabled(fluentdLevel, LOG_SERVICE_LEVEL_THRESHOLD_DEBUG)) {
            sendEvent(debugTag, message, null);
        }
    }

    public void debug(final String message, final Map<String, ? extends Object> attrs)
    {
        logger.debug(message + " {}", filterAttrs(attrs));
        if (isEnabled(fluentdLevel, LOG_SERVICE_LEVEL_THRESHOLD_DEBUG)) {
            sendEvent(debugTag, message, attrs);
        }
    }

    public void debug(final String message, final Throwable e)
    {
        debug(message, e, null);
    }

    public void debug(final String message, final Throwable e, final Map<String, ? extends Object> attrs)
    {
        if (attrs == null) {
            logger.debug(message, e);
        }
        else {
            logger.debug(message + " {}", filterAttrs(attrs), e);
        }
        if (isEnabled(fluentdLevel, LOG_SERVICE_LEVEL_THRESHOLD_DEBUG)) {
            sendEvent(debugTag, message, e, attrs);
        }
        if (isEnabled(sentryLevel, LOG_SERVICE_LEVEL_THRESHOLD_DEBUG)) {
            sendException(e, attrs);
        }
    }

    public void trace(final String message)
    {
        logger.trace(message);
        if (isEnabled(fluentdLevel, LOG_SERVICE_LEVEL_THRESHOLD_TRACE)) {
            sendEvent(traceTag, message, null);
        }
    }

    public void trace(final String message, final Map<String, ? extends Object> attrs)
    {
        logger.trace(message + " {}", filterAttrs(attrs));
        if (isEnabled(fluentdLevel, LOG_SERVICE_LEVEL_THRESHOLD_TRACE)) {
            sendEvent(traceTag, message, attrs);
        }
    }

    public void trace(final String message, final Throwable e)
    {
        trace(message, e, null);
    }

    public void trace(final String message, final Throwable e, final Map<String, ? extends Object> attrs)
    {
        if (attrs == null) {
            logger.trace(message, e);
        }
        else {
            logger.trace(message + " {}", filterAttrs(attrs), e);
        }
        if (isEnabled(fluentdLevel, LOG_SERVICE_LEVEL_THRESHOLD_TRACE)) {
            sendEvent(traceTag, message, e, attrs);
        }
        if (isEnabled(sentryLevel, LOG_SERVICE_LEVEL_THRESHOLD_TRACE)) {
            sendException(e, attrs);
        }
    }
}
