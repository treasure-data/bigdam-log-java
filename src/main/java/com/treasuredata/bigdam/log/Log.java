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
import java.util.function.Supplier;

import org.komamitsu.fluency.EventTime;
import org.komamitsu.fluency.Fluency;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.sentry.Sentry;
import io.sentry.SentryClient;
import io.sentry.SentryClientFactory;
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

    private static final int SENTRY_LEVEL_THRESHOLD_NEVER = 5;
    private static final int SENTRY_LEVEL_THRESHOLD_ERROR = 4;
    private static final int SENTRY_LEVEL_THRESHOLD_WARN = 3;
    private static final int SENTRY_LEVEL_THRESHOLD_INFO = 2;
    private static final int SENTRY_LEVEL_THRESHOLD_DEBUG = 1;
    private static final int SENTRY_LEVEL_THRESHOLD_TRACE = 0;

    private static Function<Class<?>, Logger> loggerGetter = Log::defaultLoggerGetter;

    private static Map<String, ? extends Object> defaultAttributes = ImmutableMap.of();
    private static List<String> attributeKeysHidden = ImmutableList.of();
    private static List<String> attributeKeysMasked = ImmutableList.of();
    private static int maskedValueLength = 8;

    private static SentryClient sentry = null;
    private static int sentryLevel = SENTRY_LEVEL_THRESHOLD_NEVER;

    private static Fluency fluency = null;

    private static String errorTag = DEFAULT_ERROR_TAG;
    private static String warnTag = DEFAULT_WARN_TAG;
    private static String infoTag = DEFAULT_INFO_TAG;
    private static String debugTag = DEFAULT_DEBUG_TAG;
    private static String traceTag = DEFAULT_TRACE_TAG;

    private final Class<?> clazz;
    private Logger logger;

    public static Logger defaultLoggerGetter(final Class<?> clazz)
    {
        return LoggerFactory.getLogger(clazz);
    }

    public static SentryClient defaultSentryClientGetter()
    {
        return SentryClientFactory.sentryClient();
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
            final String host,
            final int port
    )
    {
        setup(
                enableSentry, sentryLevelThreshold, dsn, sampleRate,
                enableFluentd, host, port,
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
            final String host,
            final int port,
            final Function<Class<?>, Logger> loggerGetter,
            final Supplier<SentryClient> sentryGetter,
            final BiFunction<String, Integer, Fluency> fluencyGetter
    )
    {
        setupLogger(loggerGetter);
        if (enableSentry) {
            setupSentry(sentryGetter, sentryLevelThreshold, dsn, sampleRate);
        }
        if (enableFluentd) {
            setupFluentd(fluencyGetter, host, port);
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

    public static void setupSentry(final Supplier<SentryClient> sentryGetterArg, final String sentryLevelThreshold, final String dsn, final Optional<Float> sampleRate)
    {
        if (sampleRate.isPresent()) {
            setupSentry(sentryGetterArg, sentryLevelThreshold, String.format("%s?sample.rate=%f", dsn, sampleRate.get()));
        }
        else {
            setupSentry(sentryGetterArg, sentryLevelThreshold, dsn);
        }
    }

    public static void setupSentry(final Supplier<SentryClient> sentryGetterArg, final String sentryLevelThreshold, final String dsn)
    {
        if (sentryLevelThreshold == null) {
            throw new IllegalArgumentException("Sentry log level is not specified.");
        }

        Sentry.init(dsn);

        // "+ 1" is to be bigger than threshold
        if (sentryLevelThreshold.equals("error")) {
            sentryLevel = SENTRY_LEVEL_THRESHOLD_ERROR + 1;
        }
        else if (sentryLevelThreshold.equals("warn")) {
            sentryLevel = SENTRY_LEVEL_THRESHOLD_WARN + 1;
        }
        else if (sentryLevelThreshold.equals("info")) {
            sentryLevel = SENTRY_LEVEL_THRESHOLD_INFO + 1;
        }
        else if (sentryLevelThreshold.equals("debug")) {
            sentryLevel = SENTRY_LEVEL_THRESHOLD_DEBUG + 1;
        }
        else if (sentryLevelThreshold.equals("trace")) {
            sentryLevel = SENTRY_LEVEL_THRESHOLD_TRACE + 1;
        }
        else {
            throw new IllegalArgumentException("Invalid log level for Sentry log level:" + sentryLevelThreshold);
        }
        sentry = sentryGetterArg.get();
    }

    public static void setupFluentd(final String host, final int port)
    {
        setupFluentd(Log::defaultFluencyGetter, host, port);
    }

    public static void setupFluentd(
            final BiFunction<String, Integer, Fluency> fluencyGetterArg,
            final String host,
            final Integer port
    )
    {
        fluency = fluencyGetterArg.apply(host, port);
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
    }

    public Log(Class<?> clazz)
    {
        this.clazz = clazz;
        this.logger = loggerGetter.apply(clazz);
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
            if (attributeKeysHidden.contains(key)) {
                // ignore
            }
            else if (attributeKeysMasked.contains(key)) {
                builder.withExtra(key, pair.getValue().toString().substring(0, maskedValueLength));
            }
            else {
                builder.withExtra(key, pair.getValue().toString());
            }
        }
        builder.withSentryInterface(new ExceptionInterface(e));
        sentry.sendEvent(builder);
    }

    private EventTime eventTime(final Instant now)
    {
        return EventTime.fromEpoch((int) now.getEpochSecond(), now.getNano());
    }

    private Map<String, Object> buildEvent(final String messageKey, final String message, final Throwable e, final Map<String, ? extends Object> attrs)
    {
        HashMap<String, Object> event = new HashMap<>();
        if (messageKey != null) {
            event.put(messageKey, message);
        }
        if (e != null) {
            event.put("errorClass", e.getClass().getName());
            event.put("error", e.getMessage());
        }
        event.putAll(defaultAttributes);
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
            fluency.emit(tag, eventTime(now), buildEvent(messageKey, message, e, attrs));
        }
        catch (IOException ex) {
            logger.error("Failed to emit event to Fluentd", ex);
        }
    }

    public void error(final String message)
    {
        logger.error(message);
        sendEvent(errorTag, message, null);
    }

    public void error(final String message, final Map<String, ? extends Object> attrs)
    {
        logger.error(message);
        sendEvent(errorTag, message, attrs);
    }

    public void error(final String message, final Throwable e)
    {
        error(message, e, null);
    }

    public void error(final String message, final Throwable e, final Map<String, ? extends Object> attrs)
    {
        logger.error(message, e);
        sendEvent(errorTag, message, e, attrs);
        if (sentryLevel > SENTRY_LEVEL_THRESHOLD_ERROR) {
            sendException(e, attrs);
        }
    }

    public void warn(final String message)
    {
        logger.warn(message);
        sendEvent(warnTag, message, null);
    }

    public void warn(final String message, final Map<String, ? extends Object> attrs)
    {
        logger.warn(message);
        sendEvent(warnTag, message, attrs);
    }

    public void warn(final String message, final Throwable e)
    {
        warn(message, e, null);
    }

    public void warn(final String message, final Throwable e, final Map<String, ? extends Object> attrs)
    {
        logger.warn(message, e);
        sendEvent(warnTag, message, e, attrs);
        if (sentryLevel > SENTRY_LEVEL_THRESHOLD_WARN) {
            sendException(e, attrs);
        }
    }

    public void info(final String message)
    {
        logger.info(message);
        sendEvent(infoTag, message, null);
    }

    public void info(final String message, final Map<String, ? extends Object> attrs)
    {
        logger.info(message);
        sendEvent(infoTag, message, attrs);
    }

    public void info(final String message, final Throwable e)
    {
        info(message, e, null);
    }

    public void info(final String message, final Throwable e, final Map<String, ? extends Object> attrs)
    {
        logger.info(message, e);
        sendEvent(infoTag, message, e, attrs);
        if (sentryLevel > SENTRY_LEVEL_THRESHOLD_INFO) {
            sendException(e, attrs);
        }
    }

    public void debug(final String message)
    {
        logger.debug(message);
        sendEvent(debugTag, message, null);
    }

    public void debug(final String message, final Map<String, ? extends Object> attrs)
    {
        logger.debug(message);
        sendEvent(debugTag, message, attrs);
    }

    public void debug(final String message, final Throwable e)
    {
        debug(message, e, null);
    }

    public void debug(final String message, final Throwable e, final Map<String, ? extends Object> attrs)
    {
        logger.debug(message, e);
        sendEvent(debugTag, message, e, attrs);
        if (sentryLevel > SENTRY_LEVEL_THRESHOLD_DEBUG) {
            sendException(e, attrs);
        }
    }

    public void trace(final String message)
    {
        logger.trace(message);
        sendEvent(traceTag, message, null);
    }

    public void trace(final String message, final Map<String, ? extends Object> attrs)
    {
        logger.trace(message);
        sendEvent(traceTag, message, attrs);
    }

    public void trace(final String message, final Throwable e)
    {
        trace(message, e, null);
    }

    public void trace(final String message, final Throwable e, final Map<String, ? extends Object> attrs)
    {
        logger.trace(message, e);
        sendEvent(traceTag, message, e, attrs);
        if (sentryLevel > SENTRY_LEVEL_THRESHOLD_TRACE) {
            sendException(e, attrs);
        }
    }
}
