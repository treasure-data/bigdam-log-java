package com.treasuredata.bigdam.log;

import com.google.common.collect.ImmutableMap;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Supplier;

public class MetricMonitor
{
    @FunctionalInterface
    public interface MetricProducer
    {
        public Map<String, Object> produce();
    }

    @FunctionalInterface
    public interface ComplexMetricProducer
    {
        public List<ComplexMetric> produce();
    }

    private final Log logger;

    private final String tagPrefixMetric;
    private final String tagPrefixRawMetric;
    private final String metricFieldName;

    private final List<MetricProducer> metricProducers;
    private final List<MetricProducer> rawMetricProducers;
    private final List<ComplexMetricProducer> metricProducersComplex;
    private final List<ComplexMetricProducer> rawMetricProducersComplex;

    private final int metricIntervalSeconds;

    private final AtomicBoolean running;
    private Thread monitorThread;
    private long sleepIntervalMilliSeconds;

    private static final long SLEEP_INTERVAL_MSEC = 800L;
    private static final long MONITOR_THREAD_STOP_TIMEOUT = 2000L; // sleep interval * 2 + alpha

    public MetricMonitor(
            final Log logger,
            final String tagPrefixMetric,
            final String tagPrefixRawMetric,
            final String metricFieldName,
            final int metricIntervalSeconds
    )
    {
        this.logger = logger;

        this.tagPrefixMetric = tagPrefixMetric;
        this.tagPrefixRawMetric = tagPrefixRawMetric;
        this.metricFieldName = metricFieldName;

        this.metricIntervalSeconds = metricIntervalSeconds;
        this.sleepIntervalMilliSeconds = SLEEP_INTERVAL_MSEC;

        this.metricProducers = new ArrayList<>();
        this.rawMetricProducers = new ArrayList<>();
        this.metricProducersComplex = new ArrayList<>();
        this.rawMetricProducersComplex = new ArrayList<>();

        this.running = new AtomicBoolean(false);
    }

    public void addMetricProducer(final MetricProducer producer)
    {
        metricProducers.add(producer);
    }

    public void addMetricProducer(final ComplexMetricProducer producer)
    {
        metricProducersComplex.add(producer);
    }

    public void addRawMetricProducer(final MetricProducer producer)
    {
        rawMetricProducers.add(producer);
    }

    public void addRawMetricProducer(final ComplexMetricProducer producer)
    {
        rawMetricProducersComplex.add(producer);
    }

    // only for MetricMonitorTest
    void setSleepInterval(final long sleepInterval)
    {
        this.sleepIntervalMilliSeconds = sleepInterval;
    }

    public void start()
    {
        running.set(true);
        monitorThread = new Thread(this::loop);
        monitorThread.start();
    }

    public void stop()
    {
        running.set(false);
        try {
            monitorThread.join(MONITOR_THREAD_STOP_TIMEOUT);
            if (monitorThread.isAlive()) {
                monitorThread.interrupt();
                monitorThread.join(MONITOR_THREAD_STOP_TIMEOUT);
            }
        }
        catch (InterruptedException e) {
            Thread.currentThread().interrupt(); // preserve interrupted status
        }
    }

    private void loop()
    {
        long metricInterval = metricIntervalSeconds * 1_000_000_000L;

        long nextTick = Clock.now() + metricInterval;
        while (running.get()) {
            try {
                Thread.sleep(sleepIntervalMilliSeconds);
                if (running.get() && Clock.now() >= nextTick) {
                    nextTick += metricInterval; // to set nextTick even when run() throws exceptions
                    run();
                }
            }
            catch (Throwable e) {
                logger.error(String.format("MetricMonitor got an error %s: %s", e.getClass().getName(), e.getMessage()), e);
            }
        }
    }

    private void run()
    {
        Instant now = Instant.now();

        processMetric(logger, now, metricProducers, tagPrefixMetric);
        processComplexMetric(logger, now, metricProducersComplex, tagPrefixMetric);

        processMetric(logger, now, rawMetricProducers, tagPrefixRawMetric);
        processComplexMetric(logger, now, rawMetricProducersComplex, tagPrefixRawMetric);
    }

    private void processMetric(Log logger, Instant now, List<MetricProducer> producers, String tagPrefix)
    {
        for (MetricProducer producer : producers) {
            for (Map.Entry<String, Object> kv : producer.produce().entrySet()) {
                logger.sendEvent(tagPrefix + kv.getKey(), now, ImmutableMap.of(metricFieldName, kv.getValue()));
            }
        }
    }

    private void processComplexMetric(Log logger, Instant now, List<ComplexMetricProducer> producers, String tagPrefix)
    {
        for (ComplexMetricProducer producer : producers) {
            for (ComplexMetric metric : producer.produce()) {
                Map<String, Object> record = ImmutableMap.<String, Object>builder()
                        .put(metricFieldName, metric.getValue())
                        .putAll(metric.getAdditional())
                        .build();
                logger.sendEvent(tagPrefix + metric.getName(), now, record);
            }
        }
    }
}
