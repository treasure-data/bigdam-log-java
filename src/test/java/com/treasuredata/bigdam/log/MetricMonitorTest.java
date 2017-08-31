package com.treasuredata.bigdam.log;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

import java.time.Instant;
import java.util.Map;

import org.junit.After;
import org.junit.Test;

import static org.hamcrest.Matchers.is;
import static org.junit.Assert.*;
import static org.mockito.Mockito.*;

public class MetricMonitorTest
{
    @After
    public void teardown()
    {
        Clock.clear();
    }

    private void sleep(long milli)
    {
        try {
            Thread.sleep(milli);
        }
        catch (InterruptedException e) {
            throw new RuntimeException("test was interrupted", e);
        }
    }

    @Test
    public void createAndRunWithoutProducers()
    {
        Log logger = mock(Log.class);
        MetricMonitor monitor = new MetricMonitor(logger, "metric.", "raw.", "v", 60);

        long start = System.nanoTime();

        Clock.set(start);
        monitor.setSleepInterval(100L);

        monitor.start();

        sleep(500L);

        verify(logger, never()).sendEvent(any(String.class), any(Instant.class), any());

        long now = start + 60_100_000_000L; // 0.1sec for care of diff
        Clock.set(now);

        sleep(500L);

        verify(logger, never()).sendEvent(any(String.class), any(Instant.class), any());

        monitor.stop();
    }

    @Test
    public void createAndRunWithMetrics()
    {
        Log logger = mock(Log.class);
        MetricMonitor monitor = new MetricMonitor(logger, "metric.", "raw.", "v", 60);
        monitor.addMetricProducer(() -> ImmutableMap.of("name1", 100, "name2", 1));
        monitor.addMetricProducer(() -> ImmutableList.of(
                new ComplexMetric("name3", 3, "label", "tagomoris"),
                new ComplexMetric("name4", 400, ImmutableMap.of("k1", "v1", "k2", "v2"))
        ));
        monitor.addRawMetricProducer(() -> ImmutableMap.of("name5", 50, "name6", 6, "name7", 7000L));
        monitor.addRawMetricProducer(() -> ImmutableList.of(new ComplexMetric("name8", 8, "k8", "v8")));

        long start = System.nanoTime();

        Clock.set(start);
        monitor.setSleepInterval(100L);

        monitor.start();

        sleep(500L);

        verify(logger, never()).sendEvent(any(String.class), any(Instant.class), any());

        long now = start + 60_100_000_000L; // 0.1sec for care of diff
        Clock.set(now);

        sleep(500L);

        verify(logger, times(1)).sendEvent(eq("metric.name1"), any(Instant.class), eq(ImmutableMap.of("v", 100)));
        verify(logger, times(1)).sendEvent(eq("metric.name2"), any(Instant.class), eq(ImmutableMap.of("v", 1)));
        verify(logger, times(1)).sendEvent(eq("metric.name3"), any(Instant.class), eq(ImmutableMap.of("v", 3, "label", "tagomoris")));
        verify(logger, times(1)).sendEvent(eq("metric.name4"), any(Instant.class), eq(ImmutableMap.of("v", 400, "k1", "v1", "k2", "v2")));
        verify(logger, times(1)).sendEvent(eq("raw.name5"), any(Instant.class), eq(ImmutableMap.of("v", 50)));
        verify(logger, times(1)).sendEvent(eq("raw.name6"), any(Instant.class), eq(ImmutableMap.of("v", 6)));
        verify(logger, times(1)).sendEvent(eq("raw.name7"), any(Instant.class), eq(ImmutableMap.of("v", 7000L)));
        verify(logger, times(1)).sendEvent(eq("raw.name8"), any(Instant.class), eq(ImmutableMap.of("v", 8, "k8", "v8")));

        monitor.stop();
    }

    private boolean exceptionThrown = false;

    private Map<String, Object> buggyCode()
    {
        if (!exceptionThrown) {
            exceptionThrown = true;
            throw new RuntimeException("yaaay");
        }
        return ImmutableMap.of("name", 100);
    }

    @Test
    public void handleExceptions()
    {
        Log logger = mock(Log.class);
        MetricMonitor monitor = new MetricMonitor(logger, "metric.", "raw.", "v", 60);
        monitor.addMetricProducer(this::buggyCode);

        long start = System.nanoTime();

        Clock.set(start);
        monitor.setSleepInterval(100L);

        monitor.start();

        sleep(500L);

        verify(logger, never()).sendEvent(any(String.class), any(Instant.class), any());

        long now = start + 60_100_000_000L; // 0.1sec for care of diff
        Clock.set(now);

        sleep(500L);

        verify(logger, times(1)).error(eq("MetricMonitor got an error java.lang.RuntimeException: yaaay"), any(RuntimeException.class));

        now = now + 30_000_000_000L;
        Clock.set(now);

        sleep(500L);

        verify(logger, never()).sendEvent(any(String.class), any(Instant.class), any());

        now = now + 30_000_000_000L;
        Clock.set(now);

        sleep(500L);

        verify(logger, times(1)).sendEvent(eq("metric.name"), any(Instant.class), eq(ImmutableMap.of("v", 100)));

        monitor.stop();
    }
}
