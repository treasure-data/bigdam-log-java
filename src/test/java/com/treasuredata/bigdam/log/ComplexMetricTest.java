package com.treasuredata.bigdam.log;

import com.google.common.collect.ImmutableMap;

import java.util.Map;

import org.junit.Test;

import static org.hamcrest.Matchers.is;
import static org.junit.Assert.*;

public class ComplexMetricTest
{
    private ComplexMetric m;

    @Test
    public void createAndGetWithAnAttribute()
    {
        m = new ComplexMetric("metric1", 1, "name", "tagomoris");
        assertThat(m.getName(), is("metric1"));
        assertThat(m.getValue(), is(1));
        assertThat(m.getAdditional().size(), is(1));
        assertThat(m.getAdditional().get("name"), is("tagomoris"));
    }

    @Test
    public void createAndGet()
    {
        m = new ComplexMetric("metric2", 100L, ImmutableMap.of("key1", 1, "key2", "value2", "tag", "TAG"));
        assertThat(m.getName(), is("metric2"));
        assertThat(m.getValue(), is(100L));
        assertThat(m.getAdditional().size(), is(3));
        assertThat(m.getAdditional().get("key1"), is(1));
        assertThat(m.getAdditional().get("key2"), is("value2"));
        assertThat(m.getAdditional().get("tag"), is("TAG"));
    }
}
