package com.treasuredata.bigdam.log;

import com.google.common.collect.ImmutableMap;

import java.util.Map;

public class ComplexMetric
{
    private final String name;
    private final Object value;
    private final Map<String, Object> additional;

    public ComplexMetric(final String name, final Object value, final String attrName, final Object attrValue)
    {
        this(name, value, ImmutableMap.of(attrName, attrValue));
    }

    public ComplexMetric(final String name, final Object value, final Map<String, Object> additional)
    {
        this.name = name;
        this.value = value;
        this.additional = additional;
    }

    public String getName()
    {
        return name;
    }

    public Object getValue()
    {
        return value;
    }

    public Map<String, Object> getAdditional()
    {
        return additional;
    }
}

