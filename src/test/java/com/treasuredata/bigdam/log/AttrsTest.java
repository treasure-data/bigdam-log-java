package com.treasuredata.bigdam.log;

import java.util.Map;

import org.junit.Test;

import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;
import static org.junit.Assert.*;

public class AttrsTest
{
    @Test
    public void ofWithAnArgument()
    {
        Map<String, Integer> attrs = Attrs.of("one", 1);
        assertThat(attrs.size(), is(1));
        assertThat(attrs.get("one"), is(1));
    }

    @Test
    public void ofWith2args()
    {
        Map<String, Integer> attrs = Attrs.of("one", 1, "two", 2);
        assertThat(attrs.size(), is(2));
        assertThat(attrs.get("one"), is(1));
        assertThat(attrs.get("two"), is(2));
    }

    @Test
    public void ofWith2argsWithNullValue()
    {
        Map<String, Integer> attrs = Attrs.of("one", null, "two", 2);
        assertThat(attrs.size(), is(2));
        assertThat(attrs.get("one"), is(nullValue()));
        assertThat(attrs.get("two"), is(2));
    }

    @Test
    public void ofWith3args()
    {
        Map<String, Integer> attrs = Attrs.of("one", 1, "two", 2, "three", 3);
        assertThat(attrs.size(), is(3));
        assertThat(attrs.get("one"), is(1));
        assertThat(attrs.get("two"), is(2));
        assertThat(attrs.get("three"), is(3));
    }

    @Test
    public void ofWith4args()
    {
        Map<String, Integer> attrs = Attrs.of("one", 1, "two", 2, "three", 3, "four", 4);
        assertThat(attrs.size(), is(4));
        assertThat(attrs.get("one"), is(1));
        assertThat(attrs.get("two"), is(2));
        assertThat(attrs.get("three"), is(3));
        assertThat(attrs.get("four"), is(4));
    }

    @Test
    public void ofWith5args()
    {
        Map<String, Integer> attrs = Attrs.of("one", 1, "two", 2, "three", 3, "four", 4, "five", 5);
        assertThat(attrs.size(), is(5));
        assertThat(attrs.get("one"), is(1));
        assertThat(attrs.get("two"), is(2));
        assertThat(attrs.get("three"), is(3));
        assertThat(attrs.get("four"), is(4));
        assertThat(attrs.get("five"), is(5));
    }

    @Test
    public void ofWith6args()
    {
        Map<String, Integer> attrs = Attrs.of("one", 1, "two", 2, "three", 3, "four", 4, "five", 5, "six", 6);
        assertThat(attrs.size(), is(6));
        assertThat(attrs.get("one"), is(1));
        assertThat(attrs.get("two"), is(2));
        assertThat(attrs.get("three"), is(3));
        assertThat(attrs.get("four"), is(4));
        assertThat(attrs.get("five"), is(5));
        assertThat(attrs.get("six"), is(6));
    }

    @Test
    public void ofWith7args()
    {
        Map<String, Integer> attrs = Attrs.of("one", 1, "two", 2, "three", 3, "four", 4, "five", 5, "six", 6, "seven", 7);
        assertThat(attrs.size(), is(7));
        assertThat(attrs.get("one"), is(1));
        assertThat(attrs.get("two"), is(2));
        assertThat(attrs.get("three"), is(3));
        assertThat(attrs.get("four"), is(4));
        assertThat(attrs.get("five"), is(5));
        assertThat(attrs.get("six"), is(6));
        assertThat(attrs.get("seven"), is(7));
    }
}
