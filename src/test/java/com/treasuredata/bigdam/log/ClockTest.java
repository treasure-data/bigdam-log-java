package com.treasuredata.bigdam.log;

import org.junit.After;
import org.junit.Test;

import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.lessThanOrEqualTo;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.*;

public class ClockTest
{
    @After
    public void teardown()
    {
        Clock.clear();
    }

    @Test
    public void now()
    {
        long time1 = System.nanoTime();
        long time2 = Clock.now();
        long time3 = System.nanoTime();

        assertThat(time2, is(greaterThanOrEqualTo(time1)));
        assertThat(time2, is(lessThanOrEqualTo(time3)));

        Clock.set(time2);
        assertThat(Clock.now(), is(time2));

        Clock.clear();
        long time4 = System.nanoTime();
        assertThat(time4, is(greaterThanOrEqualTo(time4)));
    }
}
