package com.treasuredata.bigdam.log;

public class Clock
{
    public static long fixed = 0L;
    public static boolean set = false;

    public static synchronized long now()
    {
        return set ? fixed : System.nanoTime();
    }

    public static synchronized void set(final long v)
    {
        fixed = v;
        set = true;
    }

    public static synchronized void clear()
    {
        set = false;
        fixed = 0L;
    }
}
