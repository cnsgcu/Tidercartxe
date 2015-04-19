package web.journal.service.tools;

import backtype.storm.utils.Time;
import org.apache.commons.collections.buffer.CircularFifoBuffer;

public class NthLastModifiedTimeTracker {

    private static final int MILLIS_IN_SEC = 1000;

    private final CircularFifoBuffer lastModifiedTimesMillis;

    public NthLastModifiedTimeTracker(int numTimesToTrack)
    {
        if (numTimesToTrack < 1) {
            throw new IllegalArgumentException(
                    "numTimesToTrack must be greater than zero (you requested " + numTimesToTrack + ")");
        }
        lastModifiedTimesMillis = new CircularFifoBuffer(numTimesToTrack);
        initLastModifiedTimesMillis();
    }

    private void initLastModifiedTimesMillis()
    {
        long nowCached = now();
        for (int i = 0; i < lastModifiedTimesMillis.maxSize(); i++) {
            lastModifiedTimesMillis.add(Long.valueOf(nowCached));
        }
    }

    private long now()
    {
        return Time.currentTimeMillis();
    }

    public int secondsSinceOldestModification()
    {
        long modifiedTimeMillis = ((Long) lastModifiedTimesMillis.get()).longValue();
        return (int) ((now() - modifiedTimeMillis) / MILLIS_IN_SEC);
    }

    public void markAsModified()
    {
        updateLastModifiedTime();
    }

    private void updateLastModifiedTime()
    {
        lastModifiedTimesMillis.add(now());
    }

}