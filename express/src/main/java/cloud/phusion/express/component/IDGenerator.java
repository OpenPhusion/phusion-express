package cloud.phusion.express.component;

import cloud.phusion.Context;
import cloud.phusion.PhusionException;

import java.io.FileInputStream;
import java.io.InputStream;
import java.util.ArrayList;

public class IDGenerator {

    private long dataCenterId;
    private long workerId;

    private long sequence = 0L;
    private final long twepoch = 1288834974657L;
    private final long workerIdBits = 5L;
    private final long datacenterIdBits = 5L;
    private final long maxWorkerId = -1L ^ (-1L << workerIdBits);
    private final long maxDatacenterId = -1L ^ (-1L << datacenterIdBits);
    private final long sequenceBits = 12L;
    private final long workerIdShift = sequenceBits;
    private final long datacenterIdShift = sequenceBits + workerIdBits;
    private final long timestampLeftShift = sequenceBits + workerIdBits + datacenterIdBits;
    private final long sequenceMask = -1L ^ (-1L << sequenceBits);
    private long lastTimestamp = -1L;

    public IDGenerator(long dataCenterId, long workerId) throws Exception {
        super();

        if (workerId > maxWorkerId || workerId < 0) {
            throw new IllegalArgumentException(String.format("Worker ID must be in [0, %d]", maxWorkerId));
        }
        if (dataCenterId > maxDatacenterId || dataCenterId < 0) {
            throw new IllegalArgumentException(String.format("Data Center ID must be in [0, %d]", maxDatacenterId));
        }

        this.dataCenterId = dataCenterId;
        this.workerId = workerId;
    }

    public synchronized long nextId(Context ctx) throws Exception {
        long timestamp = System.currentTimeMillis();

        if (timestamp < lastTimestamp) {
            throw new PhusionException("SYS_CLOCK", String.format("Failed to generate ID because system clock has gone backwards, wait %dms and retry",
                    lastTimestamp - timestamp), ctx);
        }

        if (lastTimestamp == timestamp) {
            sequence = (sequence + 1) & sequenceMask;
            if (sequence == 0) timestamp = _untilNextMillis(lastTimestamp);
        }
        else sequence = 0L;

        lastTimestamp = timestamp;

        return ((timestamp - twepoch) << timestampLeftShift) | (dataCenterId << datacenterIdShift)
                | (workerId << workerIdShift) | sequence;
    }

    private long _untilNextMillis(long lastTimestamp) {
        long timestamp = System.currentTimeMillis();
        while (timestamp <= lastTimestamp) {
            timestamp = System.currentTimeMillis();
        }
        return timestamp;
    }

}
