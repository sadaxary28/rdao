package com.infomaximum.database.core.sequence;

import com.infomaximum.database.utils.TypeConvert;
import com.infomaximum.rocksdb.RocksDataBase;
import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.RocksDBException;

import java.util.concurrent.atomic.AtomicLong;

/**
 * Created by kris on 22.04.17.
 */
public class Sequence {

    private final static int SIZE_CACHE = 10;

    private final RocksDataBase rocksDataBase;
    private final ColumnFamilyHandle columnFamilyHandle;
    private final byte[] key;

    private final AtomicLong counter;
    private long maxCacheValue;

    protected Sequence(RocksDataBase rocksDataBase, ColumnFamilyHandle columnFamilyHandle, byte[] key) throws RocksDBException {
        this.rocksDataBase = rocksDataBase;
        this.columnFamilyHandle = columnFamilyHandle;
        this.key = key;

        byte[] value = rocksDataBase.getRocksDB().get(columnFamilyHandle, key);
        maxCacheValue = TypeConvert.unpackLong(value);
        counter = new AtomicLong(maxCacheValue);
        growCache();
    }

    private synchronized void growCache() throws RocksDBException {
        if (maxCacheValue - counter.get() > SIZE_CACHE) {
            return;
        }

        maxCacheValue += SIZE_CACHE;
        rocksDataBase.getRocksDB().put(columnFamilyHandle, key, TypeConvert.pack(maxCacheValue));
    }

    public long next() throws RocksDBException {
        long value;
        do {
            value = counter.get();
            if (value >= maxCacheValue) {
                //Кеш закончился-берем еще
                growCache();
            }
        } while (!counter.compareAndSet(value, value + 1));
        return value + 1;
    }
}
