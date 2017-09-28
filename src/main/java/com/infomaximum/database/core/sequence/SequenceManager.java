package com.infomaximum.database.core.sequence;

import com.infomaximum.database.utils.ByteUtils;
import com.infomaximum.database.utils.TypeConvert;
import com.infomaximum.rocksdb.RocksDataBase;
import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.RocksDBException;
import org.rocksdb.RocksIterator;

import java.util.Arrays;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

public class SequenceManager {

    public static final String SEQUENCE_PREFIX = "sequence.";

    private final RocksDataBase rocksDataBase;
    private final ColumnFamilyHandle defaultColumnFamily;
    private final ConcurrentMap<String, Sequence> sequences;

    public SequenceManager(RocksDataBase rocksDataBase) throws RocksDBException {
        this.rocksDataBase = rocksDataBase;
        this.defaultColumnFamily = rocksDataBase.getDefaultColumnFamily();
        this.sequences = new ConcurrentHashMap<>();
        readSequences();
    }

    public Sequence getSequence(String name) {
       return sequences.get(name);
    }

    public void createSequence(String name) throws RocksDBException {
        if (sequences.containsKey(name)) {
            throw new SequenceDBException(name);
        }

        final byte[] key = createSequenceKey(name);
        rocksDataBase.getRocksDB().put(defaultColumnFamily, key, TypeConvert.pack(0L));
        sequences.put(name, new Sequence(rocksDataBase, defaultColumnFamily, key));
    }

    public void dropSequence(String name) throws RocksDBException {
        sequences.remove(name);
        rocksDataBase.getRocksDB().delete(defaultColumnFamily, createSequenceKey(name));
    }

    private static byte[] createSequenceKey(String sequenceName) {
        return TypeConvert.pack(SEQUENCE_PREFIX + sequenceName);
    }

    private void readSequences() throws RocksDBException {
        try (RocksIterator i = rocksDataBase.getRocksDB().newIterator(defaultColumnFamily)) {
            final byte[] keyPrefix = TypeConvert.pack(SEQUENCE_PREFIX);
            i.seek(keyPrefix);
            while (true) {
                if (!i.isValid()) {
                    i.status();
                    break;
                }

                byte[] key = i.key();
                if (!ByteUtils.startsWith(keyPrefix, key)) {
                    break;
                }

                String sequenceName = TypeConvert.getString(Arrays.copyOfRange(key, keyPrefix.length, key.length));
                sequences.put(sequenceName, new Sequence(rocksDataBase, defaultColumnFamily, key));
                i.next();
            }
        }
    }
}
