package com.infomaximum.rocksdb.transaction;

import com.infomaximum.rocksdb.core.datasource.DataSource;
import com.infomaximum.rocksdb.core.objectsource.utils.DomainObjectFieldValueUtils;
import com.infomaximum.rocksdb.core.objectsource.utils.key.Key;
import com.infomaximum.rocksdb.core.objectsource.utils.key.KeyAvailability;
import com.infomaximum.rocksdb.core.objectsource.utils.key.KeyField;
import com.infomaximum.rocksdb.core.objectsource.utils.structentity.StructEntityUtils;
import com.infomaximum.rocksdb.core.struct.DomainObject;
import com.infomaximum.rocksdb.struct.RocksDataBase;
import com.infomaximum.rocksdb.transaction.struct.modifier.Modifier;
import com.infomaximum.rocksdb.transaction.struct.modifier.ModifierRemove;
import com.infomaximum.rocksdb.transaction.struct.modifier.ModifierSet;
import com.infomaximum.rocksdb.utils.TypeConvertRocksdb;
import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.RocksDBException;
import java.lang.reflect.Field;
import java.util.*;

/**
 * Created by user on 23.04.2017.
 */
public class Transaction {

    private final DataSource dataSource;
    private List<Modifier> queue;
    private boolean active;

    public Transaction(DataSource dataSource) {
        this.dataSource = dataSource;
        this.queue = new ArrayList<Modifier>();
        this.active=true;
    }

    public boolean isActive() {
        return active;
    }

    public void update(String columnFamily, DomainObject self, Set<Field> fields) throws IllegalAccessException {
        queue.add(new ModifierSet(columnFamily, new KeyAvailability(self.getId()).pack(), TypeConvertRocksdb.pack(self.getId())));
        for (Field field: fields) {
            String formatFieldName = StructEntityUtils.getFormatFieldName(field);
            String key = new KeyField(self.getId(), formatFieldName).pack();
            byte[] value = DomainObjectFieldValueUtils.packValue(self, field);
            if (value!=null) {
                queue.add(new ModifierSet(columnFamily, key, value));
            } else {
                queue.add(new ModifierRemove(columnFamily, key));
            }
        }
    }

    public void remove(String columnFamily, DomainObject self) {
        String removeKeys = new StringBuilder().append(Key.packId(self.getId())).append(".*").toString();
        queue.add(new ModifierRemove(columnFamily, removeKeys));
    }

    public void commit() throws RocksDBException {
        if (!active) throw new RuntimeException("Transaction is not active: is commited");

        //Комитим
        dataSource.commit(queue);

        //все ставим флаг, что транзакция больше не активна
        active=false;
        queue = null;
    }
}
