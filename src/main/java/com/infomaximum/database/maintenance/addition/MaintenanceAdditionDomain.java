package com.infomaximum.database.maintenance.addition;

import com.infomaximum.database.core.iterator.IteratorEntity;
import com.infomaximum.database.core.schema.EntityField;
import com.infomaximum.database.core.schema.EntityIndex;
import com.infomaximum.database.core.schema.StructEntity;
import com.infomaximum.database.datasource.DataSource;
import com.infomaximum.database.datasource.modifier.Modifier;
import com.infomaximum.database.datasource.modifier.ModifierSet;
import com.infomaximum.database.domainobject.DomainObject;
import com.infomaximum.database.domainobject.DomainObjectSource;
import com.infomaximum.database.domainobject.key.IndexKey;
import com.infomaximum.database.domainobject.utils.DomainIndexUtils;
import com.infomaximum.database.exeption.DataSourceDatabaseException;
import com.infomaximum.database.exeption.DatabaseException;
import com.infomaximum.database.exeption.runtime.ValidationDatabaseException;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class MaintenanceAdditionDomain {

    private final DataSource dataSource;
    private final DomainObjectSource domainObjectSource;

    private final Class<? extends DomainObject> classEntity;
    private final StructEntity structEntity;

    public MaintenanceAdditionDomain(DataSource dataSource, DomainObjectSource domainObjectSource, Class<? extends DomainObject> classEntity) {
        this.dataSource = dataSource;
        this.domainObjectSource = domainObjectSource;

        this.classEntity = classEntity;
        this.structEntity = StructEntity.getInstance(classEntity);
    }

    public void exec() throws DatabaseException {
        String columnFamilyEntity = structEntity.getName();

        //Проверяем наличие Sequence
        if (!dataSource.containsSequence(columnFamilyEntity)) {
            dataSource.createSequence(columnFamilyEntity);
        }

        //Проверяем наличие базового ColumnFamily
        if (!dataSource.containsColumnFamily(columnFamilyEntity)) {
            dataSource.createColumnFamily(columnFamilyEntity);
        }

        //Проверяем есть ли данные  в Entity
        boolean isContentEntity = isContent(columnFamilyEntity);

        //Проверяем каких индексов не хватает
        for (EntityIndex entityIndex : structEntity.getIndices()) {
            String columnFamilyIndex = entityIndex.columnFamily;

            //Прверяем наличия ColumnFamily для индекса
            if (!dataSource.containsColumnFamily(columnFamilyIndex)) {
                dataSource.createColumnFamily(columnFamilyIndex);
            }

            //Проверяем соответсвие наличия данных в индексе и данных в Entity
            boolean isContentIndex = isContent(columnFamilyIndex);

            if (isContentEntity && !isContentIndex) {
                //Данные есть, но нет индексов - строим

                long transactionId = dataSource.beginTransaction();
                final List<Modifier> modifiers = new ArrayList<>();
                try {
                    try (IteratorEntity<? extends DomainObject> ie = domainObjectSource.iterator(classEntity, null)) {
                        while (ie.hasNext()) {
                            DomainObject self = ie.next();

                            final IndexKey indexKey = new IndexKey(self.getId(), new long[entityIndex.sortedFields.size()]);

                            Map<EntityField, Object> values = new HashMap<>();
                            for (int i = 0; i < entityIndex.sortedFields.size(); ++i) {
                                EntityField field = entityIndex.sortedFields.get(i);
                                values.put(field, self.get(field.getType(), field.name));
                            }
                            DomainIndexUtils.setHashValues(entityIndex.sortedFields, values, indexKey.getFieldValues());

                            modifiers.add(new ModifierSet(columnFamilyIndex, indexKey.pack()));
                        }

                    }
                    dataSource.modify(modifiers, transactionId);
                    dataSource.commitTransaction(transactionId);
                } catch (Throwable e) {
                    dataSource.rollbackTransaction(transactionId);
                    throw e;
                }


            } else if (!isContentEntity && isContentIndex) {
                //Нет данных, но есть индексы - полная фигня - надо падать
                throw new ValidationDatabaseException("Ошибка структуры данных, построены индексы: "
                        + columnFamilyIndex + ", но данные в " + columnFamilyEntity + " отсутсвуют.");
            }
        }

    }

    private boolean isContent(String columnFamily) throws DataSourceDatabaseException {
        long iteratorId = dataSource.createIterator(columnFamily, null);
        try {
            return (dataSource.next(iteratorId) != null);
        } finally {
            dataSource.closeIterator(iteratorId);
        }
    }
}
