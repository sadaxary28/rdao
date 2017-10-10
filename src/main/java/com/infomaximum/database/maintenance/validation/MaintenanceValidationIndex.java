package com.infomaximum.database.maintenance.validation;


import com.infomaximum.database.core.schema.EntityIndex;
import com.infomaximum.database.core.schema.StructEntity;
import com.infomaximum.database.datasource.DataSource;
import com.infomaximum.database.domainobject.DomainObject;
import com.infomaximum.database.domainobject.DomainObjectSource;
import com.infomaximum.database.exeption.DataSourceDatabaseException;
import com.infomaximum.database.exeption.runtime.ValidationDatabaseException;

public class MaintenanceValidationIndex {

    private final DomainObjectSource domainObjectSource;
    private final DataSource dataSource;

    private final Class<? extends DomainObject> classEntity;
    private final StructEntity structEntity;

    public MaintenanceValidationIndex(DataSource dataSource, DomainObjectSource domainObjectSource, Class<? extends DomainObject> classEntity) {
        this.dataSource = dataSource;
        this.domainObjectSource = domainObjectSource;

        this.classEntity = classEntity;
        this.structEntity = StructEntity.getInstance(classEntity);
    }

    public void exec() throws DataSourceDatabaseException {
        String[] columnFamilies = dataSource.getColumnFamilies();
        for (String columnFamily : columnFamilies) {
            if (columnFamily.equals(structEntity.getName())) continue;

            //Нашли интексный columnFamily - проверяем его наличие в структуре
            if (columnFamily.startsWith(structEntity.getName() + ".")) {

                boolean isContainsIndex = false;
                for (EntityIndex structEntityIndex : structEntity.getIndexes()) {
                    if (structEntityIndex.columnFamily.equals(columnFamily)) {
                        isContainsIndex = true;
                        break;
                    }
                }

                if (!isContainsIndex)
                    throw new ValidationDatabaseException("Detected columnFamily is not support: " + columnFamily);
            }
        }
    }

}
