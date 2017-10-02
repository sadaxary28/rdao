package com.infomaximum.database.maintenance.validation;

import com.infomaximum.database.core.schema.StructEntity;
import com.infomaximum.database.datasource.DataSource;
import com.infomaximum.database.domainobject.DomainObject;
import com.infomaximum.database.domainobject.DomainObjectSource;
import com.infomaximum.database.exeption.DataSourceDatabaseException;

public class MaintenanceValidationStruct {

    private final DomainObjectSource domainObjectSource;
    private final DataSource dataSource;

    private final Class<? extends DomainObject> classEntity;
    private final StructEntity structEntity;

    public MaintenanceValidationStruct(DataSource dataSource, DomainObjectSource domainObjectSource, Class<? extends DomainObject> classEntity) {
        this.dataSource = dataSource;
        this.domainObjectSource = domainObjectSource;

        this.classEntity = classEntity;
        this.structEntity = StructEntity.getInstance(classEntity);
    }

    public void exec() throws DataSourceDatabaseException {
        //TODO Валидация
    }

}
