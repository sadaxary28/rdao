package com.infomaximum.database.maintenance;

import com.infomaximum.database.core.anotation.Entity;
import com.infomaximum.database.core.schema.StructEntity;
import com.infomaximum.database.datasource.DataSource;
import com.infomaximum.database.domainobject.DomainObject;
import com.infomaximum.database.domainobject.DomainObjectSource;
import com.infomaximum.database.exeption.DatabaseException;
import com.infomaximum.database.exeption.runtime.StructEntityDatabaseException;
import com.infomaximum.database.exeption.runtime.ValidationDatabaseException;
import com.infomaximum.database.maintenance.addition.MaintenanceAdditionDomain;
import com.infomaximum.database.maintenance.validation.MaintenanceValidationIndex;

import java.util.HashSet;
import java.util.Set;

/*
 Не потоко безопасный класс
 */
public class MaintenanceWorker {

    private final DomainObjectSource domainObjectSource;
    private final DataSource dataSource;

    private boolean addition = true;
    private boolean validation = true;
    private Set<Class<? extends DomainObject>> maintenanceClasses;

    private MaintenanceWorker(DomainObjectSource domainObjectSource, DataSource dataSource) {
        this.domainObjectSource = domainObjectSource;
        this.dataSource = dataSource;
    }


    public void exec() throws DatabaseException {
        //Проверяем уникальность пространсва имен
        if (validation) {
            Set<String> entityNames = new HashSet<String>();
            for (Class<? extends DomainObject> iClass : maintenanceClasses) {
                String entityName = StructEntity.getInstance(iClass).getName();
                if (entityNames.contains(entityName))
                    throw new StructEntityDatabaseException("Conflict entity names is duplicate: " + entityName);
                entityNames.add(entityName);
            }
        }

        //Проверяем наличие структур в базе данных и если надо создаем
        if (addition) {
            for (Class<? extends DomainObject> iClass : maintenanceClasses) {
                new MaintenanceAdditionDomain(dataSource, domainObjectSource, iClass).exec();
            }
        }

        //Проверяем подробно каждую сущность
        if (validation) {
            for (Class<? extends DomainObject> iClass : maintenanceClasses) {
                new MaintenanceValidationIndex(dataSource, domainObjectSource, iClass).exec();
            }
        }
    }

    public static class Builder {

        private final DomainObjectSource domainObjectSource;
        private final DataSource dataSource;

        private boolean addition = true;
        private boolean validation = true;
        private final Set<Class<? extends DomainObject>> maintenanceClasses;

        public Builder(DomainObjectSource domainObjectSource, DataSource dataSource) {
            this.domainObjectSource = domainObjectSource;
            this.dataSource = dataSource;

            this.maintenanceClasses = new HashSet<Class<? extends DomainObject>>();
        }

        public Builder withMaintenanceClass(Class<? extends DomainObject> clazz) {
            if (maintenanceClasses.contains(clazz)) return this;

            Entity entityAnnotation = clazz.getAnnotation(Entity.class);
            if (entityAnnotation == null)
                throw new ValidationDatabaseException("Not support maintenance class: " + clazz);

            maintenanceClasses.add(clazz);
            return this;
        }

        public Builder setAddition(boolean value) {
            this.addition = value;
            return this;
        }

        public Builder setValidation(boolean value) {
            this.validation = value;
            return this;
        }

        public MaintenanceWorker build() {
            MaintenanceWorker maintenanceWorker = new MaintenanceWorker(domainObjectSource, dataSource);
            maintenanceWorker.addition = addition;
            maintenanceWorker.validation = validation;
            maintenanceWorker.maintenanceClasses = maintenanceClasses;
            return maintenanceWorker;
        }
    }
}
