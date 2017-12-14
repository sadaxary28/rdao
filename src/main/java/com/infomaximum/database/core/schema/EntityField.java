package com.infomaximum.database.core.schema;

import com.infomaximum.database.core.anotation.Field;
import com.infomaximum.database.exeption.runtime.IllegalTypeException;
import com.infomaximum.database.exeption.runtime.StructEntityException;
import com.infomaximum.database.utils.EqualsUtils;

public class EntityField {

    private final String name;
    private final Class type;
    private final TypeConverter converter;
    private final StructEntity foreignDependency;

    EntityField(Field field, StructEntity parent) {
        this.name = field.name();
        this.type = field.type();
        this.converter = buildPacker(field.packerType());
        if (field.foreignDependency() != Class.class) {
            if (parent.getObjectClass() != field.foreignDependency()) {
                this.foreignDependency = Schema.ensureEntity(field.foreignDependency());
            } else {
                this.foreignDependency = parent;
            }
        } else {
            this.foreignDependency = null;
        }

        if (isForeign() && this.type != Long.class) {
            throw new StructEntityException("Type of foreign field " + field.name() + " must be " + Long.class + ".");
        }
    }

    public String getName() {
        return name;
    }

    public Class getType() {
        return type;
    }

    public TypeConverter getConverter() {
        return converter;
    }

    public boolean isForeign() {
        return foreignDependency != null;
    }

    public StructEntity getForeignDependency() {
        return foreignDependency;
    }

    public void throwIfNotMatch(Class type) {
        if (!EqualsUtils.equalsType(this.type, type)) {
            throw new IllegalTypeException(this.type, type);
        }
    }

    private static TypeConverter buildPacker(Class<?> packerClass) {
        if (packerClass == Class.class) {
            return null;
        }

        try {
            return (TypeConverter) packerClass.newInstance();
        } catch (ReflectiveOperationException e) {
            throw new IllegalTypeException(e);
        }
    }
}
