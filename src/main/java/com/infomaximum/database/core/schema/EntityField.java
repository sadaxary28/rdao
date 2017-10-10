package com.infomaximum.database.core.schema;

import com.infomaximum.database.core.anotation.Field;
import com.infomaximum.database.exeption.runtime.IllegalTypeDatabaseException;
import com.infomaximum.database.utils.EqualsUtils;

public class EntityField {

    private final String name;
    private final Class type;
    private final TypePacker packer;

    protected EntityField(Field field) {
        this.name = field.name();
        this.type = field.type();
        this.packer = buildPacker(field.packerType());
    }

    public String getName() {
        return name;
    }

    public Class getType() {
        return type;
    }

    public TypePacker getPacker() {
        return packer;
    }

    public void throwIfNotMatch(Class type) {
        if (!EqualsUtils.equalsType(this.type, type)) {
            throw new IllegalTypeDatabaseException(this.type, type);
        }
    }

    private static TypePacker buildPacker(Class<?> packerClass) {
        if (packerClass == Class.class) {
            return null;
        }

        try {
            return (TypePacker) packerClass.newInstance();
        } catch (ReflectiveOperationException e) {
            throw new IllegalTypeDatabaseException(e);
        }
    }
}
