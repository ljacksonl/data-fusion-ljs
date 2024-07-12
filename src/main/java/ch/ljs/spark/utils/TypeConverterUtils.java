/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package ch.ljs.spark.utils;

import ch.ljs.spark.type.*;
import org.apache.spark.sql.types.*;

import java.util.HashMap;
import java.util.Map;

import static com.google.common.base.Preconditions.checkNotNull;

public class TypeConverterUtils {

    private static final Map<DataType, LjsDataType<?>> TO_SEA_TUNNEL_TYPES =
            new HashMap<>(16);
    public static final String ROW_KIND_FIELD = "op";
    public static final String LOGICAL_TIME_TYPE_FLAG = "logical_time_type";

    static {
        TO_SEA_TUNNEL_TYPES.put(DataTypes.NullType, BasicType.VOID_TYPE);
        TO_SEA_TUNNEL_TYPES.put(DataTypes.StringType, BasicType.STRING_TYPE);
        TO_SEA_TUNNEL_TYPES.put(DataTypes.BooleanType, BasicType.BOOLEAN_TYPE);
        TO_SEA_TUNNEL_TYPES.put(DataTypes.ByteType, BasicType.BYTE_TYPE);
        TO_SEA_TUNNEL_TYPES.put(DataTypes.ShortType, BasicType.SHORT_TYPE);
        TO_SEA_TUNNEL_TYPES.put(DataTypes.IntegerType, BasicType.INT_TYPE);
        TO_SEA_TUNNEL_TYPES.put(DataTypes.LongType, BasicType.LONG_TYPE);
        TO_SEA_TUNNEL_TYPES.put(DataTypes.FloatType, BasicType.FLOAT_TYPE);
        TO_SEA_TUNNEL_TYPES.put(DataTypes.DoubleType, BasicType.DOUBLE_TYPE);
        TO_SEA_TUNNEL_TYPES.put(DataTypes.BinaryType, PrimitiveByteArrayType.INSTANCE);
        TO_SEA_TUNNEL_TYPES.put(DataTypes.DateType, LocalTimeType.LOCAL_DATE_TYPE);
        TO_SEA_TUNNEL_TYPES.put(DataTypes.TimestampType, LocalTimeType.LOCAL_DATE_TIME_TYPE);
    }

    private TypeConverterUtils() {
        throw new UnsupportedOperationException(
                "TypeConverterUtils is a utility class and cannot be instantiated");
    }

    public static DataType convert(LjsDataType<?> dataType) {
        checkNotNull(dataType, "The SeaTunnel's data type is required.");
        switch (dataType.getSqlType()) {
            case NULL:
                return DataTypes.NullType;
            case STRING:
                return DataTypes.StringType;
            case BOOLEAN:
                return DataTypes.BooleanType;
            case TINYINT:
                return DataTypes.ByteType;
            case SMALLINT:
                return DataTypes.ShortType;
            case INT:
                return DataTypes.IntegerType;
            case BIGINT:
                return DataTypes.LongType;
            case FLOAT:
                return DataTypes.FloatType;
            case DOUBLE:
                return DataTypes.DoubleType;
            case BYTES:
                return DataTypes.BinaryType;
            case DATE:
                return DataTypes.DateType;
            case TIME:
                return DataTypes.LongType;
            case TIMESTAMP:
                return DataTypes.TimestampType;
            case ARRAY:
                return DataTypes.createArrayType(
                        convert(((ch.ljs.spark.type.ArrayType<?, ?>) dataType).getElementType()));
            case MAP:
                ch.ljs.spark.type.MapType<?, ?> mapType = (ch.ljs.spark.type.MapType<?, ?>) dataType;
                return DataTypes.createMapType(
                        convert(mapType.getKeyType()), convert(mapType.getValueType()));
            case DECIMAL:
                ch.ljs.spark.type.DecimalType decimalType = (ch.ljs.spark.type.DecimalType) dataType;
                return new org.apache.spark.sql.types.DecimalType(
                        decimalType.getPrecision(), decimalType.getScale());
            case ROW:
                return convert((LjsRowType) dataType);
            default:
        }
        throw new IllegalArgumentException("Unsupported SeaTunnel's data type: " + dataType);
    }

    private static StructType convert(LjsRowType rowType) {
        // TODO: row kind
        StructField[] fields = new StructField[rowType.getFieldNames().length];
        for (int i = 0; i < rowType.getFieldNames().length; i++) {
            LjsDataType<?> fieldType = rowType.getFieldTypes()[i];
            Metadata metadata =
                    fieldType.getSqlType() == SqlType.TIME
                            ? new MetadataBuilder().putBoolean(LOGICAL_TIME_TYPE_FLAG, true).build()
                            : Metadata.empty();

            fields[i] =
                    new StructField(rowType.getFieldNames()[i], convert(fieldType), true, metadata);
        }
        return new StructType(fields);
    }

    public static LjsDataType<?> convert(DataType sparkType) {
        checkNotNull(sparkType, "The Spark's data type is required.");
        LjsDataType<?> dataType = TO_SEA_TUNNEL_TYPES.get(sparkType);
        if (dataType != null) {
            return dataType;
        }
        if (sparkType instanceof org.apache.spark.sql.types.ArrayType) {
            return convert((org.apache.spark.sql.types.ArrayType) sparkType);
        }
        if (sparkType instanceof org.apache.spark.sql.types.MapType) {
            org.apache.spark.sql.types.MapType mapType =
                    (org.apache.spark.sql.types.MapType) sparkType;
            return new ch.ljs.spark.type.MapType<>(convert(mapType.keyType()), convert(mapType.valueType()));
        }
        if (sparkType instanceof org.apache.spark.sql.types.DecimalType) {
            org.apache.spark.sql.types.DecimalType decimalType =
                    (org.apache.spark.sql.types.DecimalType) sparkType;
            return new ch.ljs.spark.type.DecimalType(decimalType.precision(), decimalType.scale());
        }
        if (sparkType instanceof StructType) {
            return convert((StructType) sparkType);
        }
        throw new IllegalArgumentException("Unsupported Spark's data type: " + sparkType.sql());
    }

    private static ch.ljs.spark.type.ArrayType<?, ?> convert(org.apache.spark.sql.types.ArrayType arrayType) {
        switch (convert(arrayType.elementType()).getSqlType()) {
            case STRING:
                return ch.ljs.spark.type.ArrayType.STRING_ARRAY_TYPE;
            case BOOLEAN:
                return ch.ljs.spark.type.ArrayType.BOOLEAN_ARRAY_TYPE;
            case TINYINT:
                return ch.ljs.spark.type.ArrayType.BYTE_ARRAY_TYPE;
            case SMALLINT:
                return ch.ljs.spark.type.ArrayType.SHORT_ARRAY_TYPE;
            case INT:
                return ch.ljs.spark.type.ArrayType.INT_ARRAY_TYPE;
            case BIGINT:
                return ch.ljs.spark.type.ArrayType.LONG_ARRAY_TYPE;
            case FLOAT:
                return ch.ljs.spark.type.ArrayType.FLOAT_ARRAY_TYPE;
            case DOUBLE:
                return ch.ljs.spark.type.ArrayType.DOUBLE_ARRAY_TYPE;
            default:
                throw new UnsupportedOperationException(
                        String.format("Unsupported Spark's array type: %s.", arrayType.sql()));
        }
    }

    private static LjsRowType convert(StructType structType) {
        StructField[] structFields = structType.fields();
        String[] fieldNames = new String[structFields.length];
        LjsDataType<?>[] fieldTypes = new LjsDataType[structFields.length];
        for (int i = 0; i < structFields.length; i++) {
            fieldNames[i] = structFields[i].name();
            Metadata metadata = structFields[i].metadata();
            if (metadata != null
                    && metadata.contains(LOGICAL_TIME_TYPE_FLAG)
                    && metadata.getBoolean(LOGICAL_TIME_TYPE_FLAG)) {
                fieldTypes[i] = LocalTimeType.LOCAL_TIME_TYPE;
            } else {
                fieldTypes[i] = convert(structFields[i].dataType());
            }
        }
        return new LjsRowType(fieldNames, fieldTypes);
    }
}
