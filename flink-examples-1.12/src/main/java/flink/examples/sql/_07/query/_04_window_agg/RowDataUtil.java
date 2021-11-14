package flink.examples.sql._07.query._04_window_agg;

import com.ibm.icu.util.CodePointTrie;
import org.apache.flink.api.common.typeutils.base.StringSerializer;
import org.apache.flink.formats.json.JsonToRowDataConverters;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.flink.table.data.*;
import org.apache.flink.table.data.binary.BinaryArrayData;
import org.apache.flink.table.data.binary.BinaryMapData;
import org.apache.flink.table.data.binary.BinaryRowData;
import org.apache.flink.table.data.writer.BinaryArrayWriter;
import org.apache.flink.table.data.writer.BinaryRowWriter;
import org.apache.flink.table.runtime.typeutils.RawValueDataSerializer;
import org.apache.flink.table.types.logical.*;
import org.apache.flink.types.RowKind;

import javax.annotation.Nullable;
import java.io.Serializable;
import java.math.BigDecimal;
import java.time.LocalDateTime;

import static org.apache.flink.table.types.logical.utils.LogicalTypeChecks.*;
import static org.apache.flink.util.Preconditions.checkArgument;

public class RowDataUtil {


    /**
     * Creates an accessor for getting elements in an internal row data structure at the given
     * position.
     *
     * @param fieldType the element type of the row
     * @param fieldPos the element type of the row
     */
    static FieldGetter createFieldGetter(LogicalType fieldType, int fieldPos) {
        final FieldGetter fieldGetter;
        // ordered by type root definition
        switch (fieldType.getTypeRoot()) {
            case CHAR:
            case VARCHAR:
                fieldGetter = row -> row.getString(fieldPos);
                break;
            case BOOLEAN:
                fieldGetter = row -> row.getBoolean(fieldPos);
                break;
            case BINARY:
            case VARBINARY:
                fieldGetter = row -> row.getBinary(fieldPos);
                break;
            case DECIMAL:
                final int decimalPrecision = getPrecision(fieldType);
                final int decimalScale = getScale(fieldType);
                fieldGetter = row -> row.getDecimal(fieldPos, decimalPrecision, decimalScale);
                break;
            case TINYINT:
                fieldGetter = row -> row.getByte(fieldPos);
                break;
            case SMALLINT:
                fieldGetter = row -> row.getShort(fieldPos);
                break;
            case INTEGER:
            case DATE:
            case TIME_WITHOUT_TIME_ZONE:
            case INTERVAL_YEAR_MONTH:
                fieldGetter = row -> row.getInt(fieldPos);
                break;
            case BIGINT:
            case INTERVAL_DAY_TIME:
                fieldGetter = row -> row.getLong(fieldPos);
                break;
            case FLOAT:
                fieldGetter = row -> row.getFloat(fieldPos);
                break;
            case DOUBLE:
                fieldGetter = row -> row.getDouble(fieldPos);
                break;
            case TIMESTAMP_WITHOUT_TIME_ZONE:
            case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
                final int timestampPrecision = getPrecision(fieldType);
                fieldGetter = row -> row.getTimestamp(fieldPos, timestampPrecision);
                break;
            case TIMESTAMP_WITH_TIME_ZONE:
                throw new UnsupportedOperationException();
            case ARRAY:
                fieldGetter = row -> row.getArray(fieldPos);
                break;
            case MULTISET:
            case MAP:
                fieldGetter = row -> row.getMap(fieldPos);
                break;
            case ROW:
            case STRUCTURED_TYPE:
                final int rowFieldCount = getFieldCount(fieldType);
                fieldGetter = row -> row.getRow(fieldPos, rowFieldCount);
                break;
            case DISTINCT_TYPE:
                fieldGetter =
                        createFieldGetter(((DistinctType) fieldType).getSourceType(), fieldPos);
                break;
            case RAW:
                fieldGetter = row -> row.getRawValue(fieldPos);
                break;
            case NULL:
            case SYMBOL:
            case UNRESOLVED:
            default:
                throw new IllegalArgumentException();
        }
        if (!fieldType.isNullable()) {
            return fieldGetter;
        }
        return row -> {
            if (row.isNullAt(fieldPos)) {
                return null;
            }
            return fieldGetter.getFieldOrNull(row);
        };
    }
    @FunctionalInterface
    public interface StringToRowDataConverter extends Serializable {
        Object convert(String field);
    }

    public StringToRowDataConverter createConverter(LogicalType type) {
        return wrapIntoNullableConverter(createNotNullConverter(type));
    }

    private StringToRowDataConverter wrapIntoNullableConverter(JsonToRowDataConverters.JsonToRowDataConverter converter) {
        return jsonNode -> {
            if (jsonNode == null || jsonNode.isNull() || jsonNode.isMissingNode()) {
                return null;
            }
            try {
                return converter.convert(jsonNode);
            } catch (Throwable t) {
                if (!ignoreParseErrors) {
                    throw t;
                }
                return null;
            }
        };
    }
    /** Creates a runtime converter which assuming input object is not null. */
    private StringToRowDataConverter createNotNullConverter(LogicalType type) {
        switch (type.getTypeRoot()) {
            case NULL:
                return jsonNode -> null;
            case BOOLEAN:
                return this::convertToBoolean;
            case TINYINT:
                return jsonNode -> Byte.parseByte(jsonNode.asText().trim());
            case SMALLINT:
                return jsonNode -> Short.parseShort(jsonNode.asText().trim());
            case INTEGER:
            case INTERVAL_YEAR_MONTH:
                return this::convertToInt;
            case BIGINT:
            case INTERVAL_DAY_TIME:
                return this::convertToLong;
            case DATE:
                return this::convertToDate;
            case TIME_WITHOUT_TIME_ZONE:
                return this::convertToTime;
            case TIMESTAMP_WITHOUT_TIME_ZONE:
                return this::convertToTimestamp;
            case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
                return this::convertToTimestampWithLocalZone;
            case FLOAT:
                return this::convertToFloat;
            case DOUBLE:
                return this::convertToDouble;
            case CHAR:
            case VARCHAR:
                return this::convertToString;
            case BINARY:
            case VARBINARY:
                return this::convertToBytes;
            case DECIMAL:
                return createDecimalConverter((DecimalType) type);
            case ARRAY:
                return createArrayConverter((ArrayType) type);
            case MAP:
                MapType mapType = (MapType) type;
                return createMapConverter(
                        mapType.asSummaryString(), mapType.getKeyType(), mapType.getValueType());
            case MULTISET:
                MultisetType multisetType = (MultisetType) type;
                return createMapConverter(
                        multisetType.asSummaryString(),
                        multisetType.getElementType(),
                        new IntType());
            case ROW:
                return createRowConverter((RowType) type);
            case RAW:
            default:
                throw new UnsupportedOperationException("Unsupported type: " + type);
        }
    }

    /*static void SetFieldData(LogicalType fieldType, int fieldPos,RowData row,String field){
        switch (fieldType.getTypeRoot()) {
            case CHAR:
            case VARCHAR:
                ((BinaryRowData) row ).;
                break;
            case BOOLEAN:
                ((BinaryRowData) row ).setBoolean(fieldPos,Boolean.valueOf(field));
                break;
            case BINARY:
            case VARBINARY:
                ((BinaryRowData) row ).set;
                break;
            case DECIMAL:
                final int decimalPrecision = getPrecision(fieldType);
                final int decimalScale = getScale(fieldType);
                fieldGetter = row -> row.getDecimal(fieldPos, decimalPrecision, decimalScale);
                break;
            case TINYINT:
                fieldGetter = row -> row.getByte(fieldPos);
                break;
            case SMALLINT:
                fieldGetter = row -> row.getShort(fieldPos);
                break;
            case INTEGER:
            case DATE:
            case TIME_WITHOUT_TIME_ZONE:
            case INTERVAL_YEAR_MONTH:
                fieldGetter = row -> row.getInt(fieldPos);
                break;
            case BIGINT:
            case INTERVAL_DAY_TIME:
                fieldGetter = row -> row.getLong(fieldPos);
                break;
            case FLOAT:
                fieldGetter = row -> row.getFloat(fieldPos);
                break;
            case DOUBLE:
                fieldGetter = row -> row.getDouble(fieldPos);
                break;
            case TIMESTAMP_WITHOUT_TIME_ZONE:
            case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
                final int timestampPrecision = getPrecision(fieldType);
                fieldGetter = row -> row.getTimestamp(fieldPos, timestampPrecision);
                break;
            case TIMESTAMP_WITH_TIME_ZONE:
                throw new UnsupportedOperationException();
            case ARRAY:
                fieldGetter = row -> row.getArray(fieldPos);
                break;
            case MULTISET:
            case MAP:
                fieldGetter = row -> row.getMap(fieldPos);
                break;
            case ROW:
            case STRUCTURED_TYPE:
                final int rowFieldCount = getFieldCount(fieldType);
                fieldGetter = row -> row.getRow(fieldPos, rowFieldCount);
                break;
            case DISTINCT_TYPE:
                fieldGetter =
                        createFieldGetter(((DistinctType) fieldType).getSourceType(), fieldPos);
                break;
            case RAW:
                fieldGetter = row -> row.getRawValue(fieldPos);
                break;
            case NULL:
            case SYMBOL:
            case UNRESOLVED:
            default:
                throw new IllegalArgumentException();
        }
    }*/


    /**
     * Accessor for getting the field of a row during runtime.
     *
     * @see #createFieldGetter(LogicalType, int)
     */
    interface FieldGetter extends Serializable {
        @Nullable
        Object getFieldOrNull(RowData row);
    }


    interface FieldSetter extends Serializable {
        @Nullable
        RowData setFieldOrNull(Object field);
    }


    /**************************************************/

   /* public static RowData stringToRowData(String data ,LogicalType[] types){
        BinaryRowData result = new BinaryRowData(types.length);
        String[] rowArray = data.split("##");
        String rowKind = rowArray[0];
        result.setRowKind(RowKind.valueOf(rowKind));
        String[] rowDataArray = rowArray[1].split(",");
        for(int i = 0; i < rowDataArray.length; i++){
            if(rowDataArray[i].isEmpty()){
                result.setNullAt(i);
            }else{

            }
        }

    }*/


    /** Stringify the given {@link RowData}. */
    public static String rowDataToString(RowData row, LogicalType[] types) {
        checkArgument(types.length == row.getArity());
        StringBuilder build = new StringBuilder();
        build.append(row.getRowKind().shortString()).append("##");
        for (int i = 0; i < row.getArity(); i++) {
            if (row.isNullAt(i)) {
                build.append("null");
            } else {
                RowData.FieldGetter fieldGetter = RowData.createFieldGetter(types[i], i);
                build.append(fieldGetter.getFieldOrNull(row));
            }
            if(i != row.getArity() - 1 ){
                build.append(',');
            }
        }
        //build.append(')');
        return build.toString();
    }

    public static void main(String[] args){
        StringData str;
        RawValueData<String> generic;
        DecimalData decimal1;
        DecimalData decimal2;
        BinaryArrayData array;
        BinaryMapData map;
        BinaryRowData underRow;
        byte[] bytes;
        RawValueDataSerializer<String> genericSerializer;
        TimestampData timestamp1;
        TimestampData timestamp2;
        str = StringData.fromString("haha");
        generic = RawValueData.fromObject("haha");
        genericSerializer = new RawValueDataSerializer<>(StringSerializer.INSTANCE);
        decimal1 = DecimalData.fromUnscaledLong(10, 5, 0);
        decimal2 = DecimalData.fromBigDecimal(new BigDecimal(11), 20, 0);
        array = new BinaryArrayData();
        {
            BinaryArrayWriter arrayWriter = new BinaryArrayWriter(array, 2, 4);
            arrayWriter.writeInt(0, 15);
            arrayWriter.writeInt(1, 16);
            arrayWriter.complete();
        }
        map = BinaryMapData.valueOf(array, array);
        underRow = new BinaryRowData(2);
        {
            BinaryRowWriter writer = new BinaryRowWriter(underRow);
            writer.writeInt(0, 15);
            writer.writeInt(1, 16);
            writer.complete();
        }
        bytes = new byte[] {1, 5, 6};
        timestamp1 = TimestampData.fromEpochMillis(123L);
        timestamp2 =
                TimestampData.fromLocalDateTime(LocalDateTime.of(1969, 1, 1, 0, 0, 0, 123456789));


        GenericRowData row1 = new GenericRowData(4);
        /*row1.setRowKind(RowKind.INSERT);
        row1.setField(0, true);
        row1.setField(1, (byte) 1);
        row1.setField(2, (short) 2);
        row1.setField(3, 3);
        row1.setField(4, (long) 4);*/
        row1.setRowKind(RowKind.INSERT);
        row1.setField(0, true);
        row1.setField(1, (short) 2);
        row1.setField(2, 3);
        row1.setField(3, (long) 4);

        GenericRowData row2 = new GenericRowData(13);
        row2.setField(0, (float) 5);
        row2.setField(1, (double) 6);
        row2.setField(2, (char) 7);
        row2.setField(3, str);
        row2.setField(4, generic);
        row2.setField(5, decimal1);
        row2.setField(6, decimal2);
        row2.setField(7, array);
        row2.setField(8, map);
        row2.setField(9, underRow);
        row2.setField(10, bytes);
        row2.setField(11, timestamp1);
        row2.setField(12, timestamp2);

        LogicalType[] types = new LogicalType[]{new BooleanType(),new SmallIntType(),new IntType(),new BigIntType()};

        System.out.println(rowDataToString(row1,types));

    }
}
