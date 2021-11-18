package flink.examples.sql._07.query._04_window_agg;

import com.ibm.icu.util.CodePointTrie;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.base.StringSerializer;
import org.apache.flink.core.memory.DataInputDeserializer;
import org.apache.flink.core.memory.DataOutputSerializer;
import org.apache.flink.formats.json.JsonToRowDataConverters;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.ArrayNode;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.TextNode;
import org.apache.flink.table.api.TableException;
import org.apache.flink.table.data.*;
import org.apache.flink.table.data.binary.BinaryArrayData;
import org.apache.flink.table.data.binary.BinaryMapData;
import org.apache.flink.table.data.binary.BinaryRowData;
import org.apache.flink.table.data.writer.BinaryArrayWriter;
import org.apache.flink.table.data.writer.BinaryRowWriter;
import org.apache.flink.table.runtime.typeutils.InternalSerializers;
import org.apache.flink.table.runtime.typeutils.RawValueDataSerializer;
import org.apache.flink.table.types.logical.*;
import org.apache.flink.table.types.logical.utils.LogicalTypeChecks;
import org.apache.flink.table.types.logical.utils.LogicalTypeUtils;
import org.apache.flink.types.RowKind;

import javax.annotation.Nullable;
import java.io.IOException;
import java.io.Serializable;
import java.lang.reflect.Array;
import java.math.BigDecimal;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.ZoneOffset;
import java.time.temporal.TemporalAccessor;
import java.time.temporal.TemporalQueries;
import java.util.*;

import static java.time.format.DateTimeFormatter.ISO_LOCAL_DATE;
import static org.apache.flink.table.types.logical.utils.LogicalTypeChecks.*;
import static org.apache.flink.util.Preconditions.checkArgument;

public class RowDataUtil {

    protected static  DataOutputSerializer dataOutputView = new DataOutputSerializer(128);

    protected static DataInputDeserializer dataInputView= new DataInputDeserializer();

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


    /**
     * Accessor for getting the field of a row during runtime.
     *
     * @see #createFieldGetter(LogicalType, int)
     */
    interface FieldGetter extends Serializable {
        @Nullable
        Object getFieldOrNull(RowData row);
    }


    //////////////////////////////////////////////////////////////////////////


    @FunctionalInterface
    public interface StringToRowDataConverter extends Serializable {
        Object convert(String field);
    }

    public StringToRowDataConverter createConverter(LogicalType type) {
        return wrapIntoNullableConverter(createNotNullConverter(type));
    }

    private StringToRowDataConverter wrapIntoNullableConverter(StringToRowDataConverter converter) {
        return dataStr -> {
            if (dataStr == null || dataStr.isEmpty()) {
                return null;
            }
            try {
                return converter.convert(dataStr);
            } catch (Throwable t) {

                return null;
            }
        };
    }
    /** Creates a runtime converter which assuming input object is not null. */
    private StringToRowDataConverter createNotNullConverter(LogicalType type) {
        switch (type.getTypeRoot()) {
            case NULL:
                return dataStr -> null;
            case BOOLEAN:
                return this::convertToBoolean;
            case TINYINT:
                return dataStr -> Byte.parseByte(dataStr.trim());
            case SMALLINT:
                return dataStr -> Short.parseShort(dataStr.trim());
            case INTEGER:
            case INTERVAL_YEAR_MONTH:
                return this::convertToInt;
            case BIGINT:
            case INTERVAL_DAY_TIME:
                return this::convertToLong;
            case DATE:
                return this::convertToDate;
            /*case TIME_WITHOUT_TIME_ZONE:
                return this::convertToTime;
            case TIMESTAMP_WITHOUT_TIME_ZONE:
                return this::convertToTimestamp;
            case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
                return this::convertToTimestampWithLocalZone;*/
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
            /*case DECIMAL:
                return createDecimalConverter((DecimalType) type);*/
            /*case ARRAY:
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
                        new IntType());*/
            case ROW:
                return createRowConverter((RowType) type);
            case RAW:
            default:
                throw new UnsupportedOperationException("Unsupported type: " + type);
        }
    }


    private boolean convertToBoolean(String  dataStr) {

        return Boolean.parseBoolean(dataStr.trim());

    }

    private int convertToInt(String  dataStr) {

        return Integer.parseInt(dataStr.trim());

    }

    private long convertToLong(String dataStr) {

        return Long.parseLong(dataStr.trim());

    }

    private double convertToDouble(String dataStr) {

        return Double.parseDouble(dataStr.trim());

    }

    private float convertToFloat(String dataStr) {

        return Float.parseFloat(dataStr.trim());
    }

    private int convertToDate(String dataStr) {
        LocalDate date = ISO_LOCAL_DATE.parse(dataStr).query(TemporalQueries.localDate());
        return (int) date.toEpochDay();
    }

    /*private int convertToTime(String dataStr) {
        TemporalAccessor parsedTime = SQL_TIME_FORMAT.parse(dataStr);
        LocalTime localTime = parsedTime.query(TemporalQueries.localTime());

        // get number of milliseconds of the day
        return localTime.toSecondOfDay() * 1000;
    }

    private TimestampData convertToTimestamp(JsonNode jsonNode) {
        TemporalAccessor parsedTimestamp;
        switch (timestampFormat) {
            case SQL:
                parsedTimestamp = SQL_TIMESTAMP_FORMAT.parse(jsonNode.asText());
                break;
            case ISO_8601:
                parsedTimestamp = ISO8601_TIMESTAMP_FORMAT.parse(jsonNode.asText());
                break;
            default:
                throw new TableException(
                        String.format(
                                "Unsupported timestamp format '%s'. Validator should have checked that.",
                                timestampFormat));
        }
        LocalTime localTime = parsedTimestamp.query(TemporalQueries.localTime());
        LocalDate localDate = parsedTimestamp.query(TemporalQueries.localDate());

        return TimestampData.fromLocalDateTime(LocalDateTime.of(localDate, localTime));
    }

    private TimestampData convertToTimestampWithLocalZone(JsonNode jsonNode) {
        TemporalAccessor parsedTimestampWithLocalZone;
        switch (timestampFormat) {
            case SQL:
                parsedTimestampWithLocalZone =
                        SQL_TIMESTAMP_WITH_LOCAL_TIMEZONE_FORMAT.parse(jsonNode.asText());
                break;
            case ISO_8601:
                parsedTimestampWithLocalZone =
                        ISO8601_TIMESTAMP_WITH_LOCAL_TIMEZONE_FORMAT.parse(jsonNode.asText());
                break;
            default:
                throw new TableException(
                        String.format(
                                "Unsupported timestamp format '%s'. Validator should have checked that.",
                                timestampFormat));
        }
        LocalTime localTime = parsedTimestampWithLocalZone.query(TemporalQueries.localTime());
        LocalDate localDate = parsedTimestampWithLocalZone.query(TemporalQueries.localDate());

        return TimestampData.fromInstant(
                LocalDateTime.of(localDate, localTime).toInstant(ZoneOffset.UTC));
    }*/

    private StringData convertToString(String dataStr) {

        return StringData.fromString(dataStr);

    }

    private byte[] convertToBytes(String dataStr) {

        return dataStr.getBytes();

    }

    private JsonToRowDataConverters.JsonToRowDataConverter createDecimalConverter(DecimalType decimalType) {
        final int precision = decimalType.getPrecision();
        final int scale = decimalType.getScale();
        return dataStr -> {
            BigDecimal bigDecimal;

            bigDecimal = new BigDecimal(dataStr.toString());

            return DecimalData.fromBigDecimal(bigDecimal, precision, scale);
        };
    }

    /*private JsonToRowDataConverters.JsonToRowDataConverter createArrayConverter(ArrayType arrayType) {
        JsonToRowDataConverters.JsonToRowDataConverter elementConverter = createConverter(arrayType.getElementType());
        final Class<?> elementClass =
                LogicalTypeUtils.toInternalConversionClass(arrayType.getElementType());
        return jsonNode -> {
            final ArrayNode node = (ArrayNode) jsonNode;
            final Object[] array = (Object[]) Array.newInstance(elementClass, node.size());
            for (int i = 0; i < node.size(); i++) {
                final JsonNode innerNode = node.get(i);
                array[i] = elementConverter.convert(innerNode);
            }
            return new GenericArrayData(array);
        };
    }*/

/*    private JsonToRowDataConverters.JsonToRowDataConverter createMapConverter(
            String typeSummary, LogicalType keyType, LogicalType valueType) {
        if (!LogicalTypeChecks.hasFamily(keyType, LogicalTypeFamily.CHARACTER_STRING)) {
            throw new UnsupportedOperationException(
                    "JSON format doesn't support non-string as key type of map. "
                            + "The type is: "
                            + typeSummary);
        }
        final JsonToRowDataConverters.JsonToRowDataConverter keyConverter = createConverter(keyType);
        final JsonToRowDataConverters.JsonToRowDataConverter valueConverter = createConverter(valueType);

        return jsonNode -> {
            Iterator<Map.Entry<String, JsonNode>> fields = jsonNode.fields();
            Map<Object, Object> result = new HashMap<>();
            while (fields.hasNext()) {
                Map.Entry<String, JsonNode> entry = fields.next();
                Object key = keyConverter.convert(TextNode.valueOf(entry.getKey()));
                Object value = valueConverter.convert(entry.getValue());
                result.put(key, value);
            }
            return new GenericMapData(result);
        };
    }*/

    public StringToRowDataConverter createRowConverter(RowType rowType) {
        final StringToRowDataConverter[] fieldConverters =
                rowType.getFields().stream()
                        .map(RowType.RowField::getType)
                        .map(this::createConverter)
                        .toArray(StringToRowDataConverter[]::new);
        final String[] fieldNames = rowType.getFieldNames().toArray(new String[0]);

        return dataStr -> {
            int arity = fieldNames.length;
            GenericRowData row = new GenericRowData(arity);
            String[] rowArray = dataStr.split("##");
            //String rowKind = rowArray[0];
            RowKind rowKind = getRowKind(rowArray[0]);
            row.setRowKind(rowKind);
            String[] rowDataArray = rowArray[1].split(",");

            for (int i = 0; i < arity; i++) {
                //String fieldName = fieldNames[i];
                String field = rowDataArray[i];
                Object convertedField = convertField(fieldConverters[i], field);
                row.setField(i, convertedField);
            }
            return row;
        };
    }

    public RowKind getRowKind(String rowKind){

        switch(rowKind){
            case "+I":
                return RowKind.INSERT;
            case "-U":
                return RowKind.UPDATE_BEFORE;
            case "+U":
                return RowKind.UPDATE_AFTER;
            case "-D":
                return RowKind.DELETE;
            default:
                return null;

        }

    }

    private Object convertField(
            StringToRowDataConverter fieldConverter, String field) {
        if (field == null) {
            return null;
        } else {
            return fieldConverter.convert(field);
        }
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
    public  String rowDataToString(RowData row, LogicalType[] types) {
        //checkArgument(types.length == row.getArity());
        StringBuilder build = new StringBuilder();
        build.append(row.getRowKind().shortString()).append("##");
        for (int i = 0; i < row.getArity(); i++) {
            if (row.isNullAt(i)) {
                build.append("null");
            } else {
                FieldGetter fieldGetter = createFieldGetter(types[i], i);
                build.append(fieldGetter.getFieldOrNull(row));
            }
            if(i != row.getArity() - 1 ){
                build.append(',');
            }
        }
        //build.append(')');
        return build.toString();
    }

    public static void main(String[] args) throws IOException {
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

        /*LogicalType[] types = new LogicalType[]{new BooleanType(),new SmallIntType(),new IntType(),new BigIntType()};
        RowDataUtil util = new RowDataUtil();
        System.out.println(util.rowDataToString(row1,types));

        RowType.RowField field1 = new RowType.RowField("sex",new BooleanType());
        RowType.RowField field2 = new RowType.RowField("age",new SmallIntType());
        RowType.RowField field3 = new RowType.RowField("weight",new IntType());
        RowType.RowField field4 = new RowType.RowField("num",new BigIntType());
        List<RowType.RowField> fieldList = new ArrayList<>();
        fieldList.add(field1);
        fieldList.add(field2);
        fieldList.add(field3);
        fieldList.add(field4);
        RowType rowtype = new RowType(fieldList);
        LogicalType[] types2 = rowtype.getChildren().toArray(new LogicalType[rowtype.getChildren().size()]);

        StringToRowDataConverter convert = util.createConverter(rowtype);
        String dataStr = "+I##true,2,3,4";
        RowData row = (RowData) convert.convert(dataStr);
        System.out.println(util.rowDataToString(row,types2));*/
        RowType.RowField field1 = new RowType.RowField("sex",new BooleanType());
        RowType.RowField field2 = new RowType.RowField("age",new SmallIntType());
        RowType.RowField field3 = new RowType.RowField("weight",new IntType());
        RowType.RowField field4 = new RowType.RowField("num",new BigIntType());
        List<RowType.RowField> fieldList = new ArrayList<>();
        fieldList.add(field1);
        fieldList.add(field2);
        fieldList.add(field3);
        fieldList.add(field4);
        RowType rowtype = new RowType(fieldList);
        TypeSerializer<RowData> serializer = InternalSerializers.create(rowtype);
        serializer.serialize(row1,dataOutputView);
        byte[] data = dataOutputView.getCopyOfBuffer();
        System.out.println(data.toString());
        String str1 = new String(data);
        System.out.println(str1);
        byte[] hehe = str1.getBytes();
        //byte[] hehe =data.toString().getBytes();
        dataInputView.setBuffer(hehe);
        RowData row = serializer.deserialize(dataInputView);
        System.out.println(row.getInt(2));



    }
}
