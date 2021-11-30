package flink.examples.datastream._07.query._04_window;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.typeutils.ListTypeInfo;
import org.apache.flink.api.java.typeutils.MapTypeInfo;
import org.apache.flink.api.java.typeutils.TupleTypeInfo;
import org.apache.flink.core.memory.DataInputDeserializer;
import org.apache.flink.core.memory.DataOutputSerializer;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.runtime.typeutils.InternalSerializers;
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo;
import org.apache.flink.table.types.logical.*;
import org.apache.flink.types.RowKind;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class TypeSerializerTest {
    protected static DataOutputSerializer dataOutputView = new DataOutputSerializer(128);

    protected static DataInputDeserializer dataInputView= new DataInputDeserializer();
    public static void main(String[] args) throws IOException {

        //data
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

        Tuple2 tuple = new Tuple2();
        tuple.f0 = row1;
        tuple.f1 = true;

        Tuple2 tuple2 = new Tuple2();
        tuple2.f0 = row1;
        tuple2.f1 = false;

        List<Tuple2<RowData, Boolean>> list = new ArrayList<>();
        list.add(tuple);
        list.add(tuple2);

        Map<Long, List<Tuple2<RowData, Boolean>>> map = new HashMap<>();
        map.put(1l,list);

        //typeinformation
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
        //TypeSerializer<RowData> serializer = InternalSerializers.create(rowtype);
        InternalTypeInfo<RowData> leftType = InternalTypeInfo.of(rowtype);
        ListTypeInfo<Tuple2<RowData, Boolean>> leftRowListTypeInfo =
                new ListTypeInfo<>(new TupleTypeInfo<>(leftType, BasicTypeInfo.BOOLEAN_TYPE_INFO));
        MapTypeInfo<Long, List<Tuple2<RowData, Boolean>>> mapType =
                new MapTypeInfo<>(BasicTypeInfo.LONG_TYPE_INFO,leftRowListTypeInfo);

        TypeSerializer<Map<Long, List<Tuple2<RowData, Boolean>>>> ser = mapType.createSerializer(new ExecutionConfig());


        ser.serialize(map,dataOutputView);
        byte[] data = dataOutputView.getCopyOfBuffer();
        //System.out.println(data.toString());
        String str1 = new String(data);
        System.out.println(str1);
        byte[] hehe = str1.getBytes();
        //byte[] hehe =data.toString().getBytes();
        dataInputView.setBuffer(hehe);
        Map<Long, List<Tuple2<RowData, Boolean>>>  rowMap = (Map)ser.deserialize(dataInputView);

        RowData rowdata = rowMap.get(1l).get(0).getField(0);
        System.out.println(rowdata.getInt(2));


    }
}
