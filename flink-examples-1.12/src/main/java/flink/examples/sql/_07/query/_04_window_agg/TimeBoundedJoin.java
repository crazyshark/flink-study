package flink.examples.sql._07.query._04_window_agg;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.functions.ScalarFunction;
import org.apache.flink.types.Row;
import org.apache.flink.util.Collector;
import org.apache.flink.util.IOUtils;
import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.io.Serializable;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
public class TimeBoundedJoin {
    public static AssignerWithPeriodicWatermarks<Row> getWatermark(Integer maxIdleTime, long finalMaxOutOfOrderness) {
        AssignerWithPeriodicWatermarks<Row> timestampExtractor = new AssignerWithPeriodicWatermarks<Row>() {
            private long currentMaxTimestamp = 0;
            private long lastMaxTimestamp = 0;
            private long lastUpdateTime = 0;
            boolean firstWatermark = true;
            //            Integer maxIdleTime = 30;
            @Override
            public Watermark getCurrentWatermark() {
                if(firstWatermark) {
                    lastUpdateTime = System.currentTimeMillis();
                    firstWatermark = false;
                }
                if(currentMaxTimestamp != lastMaxTimestamp) {
                    lastMaxTimestamp = currentMaxTimestamp;
                    lastUpdateTime = System.currentTimeMillis();
                }
                if(maxIdleTime != null && System.currentTimeMillis() - lastUpdateTime > maxIdleTime * 1000) {
                    return new Watermark(new Date().getTime() - finalMaxOutOfOrderness * 1000);
                }
                return new Watermark(currentMaxTimestamp - finalMaxOutOfOrderness * 1000);
            }
            @Override
            public long extractTimestamp(Row row, long previousElementTimestamp) {
                Object value = row.getField(1);
                long timestamp;
                try {
                    timestamp = (long)value;
                } catch (Exception e) {
                    timestamp = ((Timestamp)value).getTime();
                }
                if(timestamp > currentMaxTimestamp) {
                    currentMaxTimestamp = timestamp;
                }
                return timestamp;
            }
        };
        return timestampExtractor;
    }
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment bsEnv = StreamExecutionEnvironment.getExecutionEnvironment();
        EnvironmentSettings bsSettings = EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build();
        StreamTableEnvironment bsTableEnv = StreamTableEnvironment.create(bsEnv, bsSettings);
        bsEnv.setParallelism(1);
        bsEnv.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
//        DataStream<Row> ds1 = bsEnv.addSource(sourceFunction(9000));
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        List<Row> list = new ArrayList<>();
        list.add(Row.of("001",new Timestamp(sdf.parse("2020-05-13 00:00:00").getTime()), 100));
        list.add(Row.of("000",new Timestamp(sdf.parse("2020-05-13 00:20:00").getTime()), 100));
        list.add(Row.of("000",new Timestamp(sdf.parse("2020-05-13 00:40:00").getTime()), 100));
        list.add(Row.of("002",new Timestamp(sdf.parse("2020-05-13 01:00:01").getTime()), 100));
        list.add(Row.of("000",new Timestamp(sdf.parse("2020-05-13 02:20:00").getTime()), 100));
        list.add(Row.of("000",new Timestamp(sdf.parse("2020-05-13 02:30:00").getTime()), 100));
        list.add(Row.of("003",new Timestamp(sdf.parse("2020-05-13 02:00:02").getTime()), 100));
        list.add(Row.of("000",new Timestamp(sdf.parse("2020-05-13 02:20:00").getTime()), 100));
        list.add(Row.of("000",new Timestamp(sdf.parse("2020-05-13 02:40:00").getTime()), 100));
        list.add(Row.of("004",new Timestamp(sdf.parse("2020-05-13 03:00:03").getTime()), 100));
        list.add(Row.of("000",new Timestamp(sdf.parse("2020-05-13 03:20:00").getTime()), 100));
        list.add(Row.of("000",new Timestamp(sdf.parse("2020-05-13 03:40:00").getTime()), 100));
        list.add(Row.of("005",new Timestamp(sdf.parse("2020-05-13 04:00:04").getTime()), 100));
        DataStream<Row> ds1 = bsEnv.addSource(new SourceFunction<Row>() {
            @Override
            public void run(SourceContext<Row> ctx) throws Exception {
                for(Row row : list) {
                    ctx.collect(row);
                    Thread.sleep(1000);
                }
            }
            @Override
            public void cancel() {
            }
        });
        ds1 = ds1.assignTimestampsAndWatermarks(getWatermark(null, 0));
        ds1.getTransformation().setOutputType((new RowTypeInfo(Types.STRING, Types.SQL_TIMESTAMP, Types.INT)));
        bsTableEnv.createTemporaryView("order_info", ds1, "order_id, order_time, fee, rowtime.rowtime");
        List<Row> list2 = new ArrayList<>();
        list2.add(Row.of("001",new Timestamp(sdf.parse("2020-05-13 01:00:00").getTime())));
        list2.add(Row.of("111",new Timestamp(sdf.parse("2020-05-13 01:20:00").getTime())));
        list2.add(Row.of("111",new Timestamp(sdf.parse("2020-05-13 01:30:00").getTime())));
        list2.add(Row.of("002",new Timestamp(sdf.parse("2020-05-13 02:00:00").getTime())));
        list2.add(Row.of("111",new Timestamp(sdf.parse("2020-05-13 02:20:00").getTime())));
        list2.add(Row.of("111",new Timestamp(sdf.parse("2020-05-13 02:40:00").getTime())));
//        list2.add(Row.of("003",new Timestamp(sdf.parse("2020-05-13 03:00:03").getTime())));
        list2.add(Row.of("111",new Timestamp(sdf.parse("2020-05-13 03:20:00").getTime())));
        list2.add(Row.of("111",new Timestamp(sdf.parse("2020-05-13 03:40:00").getTime())));
        list2.add(Row.of("004",new Timestamp(sdf.parse("2020-05-13 04:00:00").getTime())));
        list2.add(Row.of("111",new Timestamp(sdf.parse("2020-05-13 04:20:00").getTime())));
        list2.add(Row.of("111",new Timestamp(sdf.parse("2020-05-13 04:40:00").getTime())));
        list2.add(Row.of("005",new Timestamp(sdf.parse("2020-05-13 05:00:00").getTime())));
        list2.add(Row.of("111",new Timestamp(sdf.parse("2020-05-13 05:20:00").getTime())));
        list2.add(Row.of("111",new Timestamp(sdf.parse("2020-05-13 05:40:00").getTime())));
        DataStream<Row> ds2 = bsEnv.addSource(new SourceFunction<Row>() {
            @Override
            public void run(SourceContext<Row> ctx) throws Exception {
                for(Row row : list2) {
                    ctx.collect(row);
                    Thread.sleep(1000);
                }
            }
            @Override
            public void cancel() {
            }
        });
        String sinkSql = "CREATE TABLE sink_table (\n"
                + "    order_id STRING,\n"
                + "    order_time TIMESTAMP,\n"
                + "    fee BIGINT,\n"
                + "    rowtime TIMESTAMP,\n"
                + "    order_id2 STRING\n"
                + ") WITH (\n"
                + "  'connector' = 'print'\n"
                + ")";
        bsTableEnv.executeSql(sinkSql);
        ds2 = ds2.assignTimestampsAndWatermarks(getWatermark(null, 0));
        ds2.getTransformation().setOutputType((new RowTypeInfo(Types.STRING, Types.SQL_TIMESTAMP)));
        bsTableEnv.createTemporaryView("pay", ds2, "order_id, pay_time, rowtime.rowtime");
        Table joinTable =  bsTableEnv.sqlQuery("SELECT a.*,b.order_id from order_info a left join pay b on a.order_id=b.order_id and b.rowtime between a.rowtime and a.rowtime + INTERVAL '1' HOUR where a.order_id <>'000' ");
        bsTableEnv.toAppendStream(joinTable, Row.class).process(new ProcessFunction<Row, Object>() {
            @Override
            public void processElement(Row value, Context ctx, Collector<Object> out) throws Exception {
                SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
                System.err.println("row:" + value + ",rowtime:" + value.getField(3) + ",watermark:" + sdf.format(ctx.timerService().currentWatermark()));
            }
        });
        //bsTableEnv.execute("job");
        //System.out.println(bsEnv.getExecutionPlan());
        bsEnv.execute();
        /*joinTable.insertInto("sink_table");
        bsTableEnv.execute("job");*/
    }
}