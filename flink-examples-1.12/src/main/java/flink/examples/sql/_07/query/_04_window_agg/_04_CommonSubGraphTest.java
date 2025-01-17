package flink.examples.sql._07.query._04_window_agg;

import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.StatementSet;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import java.util.concurrent.TimeUnit;


public class _04_CommonSubGraphTest {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env =
                StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(new Configuration());

        ParameterTool parameterTool = ParameterTool.fromArgs(args);

        env.setRestartStrategy(RestartStrategies.failureRateRestart(6, org.apache.flink.api.common.time.Time
                .of(10L, TimeUnit.MINUTES), org.apache.flink.api.common.time.Time.of(5L, TimeUnit.SECONDS)));
        env.getConfig().setGlobalJobParameters(parameterTool);
        env.setParallelism(1);

        // ck 设置
        env.getCheckpointConfig().setFailOnCheckpointingErrors(false);
        env.enableCheckpointing(30 * 1000L, CheckpointingMode.EXACTLY_ONCE);
        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(3L);
        env.getCheckpointConfig().enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);

        EnvironmentSettings settings = EnvironmentSettings
                .newInstance()
                .useBlinkPlanner()
                .inStreamingMode().build();

        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env, settings);

        tEnv.createTemporarySystemFunction("SplitFunction", SplitFunction.class);
        tEnv.createTemporarySystemFunction("HashFunction", HashFunction.class);

        String sourceSql = "CREATE TABLE source_table (\n"
                + "    dim STRING,\n"
                + "    click BIGINT,\n"
                + "    price BIGINT,\n"
                + "    row_time AS cast(CURRENT_TIMESTAMP as timestamp(3)),\n"
                + "    WATERMARK FOR row_time AS row_time - INTERVAL '5' SECOND\n"
                + ") WITH (\n"
                + "  'connector' = 'datagen',\n"
                + "  'rows-per-second' = '10',\n"
                + "  'fields.dim.length' = '1',\n"
                + "  'fields.click.min' = '1',\n"
                + "  'fields.click.max' = '100000',\n"
                + "  'fields.price.min' = '1',\n"
                + "  'fields.price.max' = '100000'\n"
                + ")";

        /*String sinkSql = "CREATE TABLE sink_table (\n"
                + "    dim STRING,\n"
                + "    pv BIGINT,\n"
                + "    sum_price BIGINT,\n"
                + "    max_price BIGINT,\n"
                + "    min_price BIGINT,\n"
                + "    uv BIGINT,\n"
                + "    window_start bigint,\n"
                + "    hehe BIGINT\n"
                + ") WITH (\n"
                + "  'connector' = 'print'\n"
                + ")";*/
        String sinkSql = "CREATE TABLE sink_table (\n"
                + "    dim STRING,\n"
                + "    pv BIGINT \n"
                + ") WITH (\n"
                + "  'connector' = 'print'\n"
                + ")";
        String sinkSql2 = "CREATE TABLE sink_table2 (\n"
                + "    dim STRING,\n"
                + "    sum_price BIGINT\n"
                + ") WITH (\n"
                + "  'connector' = 'print'\n"
                + ")";

        String sinkSql3 = "CREATE TABLE sink_table3 (\n"
                + "    dim STRING,\n"
                + "    pv BIGINT,\n"
                + "    sum_price BIGINT\n"
                + ") WITH (\n"
                + "  'connector' = 'print'\n"
                + ")";

        /*String selectWhereSql = "insert into sink_table\n"
                + "select dim,\n"
                + "       sum(bucket_pv) as pv,\n"
                + "       sum(bucket_sum_price) as sum_price,\n"
                + "       max(bucket_max_price) as max_price,\n"
                + "       min(bucket_min_price) as min_price,\n"
                + "       sum(bucket_uv) as uv,\n"
                + "       max(window_start) as window_start,\n"
                + "         max(hehe) as hehe \n"
                + "from (\n"
                + "     select dim,\n"
                + "            count(*) as bucket_pv,\n"
                + "            sum(price) as bucket_sum_price,\n"
                + "            max(price) as bucket_max_price,\n"
                + "            min(price) as bucket_min_price,\n"
                + "            count(distinct user_id) as bucket_uv,\n"
                + "            cast(tumble_start(row_time, interval '1' minute) as bigint) * 1000 as window_start,\n"
                + "            HashFunction(price) as hehe \n"
                + "     from source_table\n"
                + "     group by\n"
                + "            mod(user_id, 1024),\n"
                + "            dim,\n"
                + "            tumble(row_time, interval '1' minute),price\n"
                + ")\n"
                + "group by dim,\n"
                + "         window_start";*/
        String selectWhereSql ="INSERT INTO sink_table\n" +
                "SELECT dim, Sum(1) \n" +
                "FROM source_table\n" +
                "where price > 35 group by dim";

        String selectWhereSql2 ="INSERT INTO sink_table2\n" +
                "SELECT dim, MAX(price) \n" +
                "FROM source_table\n" +
                "where price > 35 group by dim";
        String selectWhereSql3 ="INSERT INTO sink_table3\n" +
                "SELECT dim, " +
                "sum(case when price >35 then 1 else 0 end), " +
                "max(case when price >35 then price else 0 end) \n" +
                "FROM source_table group by dim";

        tEnv.getConfig().getConfiguration().setString("pipeline.name", "1.12.1 TUMBLE WINDOW 案例");

        tEnv.executeSql(sourceSql);
        tEnv.executeSql(sinkSql);
        tEnv.executeSql(sinkSql2);
        tEnv.executeSql(sinkSql3);
        //tEnv.executeSql(selectWhereSql);
        //tEnv.executeSql(selectWhereSql2);
        StatementSet set = tEnv.createStatementSet();
        set.addInsertSql(selectWhereSql);
        set.addInsertSql(selectWhereSql2);
        //System.out.println(set.explain());
        set.execute();
        //tEnv.executeSql(selectWhereSql3);
        tEnv.execute("localTest");
    }

}
