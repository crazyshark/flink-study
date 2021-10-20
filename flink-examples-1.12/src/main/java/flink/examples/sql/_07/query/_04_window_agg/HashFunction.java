package flink.examples.sql._07.query._04_window_agg;

import org.apache.flink.table.annotation.DataTypeHint;
import org.apache.flink.table.annotation.InputGroup;
import org.apache.flink.table.api.*;
import org.apache.flink.table.functions.ScalarFunction;
import static org.apache.flink.table.api.Expressions.*;

public class HashFunction extends ScalarFunction {

    // 接受任意类型输入，返回 INT 型输出
    public int eval(@DataTypeHint(inputGroup = InputGroup.ANY) Object o) {
        return o.hashCode();
    }
}