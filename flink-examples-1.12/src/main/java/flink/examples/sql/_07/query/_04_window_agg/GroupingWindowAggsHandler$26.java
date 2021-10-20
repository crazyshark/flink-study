package flink.examples.sql._07.query._04_window_agg;

public final class GroupingWindowAggsHandler$26
        implements org.apache.flink.table.runtime.generated.NamespaceAggsHandleFunction<org.apache.flink.table.runtime.operators.window.TimeWindow> {

    org.apache.flink.table.data.GenericRowData acc$23 = new org.apache.flink.table.data.GenericRowData(0);
    org.apache.flink.table.data.GenericRowData acc$24 = new org.apache.flink.table.data.GenericRowData(0);
    org.apache.flink.table.data.GenericRowData aggValue$25 = new org.apache.flink.table.data.GenericRowData(4);

    private org.apache.flink.table.runtime.dataview.StateDataViewStore store;

    private org.apache.flink.table.runtime.operators.window.TimeWindow namespace;

    public GroupingWindowAggsHandler$26(Object[] references) throws Exception {

    }

    private org.apache.flink.api.common.functions.RuntimeContext getRuntimeContext() {
        return store.getRuntimeContext();
    }

    @Override
    public void open(org.apache.flink.table.runtime.dataview.StateDataViewStore store) throws Exception {
        this.store = store;

    }

    @Override
    public void accumulate(org.apache.flink.table.data.RowData accInput) throws Exception {





    }

    @Override
    public void retract(org.apache.flink.table.data.RowData retractInput) throws Exception {

        throw new java.lang.RuntimeException("This function not require retract method, but the retract method is called.");

    }

    @Override
    public void merge(org.apache.flink.table.runtime.operators.window.TimeWindow ns, org.apache.flink.table.data.RowData otherAcc) throws Exception {
        namespace = (org.apache.flink.table.runtime.operators.window.TimeWindow) ns;

        throw new java.lang.RuntimeException("This function not require merge method, but the merge method is called.");

    }

    @Override
    public void setAccumulators(org.apache.flink.table.runtime.operators.window.TimeWindow ns, org.apache.flink.table.data.RowData acc)
            throws Exception {
        namespace = (org.apache.flink.table.runtime.operators.window.TimeWindow) ns;





    }

    @Override
    public org.apache.flink.table.data.RowData getAccumulators() throws Exception {



        acc$24 = new org.apache.flink.table.data.GenericRowData(0);


        return acc$24;

    }

    @Override
    public org.apache.flink.table.data.RowData createAccumulators() throws Exception {



        acc$23 = new org.apache.flink.table.data.GenericRowData(0);


        return acc$23;

    }

    @Override
    public org.apache.flink.table.data.RowData getValue(org.apache.flink.table.runtime.operators.window.TimeWindow ns) throws Exception {
        namespace = (org.apache.flink.table.runtime.operators.window.TimeWindow) ns;



        aggValue$25 = new org.apache.flink.table.data.GenericRowData(4);


        if (false) {
            aggValue$25.setField(0, null);
        } else {
            aggValue$25.setField(0, org.apache.flink.table.data.TimestampData.fromEpochMillis(namespace.getStart()));
        }



        if (false) {
            aggValue$25.setField(1, null);
        } else {
            aggValue$25.setField(1, org.apache.flink.table.data.TimestampData.fromEpochMillis(namespace.getEnd()));
        }



        if (false) {
            aggValue$25.setField(2, null);
        } else {
            aggValue$25.setField(2, org.apache.flink.table.data.TimestampData.fromEpochMillis(namespace.getEnd() - 1));
        }



        if (true) {
            aggValue$25.setField(3, null);
        } else {
            aggValue$25.setField(3, org.apache.flink.table.data.TimestampData.fromEpochMillis(-1L));
        }


        return aggValue$25;

    }

    @Override
    public void cleanup(org.apache.flink.table.runtime.operators.window.TimeWindow ns) throws Exception {
        namespace = (org.apache.flink.table.runtime.operators.window.TimeWindow) ns;


    }

    @Override
    public void close() throws Exception {

    }
}
