package flink.examples.sql._07.query._04_window_agg;


public final class _04_TumbleWindowTest_WindowValueEqualiser$60 implements org.apache.flink.table.runtime.generated.RecordEqualiser {



    public _04_TumbleWindowTest_WindowValueEqualiser$60(Object[] references) throws Exception {

    }

    @Override
    public boolean equals(org.apache.flink.table.data.RowData left, org.apache.flink.table.data.RowData right) {
        if (left instanceof org.apache.flink.table.data.binary.BinaryRowData && right instanceof org.apache.flink.table.data.binary.BinaryRowData) {
            return left.equals(right);
        } else {

            if (left.getRowKind() != right.getRowKind()) {
                return false;
            }

            boolean isNull$61;
            boolean result$62;
            boolean isNull$63;
            boolean result$64;
            boolean isNull$65;
            boolean result$66;
            boolean isNull$67;
            boolean result$68;

            boolean leftIsNull$0 = left.isNullAt(0);
            boolean rightIsNull$0 = right.isNullAt(0);
            boolean cmp0;
            if (leftIsNull$0 && rightIsNull$0) {
                cmp0 = true;
            } else if (leftIsNull$0|| rightIsNull$0) {
                cmp0 = false;
            } else {
                long leftField$0 = left.getLong(0);
                long rightField$0 = right.getLong(0);

                cmp0 = leftField$0 == rightField$0;
            }
            if (!cmp0) {
                return false;
            }


            boolean leftIsNull$1 = left.isNullAt(1);
            boolean rightIsNull$1 = right.isNullAt(1);
            boolean cmp1;
            if (leftIsNull$1 && rightIsNull$1) {
                cmp1 = true;
            } else if (leftIsNull$1|| rightIsNull$1) {
                cmp1 = false;
            } else {
                long leftField$1 = left.getLong(1);
                long rightField$1 = right.getLong(1);

                cmp1 = leftField$1 == rightField$1;
            }
            if (!cmp1) {
                return false;
            }


            boolean leftIsNull$2 = left.isNullAt(2);
            boolean rightIsNull$2 = right.isNullAt(2);
            boolean cmp2;
            if (leftIsNull$2 && rightIsNull$2) {
                cmp2 = true;
            } else if (leftIsNull$2|| rightIsNull$2) {
                cmp2 = false;
            } else {
                long leftField$2 = left.getLong(2);
                long rightField$2 = right.getLong(2);

                cmp2 = leftField$2 == rightField$2;
            }
            if (!cmp2) {
                return false;
            }


            boolean leftIsNull$3 = left.isNullAt(3);
            boolean rightIsNull$3 = right.isNullAt(3);
            boolean cmp3;
            if (leftIsNull$3 && rightIsNull$3) {
                cmp3 = true;
            } else if (leftIsNull$3|| rightIsNull$3) {
                cmp3 = false;
            } else {
                long leftField$3 = left.getLong(3);
                long rightField$3 = right.getLong(3);

                cmp3 = leftField$3 == rightField$3;
            }
            if (!cmp3) {
                return false;
            }


            boolean leftIsNull$4 = left.isNullAt(4);
            boolean rightIsNull$4 = right.isNullAt(4);
            boolean cmp4;
            if (leftIsNull$4 && rightIsNull$4) {
                cmp4 = true;
            } else if (leftIsNull$4|| rightIsNull$4) {
                cmp4 = false;
            } else {
                long leftField$4 = left.getLong(4);
                long rightField$4 = right.getLong(4);

                cmp4 = leftField$4 == rightField$4;
            }
            if (!cmp4) {
                return false;
            }


            boolean leftIsNull$5 = left.isNullAt(5);
            boolean rightIsNull$5 = right.isNullAt(5);
            boolean cmp5;
            if (leftIsNull$5 && rightIsNull$5) {
                cmp5 = true;
            } else if (leftIsNull$5|| rightIsNull$5) {
                cmp5 = false;
            } else {
                org.apache.flink.table.data.TimestampData leftField$5 = left.getTimestamp(5, 3);
                org.apache.flink.table.data.TimestampData rightField$5 = right.getTimestamp(5, 3);



                isNull$61 = leftIsNull$5 || rightIsNull$5;
                result$62 = false;
                if (!isNull$61) {

                    result$62 = leftField$5.compareTo(rightField$5) == 0;

                }

                cmp5 = result$62;
            }
            if (!cmp5) {
                return false;
            }


            boolean leftIsNull$6 = left.isNullAt(6);
            boolean rightIsNull$6 = right.isNullAt(6);
            boolean cmp6;
            if (leftIsNull$6 && rightIsNull$6) {
                cmp6 = true;
            } else if (leftIsNull$6|| rightIsNull$6) {
                cmp6 = false;
            } else {
                org.apache.flink.table.data.TimestampData leftField$6 = left.getTimestamp(6, 3);
                org.apache.flink.table.data.TimestampData rightField$6 = right.getTimestamp(6, 3);



                isNull$63 = leftIsNull$6 || rightIsNull$6;
                result$64 = false;
                if (!isNull$63) {

                    result$64 = leftField$6.compareTo(rightField$6) == 0;

                }

                cmp6 = result$64;
            }
            if (!cmp6) {
                return false;
            }


            boolean leftIsNull$7 = left.isNullAt(7);
            boolean rightIsNull$7 = right.isNullAt(7);
            boolean cmp7;
            if (leftIsNull$7 && rightIsNull$7) {
                cmp7 = true;
            } else if (leftIsNull$7|| rightIsNull$7) {
                cmp7 = false;
            } else {
                org.apache.flink.table.data.TimestampData leftField$7 = left.getTimestamp(7, 3);
                org.apache.flink.table.data.TimestampData rightField$7 = right.getTimestamp(7, 3);



                isNull$65 = leftIsNull$7 || rightIsNull$7;
                result$66 = false;
                if (!isNull$65) {

                    result$66 = leftField$7.compareTo(rightField$7) == 0;

                }

                cmp7 = result$66;
            }
            if (!cmp7) {
                return false;
            }


            boolean leftIsNull$8 = left.isNullAt(8);
            boolean rightIsNull$8 = right.isNullAt(8);
            boolean cmp8;
            if (leftIsNull$8 && rightIsNull$8) {
                cmp8 = true;
            } else if (leftIsNull$8|| rightIsNull$8) {
                cmp8 = false;
            } else {
                org.apache.flink.table.data.TimestampData leftField$8 = left.getTimestamp(8, 3);
                org.apache.flink.table.data.TimestampData rightField$8 = right.getTimestamp(8, 3);



                isNull$67 = leftIsNull$8 || rightIsNull$8;
                result$68 = false;
                if (!isNull$67) {

                    result$68 = leftField$8.compareTo(rightField$8) == 0;

                }

                cmp8 = result$68;
            }
            if (!cmp8) {
                return false;
            }

            return true;
        }
    }
}
