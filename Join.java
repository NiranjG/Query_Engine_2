package org.example;

import java.io.IOException;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;

public class Join{
    private final Batch left;
    private final Batch right;
    private final String column_name;
    //private final int Batch_size;

    public Join(Batch left, Batch right, String column_name) {
        this.left = left;
        this.right = right;
        this.column_name = column_name;
    }

    public Batch join() throws IOException {
        if((left==null)||(left.getSize()==0)){
            return right;
        }
        if((right==null)||(right.getSize()==0)){
            Print.print(left);
            return left;
        }
        Map<String,ColumnVector> outputCols = new LinkedHashMap<>();
        Batch result = new Batch();
        for (String col : left.get_column_names()) {
            boolean isStr = left.get_columnar(col).isStringcol();
            outputCols.put("left_" + col, new ColumnVector(left.getSize(), isStr));
        }
        for (String col : right.get_column_names()) {
            boolean isStr = right.get_columnar(col).isStringcol();
            outputCols.put("right_" + col, new ColumnVector(right.getSize(), isStr));
        }

        for (int i = 0; i < left.getSize(); i++) {
            for (int j = 0; j < right.getSize(); j++) {
                if (rowsMatch(left, right, i, j)) {
                    for (String col : left.get_column_names()) {
                        ColumnVector src = left.get_columnar(col);
                        ColumnVector dest = outputCols.get("left_" + col);
                        if (src.isStringcol()) {
                            dest.append(src.string_get_index(i));
                        } else {
                            dest.append(src.double_get_index(i));
                        }
                    }
                    for (String col : right.get_column_names()) {
                        ColumnVector src = right.get_columnar(col);
                        ColumnVector dest = outputCols.get("right_" + col);
                        if (src.isStringcol()) {
                            dest.append(src.string_get_index(j));
                        } else {
                            dest.append(src.double_all_data(j));
                        }
                    }
                }
            }
        }
        for (Map.Entry<String, ColumnVector> entry : outputCols.entrySet()) {
            result.add_columns(entry.getKey(), entry.getValue());
        }

        return result;

    }
    private boolean rowsMatch(Batch b1, Batch b2, int i, int j) {
        ColumnVector col1 = b1.get_columnar(column_name);
        ColumnVector col2 = b2.get_columnar(column_name);
        if (col1.isStringcol() && col2.isStringcol()) {
            return col1.string_get_index(i).equals(col2.string_get_index(j));
        } else if(!col1.isStringcol() && !col2.isStringcol()){
            return col1.double_get_index(i) == col2.double_get_index(j);
        }
        return false;
    }

}
