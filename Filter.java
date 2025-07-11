package org.example;

import java.util.HashMap;
import java.util.Map;

public class Filter{
    private final String column;
    private final String operator;
    private final String value;
    private String value2;

    public Filter(String column, String operator, String value) {
        this.column = column;
        this.operator = operator;
        this.value = value;
    }

    public Filter(String column, String operator, String value, String value2) {
        this.column = column;
        this.operator = operator;
        this.value = value;
        this.value2 = value2;
    }

    public Batch filter(Batch input){
        if(input==null || input.getSize()==0){
            System.out.println("Empty Batch");
            return input;
        }
        ColumnVector input_col= input.get_columnar(column);

        Batch filter_batch = new Batch();
        Map<String, ColumnVector> newCol = new HashMap<>();

        for(String col: input.get_column_names()){
            boolean isStr = input.get_columnar(col).isStringcol();
            newCol.put(col, new ColumnVector(input.getSize(), isStr));
        }
        String CoVal1;
        double CoVal;
        for(int i=0; i<input.getSize(); i++){
            boolean keep=false;
            if(input_col.isStringcol()){
                CoVal1= input_col.string_all_data(i);
                if(operator.equals("=") && CoVal1.equalsIgnoreCase(value)){
                    keep=true;
                }
            }
            else{
                CoVal= input_col.double_all_data(i);
                int compareop = Integer.parseInt(value);
                int compareop2=0;
                if(value2!=null){
                    compareop2 = Integer.parseInt(value2);
                }
                switch (operator){
                    case ">":
                        keep=(CoVal>compareop);
                        break;
                    case ">=":
                        keep=(CoVal>=compareop);
                        break;
                    case "<":
                        keep=(CoVal<compareop);
                        break;
                    case "<=":
                        keep=(CoVal<=compareop);
                        break;
                    case "=":
                        keep=(CoVal==compareop);
                        break;
                    case "between":
                        keep=(CoVal>=compareop && CoVal<compareop2);
                        break;
                }

            }

            if(keep){
                for(String col:input.get_column_names()){
                    ColumnVector Out = newCol.get(col);
                    if(input.get_columnar(col).isStringcol()){
                        Out.append(input.get_columnar(col).string_get_index(i));
                    }
                    else{
                        Out.append(input.get_columnar(col).double_get_index(i));
                    }
                }
            }
        }
        for(String s : input.get_column_names()){
            filter_batch.add_columns(s, newCol.get(s));
        }
        return filter_batch;

    }
}
