package org.example;

import java.util.HashMap;

public class ProjectExpression {
    String Column_Name;
    String Operator;
    double value;
    String New_Column_name;
    String Column_name2;

    public ProjectExpression(String column_Name, String operator, double value, String new_Column_name) {
        Column_Name = column_Name;
        Operator = operator;
        this.value = value;
        New_Column_name = new_Column_name;
    }

    public ProjectExpression(String column_Name, String operator, String column_name2, String new_Column_name) {
        Column_Name = column_Name;
        Operator = operator;
        New_Column_name = new_Column_name;
        Column_name2 = column_name2;
    }

    public Batch ProjEpr_single(Batch batch){
        if((batch==null)||batch.getSize()==0){
            System.out.println("Empty Batch");
            return batch;
        }
        HashMap<String, ColumnVector> newCol= new HashMap<>();
        Batch Expr = new Batch();
        ColumnVector ori = batch.get_columnar(Column_Name);
        boolean isStr = ori.isStringcol();
        ColumnVector out = new ColumnVector(ori.get_size(),isStr);
        for(int i=0; i<ori.get_size(); i++){
            double x = 0;
            if(isStr){
                out.append(ori.string_get_index(i));
            }
            else{

                switch (Operator){
                    case "*":
                        x=ori.double_get_index(i)*value;
                        break;
                    case "+":
                        x=ori.double_get_index(i)+value;
                        break;
                    case "-":
                        x=ori.double_get_index(i)-value;
                        break;
                    case "/":
                        x=ori.double_get_index(i)/value;
                        break;
                    case "":
                        x= ori.double_get_index(i);
                        break;
                }

            }
            out.append(x);
        }
        Expr.add_columns(New_Column_name,out);
        return Expr;
    }

    public Batch ProjEpr_multi(Batch batch){
        if((batch==null)||batch.getSize()==0){
            System.out.println("Empty Batch");
            return batch;
        }
        Batch Expr = new Batch();
        ColumnVector col1= batch.get_columnar(Column_Name);
        boolean isStr1= col1.isStringcol();
        ColumnVector col2= batch.get_columnar(Column_name2);
        boolean isStr2= col2.isStringcol();
        ColumnVector out = new ColumnVector(col1.get_size(),(isStr1||isStr2));
        for(int i=0; i<col1.get_size(); i++){
            String res="";
            double res2=0;

            if(isStr1||isStr2){
                switch (Operator){
                    case "+":
                        res= col1.string_get_index(i)+col2.string_get_index(i);
                        break;
                    default:
                        System.out.println("Invalid operator for strings");
                }
                out.append(res);
            }
            else{
                switch (Operator){
                    case "+":
                        res2= col1.double_get_index(i)+col2.double_get_index(i);
                        break;
                    case "-":
                        res2= col1.double_get_index(i)-col2.double_get_index(i);
                        break;
                    case "*":
                        res2= col1.double_get_index(i)*col2.double_get_index(i);
                        break;
                    case "/":
                        res2= col1.double_get_index(i)/col2.double_get_index(i);
                        break;
                    default:
                        System.out.println("Invalid Case");
                }
                out.append(res2);
            }
        }
        Expr.add_columns(New_Column_name,out);
        return Expr;

    }
}
