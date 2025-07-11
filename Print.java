package org.example;

public class Print {


    public static void print(Batch batch){
        if(batch==null || batch.getSize()==0){
            System.out.println("Empty Batch");
            return;
        }
        final int COL_WIDTH = 15;
        for(String header: batch.get_column_names()){

            System.out.printf("%-"+COL_WIDTH+"s",header);
        }
        System.out.println();

        for (int i=0; i< batch.getSize(); i++){
            for(String s: batch.get_column_names()){
                ColumnVector col= batch.get_columnar(s);
                if(col.isStringcol()){
                    System.out.printf("%-"+COL_WIDTH+"s",col.string_get_index(i));
                }
                else {
                    System.out.printf("%-"+COL_WIDTH+"s",col.double_get_index(i));
                }
            }
            System.out.println();
        }
    }

    public static void print_column(Batch batch, String colName){
        ColumnVector one = batch.get_columnar(colName);
        if(one==null){
            System.out.println("Empty Column");
            return;

        }
        if (colName.equalsIgnoreCase("Name") || colName.equalsIgnoreCase("Domain")) {
            for (String i : one.string_all_data()) {
                System.out.println(i);
            }
        }
        else{
            for(double i : one.double_all_data()){
                System.out.println(i);
            }
        }
        }
    }

