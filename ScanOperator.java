package org.example;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;

public class ScanOperator{
    String filepath;
    int batchsize;
    BufferedReader reader;
    String[] column_name;

    public ScanOperator(String filepath, int batchsize) {
        this.filepath = filepath;
        this.batchsize = batchsize;
    }
    public void open() throws IOException {
        reader = new BufferedReader(new FileReader(filepath));
        String Header= reader.readLine();
        column_name= Header.split(",");
        //System.out.println("Opened file: "+filepath);
        //System.out.println("Column names :"+ Arrays.toString(column_name));
    }

    private boolean isString_Col(String data){
        return data.equalsIgnoreCase("name") || data.equalsIgnoreCase("domain");
    }

    public Batch get_batch() throws IOException {
        Batch batch= new Batch();
        ColumnVector[] tuple= new ColumnVector[column_name.length];
        for (int i=0; i<column_name.length; i++){
            boolean isString = isString_Col(column_name[i]);
            tuple[i]= new ColumnVector(batchsize,isString);
        }
        int count=0;
        String line;
        //System.out.println("Reading next batch...");
        while (count<batchsize && ((line= reader.readLine())!=null)){
            String[] each_column = line.split(",");
            //System.out.println("Line read- "+line);
            //System.out.println("Split: "+Arrays.toString(each_column));
            for(int i=0; i<each_column.length;i++){
                if(tuple[i].isStringcol()){
                    tuple[i].append(each_column[i]);
                }
                else {
                    tuple[i].append(Integer.parseInt(each_column[i]));
                }

            }
            //System.out.println("Batch size: "+count);
            count++;
        }
        if (count==0){
            return null;
        }
        for(int i=0; i< column_name.length; i++){
            batch.add_columns(column_name[i],tuple[i]);
        }
        return batch;
    }

    public void close() throws IOException {
        if(reader!=null){
            reader.close();
        }
    }
}