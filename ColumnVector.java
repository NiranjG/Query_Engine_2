package org.example;

public class ColumnVector {
     private double[] data;
     private String[] data_string;
     private boolean isString;
     private int size;

    public ColumnVector(){
    }

    public ColumnVector(int capacity, boolean isString){
        this.isString=isString;
        if(!isString) {
            this.data = new double[capacity];
        }
        else{
            this.data_string = new String[capacity];
        }
        this.size= 0;
    }

    public void append(double value){
        data[size++]=value;
    }

//    public void append(double value, double value2){
//        data[size++]= new double(value, value2);
//    }

    public void append(String value){
        data_string[size++]=value;
    }


    public double double_get_index(int index){
        return data[index];
    }

    public String string_get_index(int index){
        return data_string[index];
    }

    public double[] double_all_data(){
        return data;
    }

    public double double_all_data(int i){
        return data[i];
    }

    public String[] string_all_data(){
        return data_string;
    }

    public String string_all_data(int i){
        return data_string[i];
    }

    public int get_size(){
        return size;
    }

    public boolean isStringcol(){
        return isString;

    }

}
