package org.example;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class Batch {
    private int size;
    private final Map<String, ColumnVector> columns;
    private final List<String> name_of_columns= new ArrayList<>();

    public Batch(){
        this.columns = new HashMap<>();
        this.size=0;
    }

    public void add_columns(String name, ColumnVector vector){
        columns.put(name, vector);
        this.size= vector.get_size();
        name_of_columns.add(name);
    }

    public ColumnVector get_columnar(String name){
        return columns.get(name);
    }

    public int getSize(){
        return size;
    }

    public List<String> get_column_names(){
        return name_of_columns;
    }

}
