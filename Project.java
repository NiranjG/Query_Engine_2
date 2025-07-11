package org.example;

import java.util.HashMap;

public class Project {
    private final String[] project;

    public Project(String[] project) {
        this.project = project;
    }


    public Batch project(Batch batch){
        if(batch==null || batch.getSize()==0){
            System.out.println("Empty Batch");
            return batch;
        }
        Batch projectedbatch= new Batch();
        HashMap<String, ColumnVector> newcol = new HashMap<>();
        for(String col: project){
            ColumnVector orignal = batch.get_columnar(col);
            boolean isStr = orignal.isStringcol();
            ColumnVector out= new ColumnVector(orignal.get_size(), isStr);
            //Print.print_column(batch,col);
            for(int i=0; i<orignal.get_size(); i++){
                if(isStr){
                    out.append(orignal.string_get_index(i));
                }
                else{
                    out.append(orignal.double_get_index(i));
                }
            }
            newcol.put(col,out);
        }

        for(String col: project){
            projectedbatch.add_columns(col,newcol.get(col));
        }
        return projectedbatch;
    }
}
