package org.example;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class Main {
    public static void main(String[] args) throws IOException {



        ScanOperator scan = new ScanOperator("src/main/resources/employees.csv", 5);
        ScanOperator scan2 = new ScanOperator("src/main/resources/age.csv", 5);
        Filter f1 = new Filter("domain","=","IT");
        Filter f2 = new Filter("salary","<=","6000");


        scan2.open();
        scan.open();
        List<Batch> filter_age = new ArrayList<>();
        List<Batch> filter_emp = new ArrayList<>();
        Batch age;
        while((age = scan2.get_batch())!=null){
            Batch filtered = f1.filter(age);
            filter_age.add(filtered);
        }
        Batch emp;
        while((emp=scan.get_batch())!=null){
            Batch filtered = f2.filter(emp);
            filter_emp.add(filtered);
        }

        for(Batch emps: filter_emp){
            for(Batch ages: filter_age){
                if(emps==null || emps.getSize()==0 || ages==null || ages.getSize()==0) continue;
                Join j = new Join(emps, ages, "id");
                Batch filtered = j.join();
                if(filtered == null || filtered.getSize()==0)continue;
                Project p = new Project(new String[]{"left_name","right_age"});
                Batch result = p.project(filtered);
                if(result.getSize()>0 && result!=null){
                    Print.print(result);
                    System.out.println("-----------");
                }
            }

        }





//        while ((emp=scan.get_batch())!=null){
//            for(Batch filter : filteredge){
//                if(filter==null || filter.getSize()==0)continue;
//                Join join = new Join(emp,filter,"id");
//                Batch joined = join.join();
//                if((joined == null) || (joined.getSize()==0))continue;
//                Project project = new Project(new String[]{"left_name","left_salary"});
//                Batch result = project.project(joined);
//                if(result!=null && result.getSize()>0) {
//                    Print.print(result);
//                }
//            }
//        }
    }


}
