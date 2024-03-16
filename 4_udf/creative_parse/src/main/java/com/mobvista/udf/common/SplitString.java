package com.mobvista.udf.common;

import org.apache.hadoop.hive.ql.exec.UDF;

import java.util.ArrayList;
import java.util.List;

public class SplitString extends UDF {

    public List<String> evaluate(String input, String sep){

        List<String> resList = new ArrayList<String>();

        if(input != null && !input.equals(",") && !input.equals("-") && !input.equals(sep)){
            if(input.startsWith(sep))
                input = input.substring(1);
            String[] inputSplit = input.split(sep);

            for(String seg: inputSplit){
                resList.add(seg);
            }
        }

        return resList;




    }
}
