package com.mobvista.udf.common;

import org.apache.hadoop.hive.ql.exec.UDF;

public class GetDeviceId extends UDF {

    public String evaluate(String input){

        String res = "";
        if(null != input){

//            int firstIdx = input.indexOf(",");
//            if(firstIdx > 0){
//                res = input.substring(0, firstIdx);
//            }else{
//                int secondIdx = input.indexOf(",", firstIdx + 1);
//                if(secondIdx > firstIdx + 1){
//                    res = input.substring(firstIdx + 1, secondIdx);
//                }
//            }

            String[] inputSplit = input.split(",");
            for(String seg: inputSplit){
                if(seg.length() > 20){
                    res = seg;
                    break;
                }
            }
        }

        return res;
    }

}
