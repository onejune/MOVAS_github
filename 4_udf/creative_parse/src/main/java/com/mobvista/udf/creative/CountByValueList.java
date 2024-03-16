package com.mobvista.udf.creative;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import org.apache.hadoop.hive.ql.exec.UDF;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

public class CountByValueList extends UDF {

    public String evaluate(ArrayList<String> vList){

        if(vList == null || vList.size() <= 0){
            return "";
        }

        Map<String, Integer> cMap = new HashMap<String, Integer>();
        for(String value: vList){
            if(cMap.containsKey(value)){
                cMap.put(value, cMap.get(value) + 1);
            }else{
                cMap.put(value, 1);
            }
        }

        StringBuilder res = new StringBuilder();
        for(Map.Entry<String, Integer> entry: cMap.entrySet()){
            if(res.length() <= 0){
                res.append(entry.getKey() + "_" + entry.getValue());
            }else {
                res.append("," + entry.getKey() + "_" + entry.getValue());
            }
        }

        return res.toString();
    }
}
