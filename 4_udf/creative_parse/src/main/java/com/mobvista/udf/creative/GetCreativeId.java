package com.mobvista.udf.creative;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import org.apache.hadoop.hive.ql.exec.UDF;

import java.util.HashMap;
import java.util.Map;

public class GetCreativeId extends UDF {

    public String evaluate(String extStats, String creativeType){

        Object[] creativeArr = JSON.parseObject(extStats)
                .getJSONObject("value")
                .getJSONObject("creative")
                .getJSONArray("group_list")
                .toArray();
        Map<String, String> typeMap = new HashMap<String, String>();
        for(Object obj: creativeArr){
            JSONObject json = JSON.parseObject(obj.toString());
            typeMap.put(json.getString("creative_type"), json.getString("adn_creative_id"));
        }
        if(typeMap.containsKey(creativeType)){
            return typeMap.get(creativeType);
        }else {
            return "null";
        }



    }
}
