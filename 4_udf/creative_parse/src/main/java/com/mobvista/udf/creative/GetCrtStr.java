package com.mobvista.udf.creative;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import org.apache.hadoop.hive.ql.exec.UDF;

import java.util.*;

public class GetCrtStr extends UDF {

    public String evaluate(String extStats){

        Object[] creativeArr = JSON.parseObject(extStats)
                .getJSONObject("value")
                .getJSONObject("creative")
                .getJSONArray("group_list")
                .toArray();
        Map<String, String> crtMap = new HashMap<String, String>(){{
            put("201", ";;;;");
            put("50002", ";;;;");
            put("61002", ";;;;");
        }};
        for(Object obj: creativeArr){
            JSONObject json = JSON.parseObject(obj.toString());
            String crtType = json.getString("creative_type");
            if(crtMap.containsKey(crtType)){
                String value = crtType + ";" + json.getString("adn_creative_id") + ";" + json.getString("dId") + ";" +
                        json.getString("dgId") + ";" + json.getString("parts") + ";" + json.getString("uniq_cid");
                crtMap.put(crtType, value);
            }
        }
        StringBuilder sb = new StringBuilder();
        sb.append(crtMap.get("201")).append("#").append(crtMap.get("50002")).append("#").append(crtMap.get("61002"));

        return sb.toString();
//        List<Map.Entry<String, String>> crtList = new ArrayList<Map.Entry<String, String>>(crtMap.entrySet());
//        Collections.sort(crtList, new Comparator<Map.Entry<String, String>>() {
//            public int compare(Map.Entry<String, String> o1, Map.Entry<String, String> o2) {
//                return o1.getKey().compareTo(o2.getKey());
//            }
//        });
//        StringBuilder sb = new StringBuilder();
//        for(Map.Entry<String, String> entry: crtList){
//            sb.append(entry.getValue());
//            sb.append("#");
//        }
//        return sb.substring(0, sb.length() - 1).toString();


    }
}
