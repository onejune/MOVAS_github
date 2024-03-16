package com.mobvista.udf.creative;

import org.apache.hadoop.hive.ql.exec.UDF;

import java.util.ArrayList;
import java.util.List;

public class GetCrtTrynewTypeList extends UDF {

    public List<String> evaluate(String extAlgo){

        try{
            List<String> res = new ArrayList<String>();
            res.add("impression");

            String[] crtTrynewSplit = extAlgo.split(";")[0].split(",")[20].split("\004");
            if(crtTrynewSplit[0].equals("1")){
                res.add("tpl_trynew");
            }
            if(crtTrynewSplit[1].equals("1")){
                res.add("image_trynew");
            }
            if(crtTrynewSplit[2].equals("1")){
                res.add("video_trynew");
            }
            if(crtTrynewSplit[3].equals("1")){
                res.add("playable_trynew");
            }

            return res;
        }catch (ArrayIndexOutOfBoundsException e){
            return new ArrayList<String>();
        }


    }

}
