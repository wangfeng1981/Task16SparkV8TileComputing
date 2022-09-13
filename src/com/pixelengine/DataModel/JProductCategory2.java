//2022-7-31 created
//2022-8-3

package com.pixelengine.DataModel;

import java.lang.reflect.Array;
import java.util.ArrayList;

public class JProductCategory2 {
    // CATLVL1
    public static class CateLevel1 {
        public String displayid="";
        public JMeta meta;
        public ArrayList<CateLevel2> level2Array = new ArrayList<>() ;
    }
    // CATLVL2
    public static class CateLevel2 {
        public String displayid="";
        public JMeta meta ;
        public ArrayList<JProduct> productArray=new ArrayList<>();
    }

    public ArrayList<CateLevel1> level1Array=new ArrayList<>() ;


}
