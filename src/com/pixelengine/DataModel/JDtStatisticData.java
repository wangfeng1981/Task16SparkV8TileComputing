package com.pixelengine.DataModel;
//add datetime to JStatisticData
//2022-3-27 created
//2022-3-31 use String key replace Long datetime
//2022-4-4 use array data .

import com.pixelengine.JStatisticData;

public class JDtStatisticData implements Comparable<JDtStatisticData>{
    public String key;
    public JStatisticData[] data ;

    @Override
    public int compareTo(JDtStatisticData o) {
        return this.key.compareTo(o.key);
    }
}
