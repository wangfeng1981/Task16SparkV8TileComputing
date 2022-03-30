package com.pixelengine.DataModel;
//this class is deprecated on 2021-3-27, use JProduct
import java.io.Serializable;

public class JProductInfo implements Serializable {
    public String productName , proj ;
    public int minZoom, maxZoom , dataType , bandNum ;
    public String[] bandNames ;
    public int[] bandPids ;
    public int[] bandBandIndices;
    public int[] bandBandNums;
    public double[] scales;
    public double[] offsets;
    public String hTableName, hFamily ;
    public int hPidByteNum ;
    public int hPid;// pid in hbase;
    public int pid ;//pid in mysql
    public int hYXByteNum , noData, tileWid, tileHei ;
    public String compress ;//none, deflate
    public long hcol;//new added, only for user file.
    public int getDataTypeByteLen() {
        switch (dataType){
            case 1: return 1;
            case 2: return 2;
            case 3: return 2;
            case 4: return 4;
            case 5: return 4;
            case 6: return 4;
            case 7: return 8;
        }
        return 0 ;
    }

}
