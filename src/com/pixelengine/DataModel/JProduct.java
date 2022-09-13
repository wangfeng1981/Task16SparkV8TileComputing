package com.pixelengine.DataModel;
//2021-3
//2022-01-01
//2022-8-3 displayid
//2022-9-8 gots
import com.pixelengine.JRDBHelperForWebservice;

import java.sql.SQLException;
import java.util.ArrayList;



public class JProduct {
    public String displayid=""; //only used in display
    public int pid ;
    public String name,proj;
    public int minZoom,maxZoom,dataType,timeType;
    public String hTableName;
    public int tileWid,tileHei ;
    public String compress ;
    public int styleid , userid ;
    public String source;//hbase or file or sqlite(not use)
    public String[] caps ;//not in use
    public ArrayList<JMeta> gots = new ArrayList<>();
    /*
    区域统计	zs
    序列分析	xl
    数据合成	co
    数据导出	ex
    渲染方案修改	st
    日期选择	dt
     */

    //exteranl
    public ArrayList<JProductBand> bandList = new ArrayList<>() ;
    public JHBaseTable hbaseTable = new JHBaseTable() ;
    public JProductDisplay productDisplay = new JProductDisplay() ;
    public JProductDataItem latestDataItem = new JProductDataItem() ;

    public static int getDataByteLenByDataType(int datatype){
        if( datatype==1 ){
            return 1;
        }else if( datatype==2 || datatype==3 ){
            return 2 ;
        }else if( datatype==4 || datatype==5 || datatype==6 ){
            return 4 ;
        }else if( datatype==7) {
            return 8 ;
        }else{
            return 0 ;
        }
    }
    public int getDataByteLen(){
        return JProduct.getDataByteLenByDataType(this.dataType) ;
    }

    private static ArrayList<JProduct> s_sharedlist ;
    public static ArrayList<JProduct> getSharedList() throws SQLException {
        if( s_sharedlist==null ){
            //load from db
            JRDBHelperForWebservice rdb = new JRDBHelperForWebservice() ;

            //1.load from tbProduct
            s_sharedlist = rdb.rdbGetProducts() ;

            return s_sharedlist ;
        }else{
            return s_sharedlist ;
        }
    }
}
