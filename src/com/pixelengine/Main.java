package com.pixelengine;
//2022-3-27 改造该程序，使其能够支持 离线js计算写入hbase，区域统计，序列分析
// 共同点就是Spark计算，通过传入参数来判断哪种模式

import com.google.gson.Gson;
import com.pixelengine.DataModel.*;
import com.pixelengine.tools.*;
import org.apache.calcite.materialize.Lattice;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.FuzzyRowFilter;
import org.apache.hadoop.hbase.filter.KeyOnlyFilter;
import org.apache.hadoop.hbase.filter.MultiRowRangeFilter;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.CellCreator;
import org.apache.hadoop.hbase.spark.JavaHBaseContext;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.catalyst.plans.logical.Except;
import org.apache.spark.util.CollectionAccumulator;
import org.apache.spark.util.LongAccumulator;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.*;
import org.apache.spark.broadcast.Broadcast;
import scala.Int;
import scala.Tuple2;


import javax.swing.*;
import java.io.*;
import java.net.URL;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.ListIterator;
import java.util.logging.FileHandler;
import java.util.logging.Filter;
import java.util.logging.Logger ;
import java.util.logging.SimpleFormatter;




public class Main {





    ///
    public static void main(String[] args) throws IOException {

        System.out.println("Spark v8 tile computing and output to HBase.2022-3-17");
        System.out.println("Spark v8 tile computing include (1)js compute to hbase," +
                " (2)region statistic, (3)serial statistic. 2022-3-27");

        {//print pe version info
            System.out.println("----------------");
            System.out.println("pe version info:");
            HBasePeHelperCppConnector cppconn0 = new HBasePeHelperCppConnector() ;
            String peversion = cppconn0.GetVersion() ;
            System.out.println(peversion) ;
            System.out.println("----------------");
        }

        System.out.println("v1.0.0 created 2022-3-18.") ;
        System.out.println("v1.0.1 created 2022-3-23.") ;
        System.out.println("v1.1.1 use outter task17config.json 2022-3-23.") ;
        System.out.println("v1.2.0 add direct roi2 clip 2022-3-24.") ;
        System.out.println("v1.3.0 add mysql staff 2022-3-24.") ;
        System.out.println("v2.0.2 add region stat and serial stat. 2022-3-27") ;
        System.out.println("v2.0.3 test dtcollection ok. 2022-3-31") ;
        System.out.println("v2.1.2 make serial working. 2022-4-4") ;
        System.out.println("v2.1.3 shikuang serial bugfixed. 2022-4-5") ;
        System.out.println("v2.1.4 do not update dsname. 2022-4-5") ;
        System.out.println("v2.1.4.1 add debug info output for finding bugs in getTileDataCollection. 2022-6-6") ;
        System.out.println("v2.1.5.0 debug infos") ;
        System.out.println("v2.1.5.1 update some java files from task17. 2022-7-3") ;
        System.out.println("v2.1.5.2 update some java files from task17 for nearest datetimeobj. 2022-7-8") ;
        System.out.println("v2.1.6.0 write dataitem with dt0 dt1. 2022-7-13");
        System.out.println("v2.1.6.1 update java files from task17. 2022-7-17");
        System.out.println("v2.1.7.1 update pe product script. 2022-7-27");
        System.out.println("v2.2.0.0 add General Spark V8 Computing 2022-9-2.");//this give up.
        //v2.2.0.0 尝试做一个通用的MapReduce流程，但是太难了，放弃。

        //v2.3.x
        //1.增加脚本序列分析
        //2.序列分析结果排序
        System.out.println("v2.3.1.0 add serial compute for script product. 2022-9-4");
        System.out.println("usage:");
        System.out.println("spark-submit --master spark://xxx:7077 Task16SparkV8TileComputingToHbase.jar ");
        System.out.println("    task17config.json ");
        System.out.println("    tasktype ");
        System.out.println("    task-order.json ");
        System.out.println("    output.json ");
        System.out.println("--------------------------");
        System.out.println("tasktype: jshbase 瓦片计算到HBase, stat 区域统计, serial 序列分析");
        System.out.println("tasktype: sserial 脚本产品序列分析.");
        System.out.println("--------------------------");

        if( args.length != 4 ){
            System.out.println("Error : args.length!=4, out.") ;
            return ;
        }
        //inputs
        String task17configfile = args[0] ;
        String tasktype = args[1] ;//jshbase,stat,serial
        String inputOrderfile = args[2];
        String outputJsonFilename = args[3] ;
        System.out.println("task17config:"+task17configfile);
        System.out.println("tasktype:"+tasktype);
        System.out.println("inputOrderfile:"+inputOrderfile);
        System.out.println("outputJsonFile:"+outputJsonFilename);

        try {
            if( tasktype.compareTo("jshbase") == 0 ){
                WTileComputing2HBaseProcessor tc = new WTileComputing2HBaseProcessor();
                tc.orderJsonFile = inputOrderfile ;
                tc.task17configFile = task17configfile ;
                tc.resultJsonFile = outputJsonFilename ;
                int state = tc.runit();//程序里面负责写明结果正常还是失败，外边不写这个outjson
                System.exit(state);  // Signifying the normal end of your program
            }else if( tasktype.compareTo("stat") == 0 )
            {
                WTileComputingStatisticProcessor proc = new WTileComputingStatisticProcessor() ;
                proc.orderJsonFile = inputOrderfile ;
                proc.task17configFile = task17configfile ;
                proc.resultJsonFile = outputJsonFilename ;
                int state = proc.runit() ;
                System.exit(state);
            }
            else if( tasktype.compareTo("serial")==0 )
            {
                WTileComputingSerialProcessor proc = new WTileComputingSerialProcessor() ;
                proc.orderJsonFile = inputOrderfile ;
                proc.task17configFile = task17configfile ;
                proc.resultJsonFile = outputJsonFilename ;
                int state = proc.runit() ;
                System.exit(state);
            }
            else if( tasktype.compareTo("sserial")==0 )
            {
                WTileComputingScriptSerialProcessor proc = new WTileComputingScriptSerialProcessor() ;
                proc.orderJsonFile = inputOrderfile ;
                proc.task17configFile = task17configfile ;
                proc.resultJsonFile = outputJsonFilename ;
                int state = proc.runit() ;
                System.exit(state);
            }
            else{
                System.out.println("unsupported task type:"+ tasktype);
                System.exit(3);
            }

        } catch (Exception ex) {
            System.out.println("runApp exception:"+ ex.getMessage());
            System.exit(2);  // Signifying that there is an error
        }
    }
}


/*
//some unit test for dtcollection, need delete this block of codes.
{
    System.out.println("-------------- unit test -----------------\n-\n-\n-");
    WConfig.init(task17configfile);
    JRDBHelperForWebservice.init(WConfig.getSharedInstance().connstr,
    WConfig.getSharedInstance().user,
    WConfig.getSharedInstance().pwd);
    JRDBHelperForWebservice rdb = new JRDBHelperForWebservice();


    JDtCollectionBuilder builder1 = new JDtCollectionBuilder() ;
    builder1.wholePeriod.startDt = 20010101000000L ;
    builder1.wholePeriod.startInclusive=false;
    builder1.wholePeriod.stopDt = 20201206000000L ;
    builder1.wholePeriod.stopInclusive=false;
    builder1.repeatType = "" ;
    builder1.repeatPeriod.startDt = 0;
    builder1.repeatPeriod.stopDt  = 0;
    builder1.repeatPeriod.stopInNextYear = 1 ;
    JDtCollection[] collarr1 = rdb.buildDtCollection("mod/ndvi" , builder1) ;

    HBasePixelEngineHelper helper = new HBasePixelEngineHelper() ;

    JDtCollection tempdtc1 = new JDtCollection() ;
    tempdtc1.key = "test" ;
    tempdtc1.datetimes = new long[] {20010101000000L,20200500000000L
    ,20200600000000L,20210100000000L,20220000000000L } ;
    JDtCollection[] dtcollection = helper.buildDatetimeCollections("mod/ndvi"
    ,20000101000000L,1,20210101000000L,1
    ,"",0,0,0,0,0);
    TileData testtiledata = helper.getTileDataCollection("mod/ndvi", tempdtc1.datetimes,0,0,0);

    //unit test js
    {
    HBasePeHelperCppConnector cc = new HBasePeHelperCppConnector() ;
    String testScript = "function main(){" +
    "let dtcoll = pe.RemoteBuildDtCollections('mod/ndvi',20010101000000,1,20210101000000,1,'',0,0,0,0,0);" +
    "let dscoll = pe.DatasetCollections('mod/ndvi',dtcoll) ;" +
    "let ds = pe.CompositeDsCollections(dscoll,pe.CompositeMethodMax,-2000,10000,-19999) ;" +
    "return ds;" +
    "}" ;
    TileComputeResult tcr = cc.RunScriptForTileWithoutRenderWithExtra(
    "com/pixelengine/HBasePixelEngineHelper"
    , testScript, "" , 0,0,0
    ) ;
    FileDirTool.writeToBinaryFile("test-tileresultdata-raw" , tcr.binaryData) ;
    }


    {
    long[] dtarr = new long[]{20200500000000L,20200600000000L,20200700000000L} ;
    for(int idt = 0 ; idt < dtarr.length;++ idt )
    {
    String extraText = "{\"datetime\":" + dtarr[idt] + "}" ;
    HBasePeHelperCppConnector cc = new HBasePeHelperCppConnector() ;
    String testScript = "function main(){" +
    "let ds = pe.Dataset('mod/ndvi',pe.extraData.datetime) ;" +
    "return ds;" +
    "}" ;
    TileComputeResult tcr = cc.RunScriptForTileWithoutRenderWithExtra(
    "com/pixelengine/HBasePixelEngineHelper"
    , testScript, extraText , 0,0,0
    ) ;
    FileDirTool.writeToBinaryFile("test-tileresultdata-raw"+idt , tcr.binaryData) ;
    }
    }

    System.out.println("unit test out.");
}
*/

