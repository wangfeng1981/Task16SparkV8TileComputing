package com.pixelengine.tools;
//2022-2-6
//2022-5-11
//2022-5-15
//2022-7-17


import com.pixelengine.JRDBHelperForWebservice;
import com.pixelengine.DataModel.JScript;
import com.pixelengine.DataModel.WConfig;

import java.sql.Timestamp;

//缓存脚本内容
// 2022-2-6 假设每个脚本不超过2KB字符，缓存1000个脚本（大约2MB），如果缓存脚本utime变化了那么从文件里重新读取，
// 不在缓存素组的脚本从数据库读取然后写入缓存素组覆盖掉老的脚本
public class ScriptsGetterTool {
    public class SciptContent{
        String content ;
        int sid ;
        long utime ;//update time stamp
        String utimestr;//yyyy-MM-dd hh:mm:ss
    }

    private static final int bufferSize = 1000 ;
    private static ScriptsGetterTool sharedInstance = null ;
    public static ScriptsGetterTool getSharedInstance() {
        if( sharedInstance == null ){
            sharedInstance = new ScriptsGetterTool() ;
            sharedInstance.bufferArray = new SciptContent[bufferSize];//缓存100个
        }
        return sharedInstance ;
    }

    private Object mutex = new Object();
    private SciptContent[] bufferArray = null;
    public String getScriptContent(int sid){
        int bufferIndex = sid%bufferSize ;
        if(bufferArray[bufferIndex]!=null &&
                bufferArray[bufferIndex].sid==sid ){
            return bufferArray[bufferIndex].content ;
        }
        // not in buffer or utime is not ok.
        JRDBHelperForWebservice rdb = new JRDBHelperForWebservice();
        JScript scriptObj = rdb.rdbGetScript(sid) ;
        //2022-7-17
        if( scriptObj==null ){
            return null ;
        }
        String jsfile = WConfig.getSharedInstance().pedir + scriptObj.jsfile ;
        try{
            SciptContent sc = new SciptContent();
            sc.sid = sid ;
            sc.content = FileDirTool.readFileAsString(jsfile) ;
            sc.utime = Timestamp.valueOf( scriptObj.utime).getTime()/1000 ;//2022-5-11
            sc.utimestr = scriptObj.utime ;//2022-5-15
            //加锁写入缓存数组
            synchronized ( mutex ){
                bufferArray[bufferIndex] = sc ;
            }
            return sc.content ;
        }catch(Exception ex ){
            System.out.println("getScriptContent exception:"+ex.getMessage());
            return "" ;
        }
    }

    //2022-5-15
    //force to refresh cache from local file system.
    public void updateOneScriptCache(int sid){
        int bufferIndex = sid%bufferSize ;
        // not in buffer or utime is not ok.
        JRDBHelperForWebservice rdb = new JRDBHelperForWebservice();
        JScript scriptObj = rdb.rdbGetScript(sid) ;
        if( scriptObj!=null )
        {
            String jsfile = WConfig.getSharedInstance().pedir + scriptObj.jsfile ;
            try{
                SciptContent sc = new SciptContent();
                sc.sid = sid ;
                sc.content = FileDirTool.readFileAsString(jsfile) ;
                sc.utime = Timestamp.valueOf( scriptObj.utime).getTime()/1000 ;//2022-5-11
                sc.utimestr = scriptObj.utime ;//2022-5-15
                //加锁写入缓存数组
                synchronized ( mutex ){
                    bufferArray[bufferIndex] = sc ;
                }
            }catch(Exception ex ){
                System.out.println("getScriptContent exception:"+ex.getMessage());
            }
        }
    }


    //2022-5-15
    //force to refresh cache from local file system.
    public void updateAllScriptCache(){
        for(int i = 0 ; i < bufferArray.length;++i )
        {
            if( bufferArray[i]!=null){
                int sid = bufferArray[i].sid ;
                JRDBHelperForWebservice rdb = new JRDBHelperForWebservice();
                JScript scriptObj = rdb.rdbGetScript(sid) ;
                if( scriptObj!=null )
                {
                    String jsfile = WConfig.getSharedInstance().pedir + scriptObj.jsfile ;
                    try{
                        SciptContent sc = new SciptContent();
                        sc.sid = sid ;
                        sc.content = FileDirTool.readFileAsString(jsfile) ;
                        sc.utime = Timestamp.valueOf( scriptObj.utime).getTime()/1000 ;//2022-5-11
                        sc.utimestr = scriptObj.utime ;//2022-5-15
                        //加锁写入缓存数组
                        synchronized ( mutex ){
                            bufferArray[i] = sc ;
                        }
                    }catch(Exception ex ){
                        System.out.println("getScriptContent exception:"+ex.getMessage());
                    }
                }
            }
        }
    }



}
