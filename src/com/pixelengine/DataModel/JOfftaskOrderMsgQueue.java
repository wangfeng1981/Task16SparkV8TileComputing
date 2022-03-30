package com.pixelengine.DataModel;
//2022-01-01
import java.util.ArrayList;
import java.util.List;

public class JOfftaskOrderMsgQueue {
    private static JOfftaskOrderMsgQueue sharedInstance = null ;
    public static JOfftaskOrderMsgQueue getSharedInstance(){
        if( sharedInstance==null) sharedInstance=new JOfftaskOrderMsgQueue() ;
        return sharedInstance ;
    }
    private ArrayList<JOfftaskOrderMsg> msgList = new ArrayList<>() ;

    //if exceed max size get rid of the msg, otherwise append into list.
    public synchronized  boolean append( JOfftaskOrderMsg msg) {
        final int maxSize = 100 ;
        if( msgList.size() > maxSize ){
            //get rid of msg
            System.out.println("msgList exceed maxSize:"+maxSize);
            return false ;
        }else{
            msgList.add(msg) ;
            return true ;
        }
    }

    public synchronized JOfftaskOrderMsg pop() {
        if( msgList.size()>0 ){
            JOfftaskOrderMsg msg = msgList.get(0) ;
            msgList.remove(0) ;
            return msg ;
        }else{
            return null ;
        }
    }



}
