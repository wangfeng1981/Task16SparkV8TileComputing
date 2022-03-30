package com.pixelengine.DataModel;
//2022-01-01
import java.io.Serializable;

public class JOfflineTask implements Serializable {
    public int oftid;
    public String scriptContent;
    public String extent;
    public int zmin,zmax,outProductId;
    public long outDatetime,startTime,endTime;
    public int uid,storage,stype;
    public String resultjson,path,htable;
    public int hpid;
    public long hcol;
    public String hfami;
    public int hpidlen;
    public int hxylen;
    public int status;
}
