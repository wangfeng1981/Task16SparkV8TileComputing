package com.pixelengine.DataModel;
//2022-01-01
public class JOfftaskWorkerResult {
    public int ofid ;//offtask 主键
    public int state=0;//0-good, other failed.
    public String resultRelFilepath="";//should be a json file , should be relative path under nginx-pedir
    public int status=0;//0-not start; 1-running; 2-done; 3-failed.
}
