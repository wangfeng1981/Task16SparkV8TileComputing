package com.pixelengine.DataModel;
//2022-01-01
import com.google.gson.Gson;

public class JCompositeParams {
    public int inpid ;//input tbproduct.pid in tbproduct
    public int bandindex ;
    public long fromdt ;
    public long todt ;
    public double vmin ;
    public double vmax ;
    public double filldata ;
    public String method;//min max ave
    //上面是用户输入的，下面是系统自动生成的
    public String scriptfilename ;
    public String outhtable , outhfami ;
    public int outpid;  //output product tbproduct.pid.
    public int outhpid; //output product in hbase hpid.
    public int outhpidblen, outyxblen ;
    public int outhcol ;//这里使用outhpid来区别每次客户的合成操作，所以这里hcol基本没有用处了，直接给个1值就完了
    public int usebound ;// 0 or 1
    public double left,right,top,bottom ;
    public int zmin,zmax ;

    public String outfilename,outfilenamedb ;//new

    public String toJson(){
        Gson gson = new Gson();
        String jsonstr = gson.toJson(this);
        return jsonstr ;
    }
}
