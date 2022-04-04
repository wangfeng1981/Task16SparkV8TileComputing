package com.pixelengine;
//2022-3-27 created . this class should be in com/pixelengine/... otherwise cpp lib can not find it.


import scala.Serializable;

public class JStatisticData implements Serializable {
    public double sum=0;
    public double sq_sum=0;
    public double validCnt=0;
    public double validMin=0;
    public double validMax=0;
    public double areakm2=0;
    public double allCnt=0;
    public double fillCnt=0;

    public double computeMean() {
        if( validCnt>0 ) return sum / validCnt ;
        else return 0 ;
    }

    public double computeStdev() {
        if( validCnt==0 ) return 0 ;
        double mean = computeMean() ;
        double var = sq_sum / validCnt - mean*mean ;
        return Math.sqrt(var) ;
    }
}
