package com.pixelengine.tools;
/// 2022-3-5 2210


import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.text.SimpleDateFormat;
import java.util.Date;

public class FileDirTool {
    ///ext: .js .json .xml
    static public String makeDatetimeFilename(String rootdir, String prefix, String ext){
        Date date = new Date();
        SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMdd");
        String da = sdf.format(date);
        SimpleDateFormat sdf1 = new SimpleDateFormat("yyyyMMddhhmmss");
        String da1 = sdf1.format(date);
        String yearstr = da.substring(0,4) ;
        String thepath = rootdir + "/" +yearstr + "/" +da +"/";

        File dirfile = new File( thepath) ;
        File dirfile0 = new File( dirfile.getParent()) ;
        if( dirfile0.exists() == false ){
            dirfile0.mkdirs() ;
        }
        if( dirfile.exists()==false ){
            boolean dirok = dirfile.mkdir() ;
            if( dirok==false ){
                System.out.println("Create dir failed:" + thepath);
                return null ;
            }
        }

        String tempfullname = thepath + prefix + "-" + da1 + ext ;
        int itry = 1;
        while( (new File(tempfullname)).exists()==true ){
            tempfullname = thepath + prefix + "-" + da1 +"-" + itry + ext ;
            ++itry ;
        }
        return tempfullname ;
    }

    static public String dateTimeString()
    {
        Date date = new Date();
        SimpleDateFormat sdf1 = new SimpleDateFormat("yyyyMMddhhmmss");
        String dtstr = sdf1.format(date);
        return dtstr ;
    }

    ///根据当前日期构建一个目录，格式 {rootdir}/{subdir}/yyyy/yyyyMMdd/
    ///rootdir必须存在，函数不对rootdir做检查
    ///subdir 可以存在也可以不存在，如果不存在就创建
    ///返回绝对路径和相对路径包含斜杠结尾 0-绝对路径， 1-相对路径
    static public String[] checkAndMakeCurrentYearDateDir(String rootdir, String subdir ){
        Date date = new Date();
        SimpleDateFormat yearformat = new SimpleDateFormat("yyyy");
        String yearstr = yearformat.format(date);
        SimpleDateFormat ymdformat = new SimpleDateFormat("yyyyMMdd");
        String ymdstr = ymdformat.format(date);

        String relativeDir = "/" + subdir + "/" +yearstr + "/" +ymdstr +"/" ;
        String absDir = rootdir + relativeDir ;
        File dirfile = new File( absDir) ;
        if( dirfile.exists() == false ){
            dirfile.mkdirs() ;
        }
        if( dirfile.exists()==false ){
            System.out.println("Create dir failed:" + absDir);
            return null ;
        }
        String[] outdir = new String[2] ;
        outdir[0] = absDir ;
        outdir[1] = relativeDir ;
        return outdir ;
    }

    static public boolean writeToFile(String filename,String content)
    {
        BufferedOutputStream stream = null;
        try {
            stream = new BufferedOutputStream(new FileOutputStream(filename));
            stream.write(content.getBytes());
            stream.flush();
            stream.close();
            return true ;
        }catch (IOException e){
            e.printStackTrace();
            return false ;
        }
    }

    static public  String readFileAsString(String fileName)throws Exception
    {
        String data = "";
        data = new String(Files.readAllBytes(Paths.get(fileName)));
        return data;
    }

    static public byte[] readFileAsBytes(String fileName)
    {
        try{
            return Files.readAllBytes(Paths.get(fileName));
        }catch (Exception ex){
            System.out.println("readFileAsBytes exception:" + ex.getMessage());
            return null ;
        }
    }

    /**
     * 判断文件夹是否存在,如果不存在创建他
     * @param dirPathStr
     */
    static public boolean checkDirExistsOrCreate(String dirPathStr) {
        File file = new File(dirPathStr) ;
        if (file.exists()) {
            return true ;
        } else {
            return file.mkdir();
        }
    }
}
