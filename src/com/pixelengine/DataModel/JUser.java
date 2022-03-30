package com.pixelengine.DataModel;
//2022-01-01

import java.util.ArrayList;

public class JUser {
    public int uid;
    public String uname;
    public String password ;
    public String token ;

    private static ArrayList<JUser> sharedUserList = new ArrayList<>();
    public static void addToSharedList(JUser user)
    {
        if( sharedUserList.size()>100 ){
            sharedUserList.remove(0) ;
        }
        sharedUserList.add(user) ;
    }
    public static JUser getUserByToken(String token)
    {
        for(JUser tempUser:sharedUserList){
            if( tempUser.token.equals(token) ==true ){
                return tempUser ;
            }
        }
        return null ;
    }

    public static void removeUserByToken(String token)
    {
        for(JUser tempUser:sharedUserList){
            if( tempUser.token.equals(token) ==true ){
                sharedUserList.remove(tempUser) ;
                return ;
            }
        }
    }

}
