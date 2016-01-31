package bb;

import java.util.*;
import java.io.*;

class MyThread extends Thread {
    public static Bound minGlobalUpperBound = new Bound();
    public double bound = 1e99;
    
    public boolean update = true;
    public boolean flag = false;
    
    public void updateBound() {
        String s = HttpRequest.sendPost("http://10.1.0.5:3000/bound/minGlobalUpperbound", "bound="+bound);
        try {
            bound = Double.parseDouble(s);
        } catch( Exception e ) {
            e.printStackTrace();
        }
    }
    
    //@override
    /*public void run() {
     while( true ) {
     synchronized(minGlobalUpperBound) {
     try {
     minGlobalUpperBound.wait();
     bound = minGlobalUpperBound.minGlobalUpperBound;
     } catch(Exception e) {
     e.printStackTrace();
     }
     }
     updateBound();
     synchronized(minGlobalUpperBound) {
     minGlobalUpperBound.minGlobalUpperBound = bound;
     }
     }
     }*/
    public void run() {
        while(true) {
            if(update) {
                updateBound();
            }
            if(flag) {
                return;
            }
            try {
                Thread.sleep(15000);
            } catch(Exception e) {
                e.printStackTrace();
            }
            //update = false;
            synchronized(minGlobalUpperBound) {
                flag = minGlobalUpperBound.flag;
                if(bound > minGlobalUpperBound.minGlobalUpperBound) {
                    bound = minGlobalUpperBound.minGlobalUpperBound;
                    update = true;
                } else {
                    minGlobalUpperBound.minGlobalUpperBound = bound;
                }
            }
        }
    }
}


