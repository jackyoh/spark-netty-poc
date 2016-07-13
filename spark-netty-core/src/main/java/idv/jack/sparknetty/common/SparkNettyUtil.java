package idv.jack.sparknetty.common;

import java.util.Random;

public class SparkNettyUtil {
	public static boolean portNumberExists(String hostName, int port){
		//TODO
		return false;
	}
	
	public static int randomPortNumber() {
		Random rand = new Random();
		return rand.nextInt(40000) + 5000;
	}

}
