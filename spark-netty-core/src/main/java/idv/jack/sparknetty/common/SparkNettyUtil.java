package idv.jack.sparknetty.common;

import java.net.Socket;

public class SparkNettyUtil {
	public static boolean portNumberExists(String hostName, int port){
		int MIN_PORT_NUMBER = 1;
		int MAX_PORT_NUMBER = 65535;
		if(port < MIN_PORT_NUMBER || port > MAX_PORT_NUMBER){
			throw new IllegalArgumentException("Invalid start port:" + port);
		}
		boolean result = false;
        try {

            Socket s = new Socket(hostName, port);
            s.close();
            result = true;

        }catch(Exception e) {
            result = false;
        }
        return result;
	}

}
