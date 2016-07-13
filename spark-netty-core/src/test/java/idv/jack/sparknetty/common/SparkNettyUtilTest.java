package idv.jack.sparknetty.common;

import java.net.InetAddress;
import java.net.ServerSocket;

import org.junit.Test;

public class SparkNettyUtilTest {
	@Test
	public void testAvailable(){
		boolean isAvailable = SparkNettyUtil.portNumberExists("192.168.1.16", 80);
		System.out.println(isAvailable);
		
	}

	@Test
	public void testLocalIP() throws Exception{
		System.out.println("Your Host addr: " + InetAddress.getLocalHost().getHostAddress());
	}

	@Test
	public void testRandomPort() throws Exception{
		ServerSocket tempServer = new ServerSocket(0);
  
        int port = tempServer.getLocalPort();
		System.out.println(port);
	}

}
