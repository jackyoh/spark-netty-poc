package idv.jack.spark.driver;

import idv.jack.netty.client.NettyClient;

public abstract class AbstractSparkDriver implements java.io.Serializable {
	
	public void run(){
		try{
			String result = this.sparkDriverLogic();
			this.resultSendToNettyServer(result);
			
		}catch(Exception e){
			throw new RuntimeException(e);
		}
		
	}
	
	public abstract String sparkDriverLogic() throws Exception;
	
	public void resultSendToNettyServer(String resultValue) throws Exception{
		String host = "192.168.1.16";
		int port = Integer.parseInt("1234");
		new NettyClient(host, port).start(resultValue);
	}

}
