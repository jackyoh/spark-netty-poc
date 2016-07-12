package idv.jack.spark.driver;

import idv.jack.netty.client.NettyClient;

public abstract class AbstractSparkDriver implements java.io.Serializable {
	protected String sparkDriverArgs[];
	
	public AbstractSparkDriver(String args[]){
		this.sparkDriverArgs = args;
	}
	
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
		String host = this.sparkDriverArgs[1];
		int port = Integer.parseInt(this.sparkDriverArgs[2]);
		new NettyClient(host, port).start(resultValue);
	}

}
