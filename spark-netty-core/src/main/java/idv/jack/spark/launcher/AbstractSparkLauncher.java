package idv.jack.spark.launcher;

import idv.jack.netty.server.EchoServerHandler;
import idv.jack.netty.server.NettyServer;
import idv.jack.sparknetty.conf.SparkNettyConf;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.List;

import org.apache.spark.launcher.SparkLauncher;

public abstract class AbstractSparkLauncher {
	private NettyServer nettyServer;
	private EchoServerHandler echoServerHandler;
	
	protected SparkNettyConf sparkNettyConf;
	
	public AbstractSparkLauncher(SparkNettyConf sparkNettyConf){
		this.sparkNettyConf = sparkNettyConf;
	}
	
	public abstract SparkLauncher createSparkLauncher();
	
	public List<String> launch() throws Exception{
		this.startNettyServer();
		
		SparkLauncher sparkLauncher = this.createSparkLauncher();
		Process spark = sparkLauncher.launch();
		
		InputStreamReaderRunnable inputStreamReaderRunnable = new InputStreamReaderRunnable(spark.getInputStream(), "input");
		Thread inputThread = new Thread(inputStreamReaderRunnable, "LogStreamReader input");
		inputThread.start();
	
		InputStreamReaderRunnable errorStreamReaderRunnable = new InputStreamReaderRunnable(spark.getErrorStream(), "error");
		Thread errorThread = new Thread(errorStreamReaderRunnable, "LogStreamReader error");
		errorThread.start();
	
		System.out.println("Waiting for finish...");
		int exitCode = spark.waitFor();
		System.out.println("Finished! Exit code:" + exitCode);
		
		this.stopNettyServer();
		
		return this.echoServerHandler.getResultList();
	}
	
	protected void startNettyServer(){
		try{
			
			Integer port = Integer.parseInt(this.sparkNettyConf.getNettyPort());
			this.nettyServer = new NettyServer(port);
			this.echoServerHandler = new EchoServerHandler();	
			//this.nettyServer.start(this.echoServerHandler);
			Thread serverThread = new Thread(new NettyServerThread(nettyServer, echoServerHandler));
			serverThread.start();
			
			
		}catch(Exception e){
			throw new RuntimeException(e);
		}
	}
	
	protected void stopNettyServer(){
		//TODO
	}
}

class InputStreamReaderRunnable implements Runnable {

    private BufferedReader reader;

    private String name;

    public InputStreamReaderRunnable(InputStream is, String name) {
        this.reader = new BufferedReader(new InputStreamReader(is));
        this.name = name;
    }

    public void run() {
        System.out.println("InputStream " + name + ":");
        try {
            String line = reader.readLine();
            while (line != null) {
                System.out.println(line);
                line = reader.readLine();
            }
            reader.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
class NettyServerThread implements Runnable {
	private NettyServer nettyServer;
	private EchoServerHandler echoServerHandler;
	
	public NettyServerThread(NettyServer nettyServer, EchoServerHandler echoServerHandler){
		this.nettyServer = nettyServer;
		this.echoServerHandler = echoServerHandler;
	}
	
	@Override
	public void run() {
		try{
			this.nettyServer.start(this.echoServerHandler);
		}catch(Exception e){
			throw new RuntimeException(e);
		}
	}
}
