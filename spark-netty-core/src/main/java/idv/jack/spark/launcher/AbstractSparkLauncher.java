package idv.jack.spark.launcher;

import idv.jack.netty.server.EchoServerHandler;
import idv.jack.netty.server.NettyServer;
import idv.jack.sparknetty.common.SparkNettyUtil;
import idv.jack.sparknetty.conf.SparkNettyConf;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.spark.launcher.SparkLauncher;

public abstract class AbstractSparkLauncher {
	private static final Logger LOG = LoggerFactory.getLogger(AbstractSparkLauncher.class);
	
	private NettyServer nettyServer;
	private EchoServerHandler echoServerHandler;
	
	protected SparkNettyConf sparkNettyConf;
	
	public AbstractSparkLauncher(SparkNettyConf sparkNettyConf){
		this.sparkNettyConf = sparkNettyConf;
	}
	
	public abstract SparkLauncher createSparkLauncher();
	
	public List<String> launch() throws Exception{
		LOG.info("SparkLaunch start...");
		this.startNettyServer();
		
		SparkLauncher sparkLauncher = this.createSparkLauncher();
		Process spark = sparkLauncher.launch();
		
		InputStreamReaderRunnable inputStreamReaderRunnable = new InputStreamReaderRunnable(spark.getInputStream(), "input");
		Thread inputThread = new Thread(inputStreamReaderRunnable, "LogStreamReader input");
		inputThread.start();
	
		InputStreamReaderRunnable errorStreamReaderRunnable = new InputStreamReaderRunnable(spark.getErrorStream(), "error");
		Thread errorThread = new Thread(errorStreamReaderRunnable, "LogStreamReader error");
		errorThread.start();
	
		LOG.info("Waiting for finish...");
		int exitCode = spark.waitFor();
		LOG.info("Finished! Exit code:" + exitCode);
		
	
		
		return this.echoServerHandler.getResultList();
	}
	
	protected void startNettyServer(){
		try{
			this.checkNettyHostIP();
			
			Integer port = this.getNettyPortNumber();
			
			LOG.info("Netty Port Number is:" + port);
			
			this.nettyServer = new NettyServer(port);
			this.echoServerHandler = new EchoServerHandler();	

			Thread serverThread = new Thread(new NettyServerThread(nettyServer, echoServerHandler));
			serverThread.start();
			
		}catch(Exception e){
			throw new RuntimeException(e);
		}
	}
	
	private void checkNettyHostIP(){
		try{
			if(this.sparkNettyConf.getNettyHostIP() == null){
				String ip = InetAddress.getLocalHost().getHostAddress();
				this.sparkNettyConf.setNettyHostIP(ip);
			}
		}catch(Exception e){
			throw new RuntimeException(e);
		}
	}
	
	private int getNettyPortNumber(){
		Integer port = this.randomPortNumber();;
		if(this.sparkNettyConf.getNettyPort() != null && 
				!SparkNettyUtil.portNumberExists(this.sparkNettyConf.getNettyHostIP(), Integer.parseInt(this.sparkNettyConf.getNettyPort()))){
			port = Integer.parseInt(this.sparkNettyConf.getNettyPort());
		}
		this.sparkNettyConf.setNettyPort(String.valueOf(port));
		return port;
	}
	
	private int randomPortNumber(){
		try{
			return SparkNettyUtil.randomPortNumber();
		}catch(Exception e){
			throw new RuntimeException(e);
		}
	}
}

class InputStreamReaderRunnable implements Runnable {
	private static final Logger LOG = LoggerFactory.getLogger(InputStreamReaderRunnable.class);
    private BufferedReader reader;

    private String name;

    public InputStreamReaderRunnable(InputStream is, String name) {
        this.reader = new BufferedReader(new InputStreamReader(is));
        this.name = name;
    }

    public void run() {
        LOG.info("InputStream " + name + ":");
        try {
            String line = reader.readLine();
            while (line != null) {
            	LOG.info(line);
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
			this.wait();
		}catch(Exception e){
			throw new RuntimeException(e);
		}
	}
}
