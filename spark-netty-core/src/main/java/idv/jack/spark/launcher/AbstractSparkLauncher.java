package idv.jack.spark.launcher;

import idv.jack.netty.server.EchoServerHandler;
import idv.jack.netty.server.NettyServer;
import idv.jack.sparknetty.conf.SparkNettyConf;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.InetAddress;
import java.util.List;

import org.apache.spark.launcher.SparkLauncher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
		
		this.nettyServer.close();
		LOG.info("Shutdown Netty Server port:" + this.sparkNettyConf.getNettyPort());
		return this.echoServerHandler.getResultList();
	}
	
	protected void startNettyServer(){
		try{
			this.checkNettyHostIP();
			
			this.nettyServer = new NettyServer();
			this.echoServerHandler = new EchoServerHandler();	
			Integer port = this.nettyServer.start(this.echoServerHandler);
			this.sparkNettyConf.setNettyPort(String.valueOf(port));
			LOG.info("Netty Port Number is:" + port);

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

