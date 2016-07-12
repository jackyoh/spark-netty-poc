package idv.jack.spark.launcher;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;

import org.apache.spark.launcher.SparkLauncher;

public abstract class AbstractSparkLauncher {
	
	public abstract SparkLauncher createSparkLauncher();
	
	public void launch() throws Exception{
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
	}
	
	protected void startNettyServer(){
		//TODO
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

