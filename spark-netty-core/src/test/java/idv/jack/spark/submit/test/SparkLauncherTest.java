package idv.jack.spark.submit.test;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;

import org.apache.spark.launcher.SparkLauncher;
import org.junit.Test;

public class SparkLauncherTest {

	@Test
	public void testSubmit() throws Exception{
		//String SPARK_HOME = System.getenv("SPARK_HOME");
		String SPARK_HOME = "/opt/spark-1.6.0-bin-hadoop2.6";
		
		
		/*Process spark = new SparkLauncher()
	                          .setSparkHome(SPARK_HOME)
	                          .setAppResource(SPARK_HOME + "/lib/spark-examples-1.6.0-hadoop2.6.0.jar")
	                          .setMainClass("org.apache.spark.examples.SparkPi").setMaster("yarn-cluster").launch();
		*/
		Process spark = new SparkLauncher()
			        .setSparkHome(SPARK_HOME)
			        .setAppResource("/home/user1/spark-netty-poc/spark-netty-core/build/libs/spark-netty-core.jar")
			        .setMainClass("idv.jack.spark.driver.SparkDriverWordCount").setMaster("yarn-cluster").launch();
		
		InputStreamReaderRunnable inputStreamReaderRunnable = new InputStreamReaderRunnable(spark.getInputStream(), "input");
		Thread inputThread = new Thread(inputStreamReaderRunnable, "LogStreamReader input");
		inputThread.start();
	
		InputStreamReaderRunnable errorStreamReaderRunnable = new InputStreamReaderRunnable(spark.getErrorStream(), "error");
		Thread errorThread = new Thread(errorStreamReaderRunnable, "LogStreamReader error");
		errorThread.start();
	
		System.out.println("Waiting for finish...");
		int exitCode = spark.waitFor();
		System.out.println("Finished! Exit code:" + exitCode);
		
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
