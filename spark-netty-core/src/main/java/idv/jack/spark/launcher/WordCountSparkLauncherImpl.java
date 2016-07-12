package idv.jack.spark.launcher;


import idv.jack.spark.driver.BootStrapperSparkDriverMain;
import idv.jack.spark.driver.WordCountSparkDriver;

import org.apache.spark.launcher.SparkLauncher;

public class WordCountSparkLauncherImpl extends AbstractSparkLauncher {

	@Override
	public SparkLauncher createSparkLauncher() {
		String SPARK_HOME = "/opt/spark-1.6.0-bin-hadoop2.6";
		
		SparkLauncher spark = new SparkLauncher()
        				.setSparkHome(SPARK_HOME)
                        .setAppResource("/home/user1/spark-netty-poc/spark-netty-core/build/libs/spark-netty-core.jar")
                        .setMainClass(BootStrapperSparkDriverMain.class.getName())
                        .addAppArgs("hdfs://apache-server-a1:9000/file1.txt 192.168.1.16 1234")
                        .setMaster("yarn-cluster");
		
		return spark;
	}

}
