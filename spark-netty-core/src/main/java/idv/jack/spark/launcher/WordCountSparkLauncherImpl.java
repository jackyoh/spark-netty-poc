package idv.jack.spark.launcher;


import idv.jack.spark.driver.BootStrapperSparkDriverMain;
import idv.jack.sparknetty.conf.SparkNettyConf;

import org.apache.spark.launcher.SparkLauncher;

public class WordCountSparkLauncherImpl extends AbstractSparkLauncher {

	public WordCountSparkLauncherImpl(SparkNettyConf sparkNettyConf) {
		super(sparkNettyConf);
	}

	@Override
	public SparkLauncher createSparkLauncher() {
		String SPARK_HOME = "/opt/spark-1.6.0-bin-hadoop2.6";
		//String SPARK_HOME = "/opt/spark";
		
		String hostName = this.sparkNettyConf.getNettyHostIP();
		String portNumber = this.sparkNettyConf.getNettyPort();
		
		String appArgs = "hdfs://apache-server-a1:9000/file1.txt," + hostName + "," + portNumber;
		
		SparkLauncher spark = new SparkLauncher()
        				.setSparkHome(SPARK_HOME)
                        .setAppResource("/home/user1/spark-netty-poc/spark-netty-core/build/libs/spark-netty-core.jar")
        				//.setAppResource("/opt/tomcat/webapps/spark-netty-web/WEB-INF/lib/spark-netty-core.jar")
                        .setMainClass(BootStrapperSparkDriverMain.class.getName())
                        .addAppArgs(appArgs)
                        
                        .setMaster("yarn-cluster");
                        //.setMaster("mesos://192.168.1.224:5050");
                       // .setMaster("spark://192.168.1.152:7077");
		return spark;
	}

}
