package idv.jack.spark.launcher;

import idv.jack.sparknetty.conf.SparkNettyConf;

import org.junit.Test;

public class WordCountSparkLauncherTest {
	
	@Test
	public void testSubmit() throws Exception{
		SparkNettyConf sparkNettyConf = new SparkNettyConf();
		sparkNettyConf.setNettyHostName("192.168.1.16");
		sparkNettyConf.setNettyPort("1234");
		
		AbstractSparkLauncher launcher = new WordCountSparkLauncherImpl(sparkNettyConf);
		launcher.launch();
	}

}
