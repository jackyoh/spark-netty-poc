package idv.jack.spark.launcher;

import java.util.List;

import idv.jack.sparknetty.conf.SparkNettyConf;

import org.junit.Test;

public class WordCountSparkLauncherTest {
	
	@Test
	public void testSubmit() throws Exception{
		SparkNettyConf sparkNettyConf = new SparkNettyConf();
		
		AbstractSparkLauncher launcher = new WordCountSparkLauncherImpl(sparkNettyConf);
		List<String> result = launcher.launch();

		for(String r : result){
			System.out.println(r);
		}
	}

}
