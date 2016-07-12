package idv.jack.spark.launcher;

import org.junit.Test;

public class WordCountSparkLauncherTest {
	
	@Test
	public void testSubmit() throws Exception{
		AbstractSparkLauncher launcher = new WordCountSparkLauncherImpl();
		launcher.launch();
	}

}
