package idv.jack.sparknetty.facade.submit;



import idv.jack.spark.launcher.AbstractSparkLauncher;
import idv.jack.spark.launcher.WordCountSparkLauncherImpl;
import idv.jack.sparknetty.conf.SparkNettyConf;

import java.util.List;

public class WordCountSubmitFacade {

	public WordCountSubmitFacade(){
		
	}
	
	public String submit(){
		try {
			SparkNettyConf sparkNettyConf = new SparkNettyConf();
			//sparkNettyConf.setNettyHostIP("192.168.1.16");
			//sparkNettyConf.setNettyPort("1234");
			
			AbstractSparkLauncher launcher = new WordCountSparkLauncherImpl(sparkNettyConf);
			List<String> result = launcher.launch();
			String str = "";
			for(String r : result){
				str = r;
			}
			return str;
		}catch(Exception e){
			throw new RuntimeException(e);
		}
	}
}
