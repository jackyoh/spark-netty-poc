package idv.jack.spark.driver;

public class BootStrapperSparkDriverMain {
	
	public static void main(String args[]) throws Exception{
		
		WordCountSparkDriver driver = (WordCountSparkDriver)Thread.currentThread()
				.getContextClassLoader()
				.loadClass(WordCountSparkDriver.class.getName())
				.newInstance();
		
		driver.run();
	}

}
