package idv.jack.spark.netty.resource;

import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;



@Path("submit")
public class SparkSubmitJobResource {

	@GET
	@Path("job")
	@Produces(MediaType.APPLICATION_JSON)
	public String submitJob(){
		return "aaaaaaaa";
	}
}
