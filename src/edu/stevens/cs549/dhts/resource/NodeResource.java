package edu.stevens.cs549.dhts.resource;

import java.util.logging.Logger;

import javax.ws.rs.Consumes;
import javax.ws.rs.DELETE;
import javax.ws.rs.GET;
import javax.ws.rs.PUT;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.*;

@Path("/dht")
public class NodeResource {

	/*
	 * Web service API.
	 * 
	 * TODO: Fill in the missing operations. Added by Sam
	 */

	Logger log = Logger.getLogger(NodeResource.class.getCanonicalName());
	@Context
	UriInfo uriInfo;

	@Context
	HttpHeaders headers;

	@GET
	@Path("info")
	@Produces("application/xml")
	public Response getNodeInfoXML() {
		return new NodeService(headers, uriInfo).getNodeInfo();
	}

	@GET
	@Path("info")
	@Produces("application/json")
	public Response getNodeInfoJSON() {
		return new NodeService(headers, uriInfo).getNodeInfo();
	}

	@GET
	@Path("pred")
	@Produces("application/xml")
	public Response getPred() {
		return new NodeService(headers, uriInfo).getPred();
	}

	@GET
	@Path("finger")
	@Produces("application/xml")
	public Response getClosestPrecedingFinger(@QueryParam("id") int id) {
		return new NodeService(headers, uriInfo).closestPrecedingFinger(id);
	}

	@GET
	@Path("succ")
	@Produces("application/xml")
	public Response getSucc() {
		return new NodeService(headers, uriInfo).getSucc();
	}

	@PUT
	@Path("notify")
	@Consumes("application/xml")
	@Produces("application/xml")
	/*
	 * Actually returns a TableRep (annotated with @XmlRootElement)
	 */
	public Response putNotify(TableRep predDb) {
		/*
		 * See the comment for WebClient::notify (the client side of this logic).
		 */
		return new NodeService(headers, uriInfo).notify(predDb);
		// NodeInfo p = predDb.getInfo();
	}

	@GET
	@Path("find")
	@Produces("application/xml")
	public Response findSuccessor(@QueryParam("id") String index) {
		int id = Integer.parseInt(index);
		return new NodeService(headers, uriInfo).findSuccessor(id);
	}

	@GET
	@Produces("application/xml")
	public Response getBinding(@QueryParam("key") String key) {
		return new NodeService(headers, uriInfo).getBinding(key);
	}

	@PUT
	@Produces("application/xml")
	public Response putBinding(@QueryParam("key") String key, @QueryParam("val") String val) {
		return new NodeService(headers, uriInfo).addBinding(key, val);
	}

	@DELETE
	@Produces("application/xml")
	public Response deleteBinding(@QueryParam("key") String key, @QueryParam("val") String val) {
		return new NodeService(headers, uriInfo).deleteBinding(key, val);
	}
}
