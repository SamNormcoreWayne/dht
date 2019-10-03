package edu.stevens.cs549.dhts.main;

import java.net.URI;
import java.util.logging.Logger;

import javax.ws.rs.client.Client;
import javax.ws.rs.client.ClientBuilder;
import javax.ws.rs.client.Entity;
import javax.ws.rs.core.GenericType;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.UriBuilder;
import javax.xml.bind.JAXBElement;

import com.ning.http.client.uri.Uri;
import edu.stevens.cs549.dhts.activity.DHTBase;
import edu.stevens.cs549.dhts.activity.NodeInfo;
import edu.stevens.cs549.dhts.resource.TableRep;
import edu.stevens.cs549.dhts.resource.TableRow;
import jdk.nashorn.internal.runtime.regexp.joni.constants.StringType;

public class WebClient {
	private Logger log = Logger.getLogger(WebClient.class.getCanonicalName());

	private void error(String msg) {
		log.severe(msg);
	}

	/*
	 * Encapsulate Web client operations here.
	 * 
	 * TODO: Fill in missing operations. Done by Sam
	 */

	/*
	 * Creation of client instances is expensive, so just create one.
	 */
	protected Client client;
	
	public WebClient() {
		client = ClientBuilder.newClient();
	}

	private void info(String mesg) {
		Log.info(mesg);
	}

	private Response getRequest(URI uri) {
		try {
			Response cr = client.target(uri)
					.request(MediaType.APPLICATION_XML_TYPE)
					.header(Time.TIME_STAMP, Time.advanceTime())
					.get();
			processResponseTimestamp(cr);
			return cr;
		} catch (Exception e) {
			error("Exception during GET request: " + e);
			return null;
		}
	}

	private Response putRequest(URI uri, Entity<?> entity) {
		// TODO by Sam
		try {
			Response res = client.target(uri)
					.request(MediaType.APPLICATION_ATOM_XML_TYPE)
					.header(Time.TIME_STAMP, Time.advanceTime())
					.put(entity);
			processResponseTimestamp(res);
			return res;
		} catch (Exception err) {
			error("Exception during PUT request: "+ err);
			return null;
		}
	}
	private Response deleteRequest(URI uri) {
		try {
			Response res = client.target(uri)
					.request(MediaType.APPLICATION_ATOM_XML_TYPE)
					.header(Time.TIME_STAMP, Time.advanceTime())
					.delete();
			processResponseTimestamp(res);
			return res;
		} catch (Exception err) {
			error("Exception during DELETE: " + err);
			return null;
		}
	}
	private Response putRequest(URI uri) {
		return putRequest(uri, Entity.text(""));
	}

	private void processResponseTimestamp(Response cr) {
		Time.advanceTime(Long.parseLong(cr.getHeaders().getFirst(Time.TIME_STAMP).toString()));
	}

	/*
	 * Jersey way of dealing with JAXB client-side: wrap with run-time type
	 * information.
	 */
	private GenericType<JAXBElement<NodeInfo>> nodeInfoType = new GenericType<JAXBElement<NodeInfo>>() {
	};
	private GenericType<JAXBElement<TableRow>> tableRowType = new GenericType<JAXBElement<TableRow>>() {};
	private GenericType<JAXBElement<String[]>> stringType = new GenericType<JAXBElement<String[]>>() {};
	/*
	 * Ping a remote site to see if it is still available.
	 */
	public boolean isFailed(URI base) {
		URI uri = UriBuilder.fromUri(base).path("info").build();
		Response c = getRequest(uri);
		return c.getStatus() >= 300;
	}

	/*
	 * Get the successor pointer at node. by Sam
	 */
	public NodeInfo getSucc(NodeInfo node) throws DHTBase.Failed {
		URI succPath = UriBuilder.fromUri(node.addr).path("succ").build();
		info("client getSucc(" + succPath + ")");
		Response res = getRequest(succPath);
		if (res == null || res.getStatus() >= 300) {
			throw new DHTBase.Failed("GET /succ");
		} else {
			NodeInfo succ = res.readEntity(nodeInfoType).getValue();
			return succ;
		}
	}
	/*
	 * Get the predecessor pointer at a node.
	 */
	public NodeInfo getPred(NodeInfo node) throws DHTBase.Failed {
		URI predPath = UriBuilder.fromUri(node.addr).path("pred").build();
		info("client getPred(" + predPath + ")");
		Response response = getRequest(predPath);
		if (response == null || response.getStatus() >= 300) {
			throw new DHTBase.Failed("GET /pred");
		} else {
			NodeInfo pred = response.readEntity(nodeInfoType).getValue();
			return pred;
		}
	}
	/*
	 * Closest Preceding Finger. Add by Sam
	 */
	public NodeInfo getClosestPrecedingFinger(NodeInfo node, int id) throws DHTBase.Failed {
		UriBuilder closestPath = UriBuilder.fromUri(node.addr).path("closestPrecedingFinger");
		URI getQuery = closestPath.queryParam("id", id).build();
		info("client closestPrecedingFinger(" + getQuery + ")");
		Response res = getRequest(getQuery);
		if (res == null || res.getStatus() >= 300) {
			throw new DHTBase.Failed("GET /closestPrecedingFinger?id=ID");
		} else {
			return res.readEntity(nodeInfoType).getValue();
		}
	}

	public NodeInfo findSuccessor(URI uri, int id) throws DHTBase.Failed {
		UriBuilder ub = UriBuilder.fromUri(uri).path("find");
		URI findSuccPath = ub.queryParam("id", id).build();
		info ("client findSuccessor (" + findSuccPath + ")");
		Response res = getRequest(findSuccPath);
		if (res == null || res.getStatus() >= 300) {
			throw new DHTBase.Failed("GET /find?id=ID");
		} else {
			return res.readEntity(nodeInfoType).getValue();
		}
	}

	/*
	 * Notify node that we (think we) are its predecessor.
	 */
	public TableRep notify(NodeInfo node, TableRep predDb) throws DHTBase.Failed {
		/*
		 * The protocol here is more complex than for other operations. We
		 * notify a new successor that we are its predecessor, and expect its
		 * bindings as a result. But if it fails to accept us as its predecessor
		 * (someone else has become intermediate predecessor since we found out
		 * this node is our successor i.e. race condition that we don't try to
		 * avoid because to do so is infeasible), it notifies us by returning
		 * null. This is represented in HTTP by RC=304 (Not Modified).
		 */
		NodeInfo thisNode = predDb.getInfo();
		UriBuilder ub = UriBuilder.fromUri(node.addr).path("notify");
		URI notifyPath = ub.queryParam("id", thisNode.id).build();
		info("client notify(" + notifyPath + ")");
		Response response = putRequest(notifyPath, Entity.xml(predDb));
		if (response != null && response.getStatusInfo() == Response.Status.NOT_MODIFIED) {
			/*
			 * Do nothing, the successor did not accept us as its predecessor.
			 */
			return null;
		} else if (response == null || response.getStatus() >= 300) {
			throw new DHTBase.Failed("PUT /notify?id=ID");
		} else {
			TableRep bindings = response.readEntity(TableRep.class);
			return bindings;
		}
	}
	/*
	 * Add by Sam
	 */
	public String [] get(NodeInfo node, String key) throws DHTBase.Failed {
		UriBuilder ub = UriBuilder.fromUri(node.addr);
		URI getKey = ub.queryParam("key", key).build();
		info ("client get(" + getKey + ")");
		Response res = getRequest(getKey);
		if (res == null || res.getStatus() >= 300) {
			throw new DHTBase.Failed("GET ?key=KEY");
		} else {
			return res.readEntity(tableRowType).getValue().vals;
		}
	}

	/* duplicated with findSucc
	public NodeInfo get(NodeInfo node, int id) throws DHTBase.Failed {
		UriBuilder ub = UriBuilder.fromUri(node.addr).path("/find");
		URI getSuccByID = ub.queryParam("id", id).build();
		info ("client notify (" + getSuccByID + ")");
		Response res = getRequest(getSuccByID);
		if (res == null || res.getStatus() >= 300) {
			throw new DHTBase.Failed("GET /find?id=ID");
		} else {
			return res.readEntity(nodeInfoType).getValue();
		}
	}
	*/
	public String [] get(NodeInfo node) throws DHTBase.Failed {
		URI getInfo = UriBuilder.fromUri(node.addr).path("info").build();
		info ("client get (" + getInfo + ")");
		Response res = getRequest(getInfo);
		if (res == null || res.getStatus() >= 300){
			throw new DHTBase.Failed("GET /info");
		} else {
			return res.readEntity(stringType).getValue();
		}
	}

	public void add(NodeInfo node, String key, String val) throws DHTBase.Failed {
		UriBuilder ub = UriBuilder.fromUri(node.addr);
		URI addNode = ub.queryParam("key", key).queryParam("val", val).build();
		info("client add (" + addNode + ")");
		TableRep tRep;
		/*
		try {
			NodeInfo succ = this.getSucc(node);
			tRep = new TableRep(node, succ, 1);
			tRep.entry[0] = new TableRow(key, new String[]{val});
			Response res = putRequest(addNode, Entity.xml(tRep));
			if (res == null || res.getStatus() >= 300)
			{
				throw new DHTBase.Failed("PUT ?key=KEY&?val=VAL");
			}
		} catch (DHTBase.Failed dhtErr) {
			tRep = new TableRep(node, null, 1);
			tRep.entry[0] = new TableRow(key, new String[]{val});
			Response res = putRequest(addNode, Entity.xml(tRep));
			if (res == null || res.getStatus() >= 300)
			{
				throw new DHTBase.Failed("PUT ?key=KEY&?val=VAL");
			}
		}
		*/
		NodeInfo succ = this.getSucc(node);
		if (succ != null)
		{
			tRep = new TableRep(node, succ, 1);
			tRep.entry[0] = new TableRow(key, new String[]{val});
			Response res = putRequest(addNode, Entity.xml(tRep));
			if (res == null || res.getStatus() >= 300)
			{
				throw new DHTBase.Failed("PUT ?key=KEY&?val=VAL");
			}
		} else {
			tRep = new TableRep(node, null, 1);
			tRep.entry[0] = new TableRow(key, new String[]{val});
			Response res = putRequest(addNode, Entity.xml(tRep));
			if (res == null || res.getStatus() >= 300)
			{
				throw new DHTBase.Failed("PUT ?key=KEY&?val=VAL");
			}
		}
	}

	public void delete(NodeInfo node, String key, String val) throws DHTBase.Failed {
		UriBuilder ub = UriBuilder.fromUri(node.addr);
		URI deleteNode = ub.queryParam("key", key).queryParam("val", val).build();
		info("client delete (" + deleteNode + ")");
		Response res = deleteRequest(deleteNode);
		if (res == null || res.getStatus() >= 300)
		{
			throw new DHTBase.Failed(" DELETE ?key=KEY&val=VAL");
		}
	}
}
