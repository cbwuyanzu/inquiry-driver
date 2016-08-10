package com.ge.predix.solsvc.api;

import javax.ws.rs.Consumes;
import javax.ws.rs.DefaultValue;
import javax.ws.rs.GET;
import javax.ws.rs.HeaderParam;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.Response;

/**
 *
 * @author predix -
 */
@Consumes(
{
        "application/json", "application/xml"
})
@Produces(
{
        "application/json", "application/xml"
})
@Path("/driver")
public interface InquiryDataAPI
{
	/**
	 * @return -
	 */
	@GET
	@Path("/ping")
	public Response greetings();

	/**
	 * @param id
	 *            -
	 * @param authorization
	 *            -
	 * @param starttime
	 *            -
	 * @param tagLimit -
	 * @param tagorder -
	 * @return -
	 */
	@GET
	@Path("/aggregated_data/{id}")
	public Response getAggregationDataPoints(@PathParam("id") String id,
			@HeaderParam(value = "Authorization") String authorization,
			@DefaultValue("1y-ago") @QueryParam("starttime") String starttime,
			@DefaultValue("10000") @QueryParam("taglimit") String tagLimit,@DefaultValue("asc") @QueryParam("order") String tagorder);
	/**
	 * @param id
	 *            -
	 * @param authorization
	 *            -
	 * @param starttime
	 *            -
	 * @param tagLimit -
	 * @param tagorder -
	 * @return -
	 */
	@GET
	@Path("/raw_data/{id}")
	public Response getRawDataPoints(@PathParam("id") String id,
			@HeaderParam(value = "Authorization") String authorization,
			@DefaultValue("1w-ago") @QueryParam("starttime") String starttime,
			@DefaultValue("10000") @QueryParam("taglimit") String tagLimit,@DefaultValue("asc") @QueryParam("order") String tagorder);

	/**
	 * @param id
	 *            -
	 * @param authorization
	 *            -
	 * @return -
	 */
	@GET
	@Path("/latest_data/{id}")
	public Response getLatestDataPoints(@PathParam("id") String id,
			@HeaderParam(value = "authorization") String authorization);

	/**
	 *
	 * @return List of tags
	 */
	@GET
	@Path("/tags")
	public Response getDataTags();
	/**
	 *
	 * @return List of tags
	 */
}
