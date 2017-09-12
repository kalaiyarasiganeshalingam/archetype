package ${package}.streamfunction;

import com.google.code.geocoder.Geocoder;
import com.google.code.geocoder.GeocoderRequestBuilder;
import com.google.code.geocoder.model.GeocodeResponse;
import com.google.code.geocoder.model.GeocoderRequest;
import org.apache.log4j.Logger;
import org.wso2.siddhi.annotation.Example;
import org.wso2.siddhi.annotation.Extension;
/*import org.wso2.siddhi.annotation.Parameter;
import org.wso2.siddhi.annotation.ReturnAttribute;
import org.wso2.siddhi.annotation.util.DataType;*/
import org.wso2.siddhi.core.config.SiddhiAppContext;
import org.wso2.siddhi.core.exception.SiddhiAppCreationException;
import org.wso2.siddhi.core.exception.SiddhiAppRuntimeException;
import org.wso2.siddhi.core.executor.ExpressionExecutor;
import org.wso2.siddhi.core.query.processor.stream.function.StreamFunctionProcessor;
import org.wso2.siddhi.core.util.config.ConfigReader;
import org.wso2.siddhi.query.api.definition.AbstractDefinition;
import org.wso2.siddhi.query.api.definition.Attribute;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * This is a sample class-level comment, explaining what the Sink extension class does.
 */

/**
 * Annotation of Siddhi Extension.
 * <pre><code>
 * eg:-
 * {@literal @}Extension(
 * name = "The name of the extension",
 * namespace = "The namespace of the extension",
 * description = "The description of the extension (optional).",
 * //Sink configurations
 * parameters = {
 * {@literal @}Parameter(name = "The name of the first parameter",
 *                               description= "The description of the first parameter",
 *                               type = "Supported parameter types.
 *                                      eg:{DataType.STRING, DataType.INT, DataType.LONG etc}",
 *                               dynamic= "true/false
 *                                      (if parameter depends on each event then it is a dynamic parameter.)",
 *                               optional= "true/false, defaultValue = if it is optional then assign a default value
 *                               according to the type."),
 * {@literal @}Parameter(name = "The name of the second parameter",
 *                               description= "The description of the second parameter",
 *                               type= "Supported parameter types."
 *                                      eg:{DataType.STRING, DataType.INT, DataType.LONG etc},
 *                               dynamic=true/false ,
 *                               optional= "true/false ,
 *                               defaultValue= if it is optional then assign a default value
 *                               according to the type."
 *                               ),
 * },
 * //If Sink system configurations will need then
 * systemParameters = {
 * {@literal @}SystemParameter(name = "The name of the first system parameter",
 *                                      description="The description of the first system parameter." ,
 *                                      defaultValue = "The default value of the system parameter.",
 *                                      possibleParameter="The possible value of the system parameter.",
 *                               ),
 * },
 * examples = {
 * {@literal @}Example(syntax= "sample query with Sink annotation that explain how extension use in Siddhi."
 *                     description =" The description of the given example's query."
 *                      ),
 * }
 * )
 * </code></pre>
 */

@Extension(
        name = "geocoder",
        namespace = "custom",
        description = "The geocoder function converting addresses into geographic coordinates, " +
                " and adding them as new attributes to the existing events.",
        parameters = {},
        examples = {
                @Example(
                        syntax = "@info(name = 'query') " +
                                "from geocodeStream#custom:streamfunction(location)" +
                                "select latitude, longitude, formattedAddress " +
                                "insert into dataOut;",
                        description = "This will return geocoder's data such as latitude, longitude, formattedAddress"
                                + "according the given location")
        }
)

public class ${siddhi_function_name}StreamFunctionProcessorExtension extends StreamFunctionProcessor {
    private static final Logger log = Logger.
        getLogger(${siddhi_function_name}StreamFunctionProcessorExtension.class);
    private final Geocoder geocoder = new Geocoder();
    private boolean debugModeOn;

    /**
     * The process method used when more than one function parameters are provided
     *
     * @param data the data values for the function parameters
     * @return the data for additional output attributes introduced by the function
     */
    @Override
    protected Object[] process(Object[] data) {

        return process(data[0]);
    }

    /**
     * The process method used when zero or one function parameter is provided
     *
     * @param data null if the function parameter count is zero or runtime data value of the function parameter
     * @return the data for additional output attribute introduced by the function
     */
    @Override
    protected Object[] process(Object data) {

        String location = data.toString();
        // Make the geocode request to API library
        GeocoderRequest geocoderRequest = new GeocoderRequestBuilder()
                .setAddress(location)
                .setLanguage("en")
                .getGeocoderRequest();
        double latitude, longitude;
        String formattedAddress;
        try {
            GeocodeResponse geocoderResponse = geocoder.geocode(geocoderRequest);
            if (!geocoderResponse.getResults().isEmpty()) {
                latitude = geocoderResponse.getResults().get(0).getGeometry().getLocation()
                        .getLat().doubleValue();
                longitude = geocoderResponse.getResults().get(0).getGeometry().getLocation()
                        .getLng().doubleValue();
                formattedAddress = geocoderResponse.getResults().get(0).getFormattedAddress();
            } else {
                latitude = -1.0;
                longitude = -1.0;
                formattedAddress = "N/A";
            }
        } catch (Exception e) {
            throw new SiddhiAppRuntimeException("Error in connection to Google Maps API.", e);
        }
        if (debugModeOn) {
            log.debug("Formatted address: " + formattedAddress + ", Location coordinates: (" +
                    latitude + ", " + longitude + ")");
        }
        return new Object[]{formattedAddress, latitude, longitude};
    }

    /**
     * The initialization method for {@link StreamFunctionProcessor},
     * which will be called before other methods and validate
     * the all configuration and getting the intial values.
     * @param abstractDefinition               the incoming stream definition
     * @param attributeExpressionExecutors     the executors of each function parameters
     * @param configReader                     this hold the {@link StreamFunctionProcessor} extensions
     *                                         configuration reader.
     * @param siddhiAppContext                 the context of the siddhi app
     * @return the output's additional attributes list introduced by the function
     */
    @Override
    protected List<Attribute> init(AbstractDefinition abstractDefinition,
                                   ExpressionExecutor[] attributeExpressionExecutors,
                                   ConfigReader configReader, SiddhiAppContext siddhiAppContext) {
        debugModeOn = log.isDebugEnabled();
        if (attributeExpressionExecutors[0].getReturnType() != Attribute.Type.STRING) {
            throw new SiddhiAppCreationException("First parameter should be of type string");
        }
        ArrayList<Attribute> attributes = new ArrayList<Attribute>(6);
        attributes.add(new Attribute("formattedAddress", Attribute.Type.STRING));
        attributes.add(new Attribute("latitude", Attribute.Type.DOUBLE));
        attributes.add(new Attribute("longitude", Attribute.Type.DOUBLE));
        log.info(attributes);

        return attributes;
    }


    /**
     * This will be called only once and this can be used to acquire
     * required resources for the processing element.
     * This will be called after initializing the system and before
     * starting to process the events.
     */
    @Override
    public void start() {

    }

    /**
     * This will be called only once and this can be used to release
     * the acquired resources for processing.
     * This will be called before shutting down the system.
     */
    @Override
    public void stop() {

    }

    /**
     * Used to collect the serializable state of the processing element, that need to be
     * persisted for reconstructing the element to the same state on a different point of time
     *
     * @return stateful objects of the processing element as an map
     */
    @Override
    public Map<String, Object> currentState() {
        Map<String, Object> state = new HashMap<>();
        return state;
    }

    /**
     * Used to restore serialized state of the processing element, for reconstructing
     * the element to the same state as if was on a previous point of time.
     *
     * @param map the stateful objects of the processing element as a map.
     *              This is the same map that is created upon calling currentState() method.
     */
    @Override
    public void restoreState(Map<String, Object> map) {

    }
}
