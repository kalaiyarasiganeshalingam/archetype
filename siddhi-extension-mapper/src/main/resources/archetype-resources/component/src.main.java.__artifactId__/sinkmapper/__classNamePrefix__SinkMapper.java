package ${package}.${artifactId}.sinkemapper;

import org.wso2.siddhi.annotation.Example;
import org.wso2.siddhi.annotation.Extension;
import org.wso2.siddhi.annotation.Parameter;
import org.wso2.siddhi.annotation.util.DataType;
import org.wso2.siddhi.core.config.SiddhiAppContext;
import org.wso2.siddhi.core.event.Event;
import org.wso2.siddhi.core.stream.output.sink.SinkListener;
import org.wso2.siddhi.core.stream.output.sink.SinkMapper;
import org.wso2.siddhi.core.util.config.ConfigReader;
import org.wso2.siddhi.core.util.transport.OptionHolder;
import org.wso2.siddhi.core.util.transport.TemplateBuilder;
import org.wso2.siddhi.query.api.definition.StreamDefinition;


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
 * {@literal @}Parameter(name = "The name of the first parameter", type = "Supprted parameter types
 *                      eg:{DataType.STRING,DataType.INT, DataType.LONG etc},dynamic=true/false ,
 *                      optinal=true/false ,if optional =true then assign default value according the type format"),
 *   System parameter is used to define common extension wide
 * },
 * add system parameters and describe these extension related configuration
 * examples = {
 * {@literal @}Example({"Example of the CustomExtension contain syntax and description,
 *                      Syntax describe about default mapping and description describe
 *                      the output of according this syntax"}),
 * {@literal @}Example({"Example of the CustomExtension contain syntax and description.Here,
 *                      Syntax describe custom mapping and description describe
 *                      the output of according this syntax "}),
 * )
 * public CustomExtension extends ExtensionSuperClass {
 *      The class to convert a Siddhi message to a CSV message. User can provide a CSV template or else we will be
 *      using a predefined CSV message format.
 * </code></pre>
 */

@Extension(
        name = " ",
        namespace = "sink",
        description = " ",
        parameters = {
                @Parameter(name = " ",
                        description = " " ,
                        /*dynamic = false/true,
                        optional = true/false, defaultValue = " ",
                        type = {DataType.INT, DataType.BOOL, DataType.STRING, DataType.DOUBLE,etc }),*/
                        type = {DataType.INT, DataType.BOOL, DataType.STRING, DataType.DOUBLE, }),
        },
        examples = {
                @Example(
                        syntax = " ",
                        description = " "
                )
        }
)
public class CSVMapper extends SinkMapper {

    /**
     * Returns a list of supported dynamic options (that means for each event value of the option can change) by
     * the transport
     *
     * @return the list of supported dynamic option keys
     */
    @Override
    public String[] getSupportedDynamicOptions() {
        return new String[0];
    }


    /**
     The initialization method for {@link SinkMapper}, which will be called before other methods and validate
     * the all configuration and getting the intial values.
     * @param streamDefinition       containing stream definition bind to the {@link SinkMapper}
     * @param optionHolder           Option holder containing static and dynamic configuration related
     *                               to the {@link SinkMapper}
     * @param payloadTemplateBuilder Unmapped payload for reference
     * @param mapperConfigReader     to read the sink related system configuration.
     * @param siddhiAppContext       the context of the {@link org.wso2.siddhi.query.api.SiddhiApp} used to
     *                               get siddhi related utilty functions.

     */
    @Override
    public void init(StreamDefinition streamDefinition, OptionHolder optionHolder,
                     TemplateBuilder payloadTemplateBuilder, ConfigReader mapperConfigReader,
                     SiddhiAppContext siddhiAppContext) {

    }

    /**
     * Returns the list of classes which this sink can consume.
     * Based on the type of the sink, it may be limited to being able to publish specific type of classes.
     * For example, a {@link SinkMapper} of type event can convert to CSV file objects of type String or byte.
     * @return array of supported classes , if extension can support of any types of classes then return empty array .
     */
    @Override
    public Class[] getOutputEventClasses() {
        return new Class[0];
    }

    /**
     * Method to map the events and send them to {@link SinkListener} for publishing
     *
     * @param events                 {@link Event}s that need to be mapped
     * @param optionHolder           Option holder containing static and dynamic options related to the mapper
     * @param payloadTemplateBuilder To build the message payload based on the given template
     * @param sinkListener           {@link SinkListener} that will be called with the mapped events
     */
    @Override
    public void mapAndSend(Event[] events, OptionHolder optionHolder, TemplateBuilder payloadTemplateBuilder,
                           SinkListener sinkListener) {

    }

    /**
     * Method to map the event and send it to {@link SinkListener} for publishing
     *
     * @param event                  {@link Event} that need to be mapped
     * @param optionHolder           Option holder containing static and dynamic options related to the mapper
     * @param payloadTemplateBuilder To build the message payload based on the given template
     * @param sinkListener           {@link SinkListener} that will be called with the mapped event
     */
    @Override
    public void mapAndSend(Event event, OptionHolder optionHolder, TemplateBuilder payloadTemplateBuilder,
                           SinkListener sinkListener) {

    }
}
