package ${package}.sinkmapper;

import org.apache.log4j.Logger;
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
 * Mapper class to convert a Siddhi message to a CSV message. User can provide a CSV template or else we will be
 * using a predefined CSV message format. In case of null elements xsi:nil="true" will be used. In some instances
 * coding best practices have been compensated for performance concerns.
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
        name = "csv",
        namespace = "sinkMapper",
        description = "This mapper converts Siddhi output events to CSV before they are published via transports " +
                "that publish in CSV format. Users can either send a pre-defined CSV format or a custom CSV message.",
        parameters = {
                @Parameter(name = "delimiter",
                        description =
                                "When convert a Siddhi output events to CSV format, A delimiter is used to " +
                                        "separate the Siddhi output event's parameters.",
                        dynamic = true,
                        optional = true, defaultValue = ",",
                        type = {DataType.STRING})
        },
        examples = {
                @Example(
                        syntax = "@sink(type='inMemory', topic='stock', @map(type='csv'))\n"
                                + "define stream FooStream (symbol string, price float, volume long);",
                        description = "Above configuration will do a default CSV input mapping which will "
                                + "generate below "
                                + "WSO2,55.6,100"
                ),
                @Example(
                        syntax = "@sink(type='inMemory', topic='{{symbol}}', @map(type='csv',\n" +
                                "@payload(\"{{symbol}},{{price}}\")))\n" +
                                "define stream BarStream (symbol string, price float, volume long);",
                        description = "Above configuration will perform a custom CSV mapping which will produce below "
                                + "output CSV message"
                                + "WSO2 , 55.6"
                ),
        }
)
public class ${siddhi_function_name}SinkMapper extends SinkMapper {
    private static final Logger log = Logger.getLogger(${siddhi_function_name}SinkMapper.class);

    /****BEGIN: Sample class variables for Sink Mapper***/
    private static final String DELIMITER = "delimiter";
    private StreamDefinition streamDefinition;
    private String delimiter;
    /****END: Sample class variables for Sink Mapper***/

    /**
     * Returns a list of supported dynamic options (that means for each event value of the option can change) by
     * the transport
     *
     * @return the list of supported dynamic option keys
     */
    @Override
    public String[] getSupportedDynamicOptions() {
        //In this case,Sink Mapper has only one dynamic option: DELIMITER
        //Change this line with appropriate dynamic options for the Sink Mapper
        return new String[]{DELIMITER};
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
        // In this example,Sink Mapper is initialized with parameters.
        // Change this implementation with appropriate static & dynamic parameters,
        // which contains what are the parameters need from{@link SinkMapper}.
        this.streamDefinition = streamDefinition;
        //get dynamic optional parameter and assign this default value
        this.delimiter = optionHolder.getOrCreateOption(DELIMITER, ",").getValue();

    }

    /**
     * Returns the list of classes which this sink can consume.
     * Based on the type of the sink, it may be limited to being able to publish specific type of classes.
     * For example, a {@link SinkMapper} of type event can convert to CSV file objects of type String or byte.
     * @return array of supported classes , if extension can support of any types of classes then return empty array .
     */
    @Override
    public Class[] getOutputEventClasses() {
        // Alter the following line with appropriate classes.
        return new Class[]{Byte[].class, Event.class, String.class};
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
        //In this mapper, the logic is used to CSV Mapper with events. first, get the events and check the mapping type
        // such as custom or default and take the action according the type .finally publish the output.
        //Change the logic according to the Sink Mapper
        if (events.length < 1) {
            log.info("can't get the event");
        }
        StringBuilder builder = new StringBuilder();
        //custom mapping
        if (payloadTemplateBuilder != null) {
            log.info("when custom mapping is enabled then can't use delimiter ");
            for (Event event : events) {
                builder.append(payloadTemplateBuilder.build(event));
                builder.append("\n");
            }
        } else {
            for (Event event : events) {
                builder.append(constructDefaultMapping(event));
            }
        }
        sinkListener.publish(builder.toString());
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
        //In this example, the logic is written to CSV Mapper to event.first, get the event and check the mapping
        // type such as custom or default and take the action according the type finally publish the output.
        //Change the logic according to the Sink Mapper
        StringBuilder builder = new StringBuilder();
        if (payloadTemplateBuilder != null) {   //custom mapping
            log.info("when custom mapping is enabled then can't use delimiter ");
            builder.append(payloadTemplateBuilder.build(event));
            builder.append("\n");
        } else {
            builder.append(constructDefaultMapping(event));
        }
        sinkListener.publish(builder.toString());
    }

    /*******BEGIN:Sample private method for Sink CSV Mapper*******/
    /**
     * Method used to default map the event
     *
     * @param event The {@link Event} that need to be mapped
     * @return The string ,which contains mapped event in CSV format.
     */
    private String constructDefaultMapping(Event event) {
        StringBuilder builder = new StringBuilder();
        Object[] data = event.getData();
        for (int i = 0; i < data.length; i++) {
            if (data[i] != null) {
                builder.append(data[i].toString());
                if (i < (data.length - 1)) {
                    builder.append(delimiter);
                }
            } else {
                builder.append("null");
                if (i < (data.length - 1)) {
                    builder.append(delimiter);
                }
            }
        }
        builder.append("\n");
        return builder.toString();
    }
    /*******END:Sample private method for Sink Mapper*******/
}
