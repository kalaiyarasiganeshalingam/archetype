package ${package}.sourcemapper;

import org.apache.log4j.Logger;
import org.wso2.siddhi.annotation.Example;
import org.wso2.siddhi.annotation.Extension;
import org.wso2.siddhi.annotation.Parameter;
import org.wso2.siddhi.annotation.util.DataType;
import org.wso2.siddhi.core.config.SiddhiAppContext;
import org.wso2.siddhi.core.event.Event;
import org.wso2.siddhi.core.exception.SiddhiAppRuntimeException;
import org.wso2.siddhi.core.stream.input.source.AttributeMapping;
import org.wso2.siddhi.core.stream.input.source.InputEventHandler;
import org.wso2.siddhi.core.stream.input.source.SourceMapper;
import org.wso2.siddhi.core.util.AttributeConverter;
import org.wso2.siddhi.core.util.config.ConfigReader;
import org.wso2.siddhi.core.util.transport.OptionHolder;
import org.wso2.siddhi.query.api.definition.Attribute;
import org.wso2.siddhi.query.api.definition.StreamDefinition;
import org.wso2.siddhi.query.api.exception.SiddhiAppValidationException;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
/**
 * This is a sample class-level comment, explaining what the extension class does.
 */

/**
 * Annotation of Siddhi Extension.
 * <pre><code>
 * eg:-
 * {@literal @}Extension(
 * name = "The name of the extension",
 * namespace = "The namespace of the extension",
 * description = "The description of the extension (optional).",
 * //Source configurations
 * parameters = {
 * {@literal @}Parameter(name = "The name of the first parameter", type = "Supprted parameter types.
 *                              eg:{DataType.STRING,DataType.INT, DataType.LONG etc},dynamic=false ,optinal=true/false ,
 *                              if optional =true then assign default value according the type"),
 * {@literal @}Parameter(name = "SecondParameterName", type = "Supprted parameter types.
 *                              eg:{DataType.STRING,DataType.INT, DataType.LONG etc},dynamic=false ,optinal=true/false ,
 *                              if optional =true then assign default value according the type")
 *   System parameter is used to define common extension wide
 * },
 * examples = {
 * {@literal @}Example({"Example of the first CustomExtension contain syntax and description.Here,
 *                      Syntax describe default mapping for SourceMapper and description describes
 *                      the output of according this syntax},
 * {@literal @}Example({"Example of the second CustomExtension contain syntax and description.Here,
 *                      Syntax describe custom mapping for SourceMapper and description describes
 *                      the output of according this syntax})
 *                      },
 * public CustomExtension extends ExtensionSuperClass {
 *      This class implements to convert CSV string input to {@link org.wso2.siddhi.core.event.ComplexEventChunk}
 *      }.
 * </code></pre>
 */

@Extension(
        name = "csv",
        namespace = "sourceMapper",
        description = "CSV to Event input mapper. Transports which accepts CSV messages can utilize this extension"
                + "to convert the incoming CSV message to Siddhi event. Users can either send a pre-defined CSV "
                + "format or custom CSV message.",
        parameters = {
                @Parameter(name = "delimiter",
                        description =
                                "Before convert a CSV format message to Siddhi event, A CSV message parameters " +
                                        "separated by delimeter then create the the event these parameter." ,
                        dynamic = false,
                        optional = true, defaultValue = "," ,
                        type = {DataType.STRING}),
                @Parameter(name = "fail.on.unknown.attribute",
                        description = "This can either have value true or false. By default it will be true. This "
                                + "attribute allows user to handle unknown attributes. By default if an execution "
                                + "fails or returns null system will drop that message. However setting this property"
                                + " to false will prompt system to send and event with null value to Siddhi where user "
                                + "can handle"
                                + " it accordingly(ie. Assign a default value)"
                                + "default value is \"true\"",
                        dynamic = false,
                        optional = true, defaultValue = "true",
                        type = {DataType.BOOL})
        },
        examples = {
                @Example(
                        syntax = "@source(type='inMemory', topic='stock', @map(type='csv'))\n"
                                + "define stream FooStream (symbol string, price float, volume long);\n",
                        description = "Above configuration will do a default CSV input mapping. Expected "
                                + "input will look like below."
                                + " WSO2 ,55.6 , 100\n"
                ),
                @Example(
                        syntax = "@source(type='inMemory', topic='stock', @map(type='csv', "
                                + "@attributes(symbol = \"2\", price = \"0\", volume = \"1\")))",
                        description = "Above configuration will perform a customCSV mapping. Expected input will "
                                + "look like below."
                                + "55.6,100,WSO2\n"
                ),
        }
)


public class ${siddhi_function_name}SourceMapper extends SourceMapper {
    /****BEGIN: Class variables for Source Mapper***/
    public static final String OPTIONAL_MAPPING_DELIMETER = "delimiter";
    private static final Logger log = Logger.getLogger(${siddhi_function_name}SourceMapper.class);
    private static final String FAIL_ON_UNKNOWN_ATTRIBUTE = "fail.on.unknown.attribute";

    private boolean isCustomMappingEnabled = false;
    private StreamDefinition streamDefinition;
    private boolean failOnUnknownAttribute;
    private String delimiter;
    private AttributeConverter attributeConverter = new AttributeConverter();
    private Map<String, String> map = new HashMap<>();
    private List<Attribute> attributeList;
    private List<AttributeMapping> attributeMappingList;
    private Map<String, Attribute.Type> attributeTypeMap = new HashMap<>();
    private Map<String, Integer> attributePositionMap = new HashMap<>();
    /****END: Class variables for Source Mapper***/

    /**
     * The initialization method for {@link SourceMapper}, which will be called before other methods and validate
     * the all configuration and getting the intial values.
     *
     * @param streamDefinition     Associated output stream definition
     * @param optionHolder         Option holder containing static configuration related to the {@link SourceMapper}
     * @param attributeMappingList Custom attribute mapping for source-mapping
     * @param configReader         to read the {@link SourceMapper} related system configuration.
     * @param siddhiAppContext     the context of the {@link org.wso2.siddhi.query.api.SiddhiApp} used to get siddhi
     */
    @Override
    public void init(StreamDefinition streamDefinition, OptionHolder optionHolder, List<AttributeMapping>
            attributeMappingList, ConfigReader configReader, SiddhiAppContext siddhiAppContext) {

        // In this example,Source Mapper is initialized with parameters
        // Change this implementation with appropriate parameters,
        // which contains what are the parameters need from{@link SourceMapper}.
        this.streamDefinition = streamDefinition;
        this.attributeList = streamDefinition.getAttributeList();
        this.attributeTypeMap = new HashMap<>(attributeList.size());
        this.attributePositionMap = new HashMap<>(attributeList.size());
        this.map = new HashMap<>(attributeList.size());

        this.delimiter = optionHolder.getOrCreateOption(OPTIONAL_MAPPING_DELIMETER, ",").getValue();
        this.failOnUnknownAttribute = Boolean.parseBoolean(optionHolder.validateAndGetStaticValue(
                FAIL_ON_UNKNOWN_ATTRIBUTE, "true"));
        for (Attribute attribute : attributeList) {
            attributeTypeMap.put(attribute.getName(), attribute.getType());
            attributePositionMap.put(attribute.getName(), streamDefinition.getAttributePosition(attribute.getName()));
        }

        if (attributeMappingList != null && attributeMappingList.size() > 0) {
            isCustomMappingEnabled = true;
            if (streamDefinition.getAttributeList().size() < attributeMappingList.size()) {
                throw new SiddhiAppValidationException("Stream: '" + streamDefinition.getId() + "' has "
                        + streamDefinition.getAttributeList().size() + " attributes, but " + attributeMappingList.size()
                        + " attribute mappings found. Each attribute should have one and only one mapping.");
            }
            this.attributeMappingList = attributeMappingList;
        }
    }

    /**
     * Returns the list of classes which this source can output.
     *
     * @return Array of classes that will be output by the source.
     * Null or empty array if it can produce any type of class.
     */
    @Override
    public Class[] getSupportedInputEventClasses() {
        // Alter the following line with appropriate classes.
        return new Class[]{Byte[].class, Event.class, String.class};
    }

    /**
     * Method to map the incoming event and as pass that via inputEventHandler to process further.
     *
     * @param eventObject           Incoming event Object based on the supported event class imported by the extensions.
     * @param inputEventHandler     Handler to pass the converted Siddhi Event for processing
     * @throws InterruptedException if it does not throw the exception immediately due to streaming
     */
    @Override
    protected void mapAndProcess(Object eventObject, InputEventHandler inputEventHandler) throws InterruptedException {
        //The logic is written to map the incoming event and as pass that via inputEventHandler.
        //Change the logic according to the SourceMapper.
        Event[] result;
        try {
            result = convertToEvents(eventObject);
            if (result.length > 0) {
                inputEventHandler.sendEvents(result);
            }
        } catch (Throwable t) {
            log.error("Exception occurred when converting CSV message to Siddhi Event", t);
        }
    }

    /*******BEGIN:Private method for Source Mapper*******/

    /**
     * Method to convert an Object to an event array
     *
     * @param eventObject Incoming event Object
     * @return Event array
     */
    private Event[] convertToEvents(Object eventObject) {
        //The logic is written to convert the object to event array.
        //Change the logic according to the Source Mapper
        List<Event> eventList = new ArrayList<>();
        boolean isMalformedEvent = false;
        if (eventObject == null) {
            throw new SiddhiAppRuntimeException("Null object received from the Source to CSVsourceMapper");
        }
        if (!(eventObject instanceof String)) {
            throw new SiddhiAppRuntimeException("Invalid CSV object received. Expected String, but found " +
                    eventObject.getClass().getCanonicalName());
        }
        //String builder = " ";
        String eventObj1 = (String) eventObject;
        String[] spliteventobj0 = eventObj1.split("\n");
        for (int i = 0; i < spliteventobj0.length; i++) {
            Event event = buildEvent(spliteventobj0[i]);
            Object[] data = event.getData();
            for (int j = 0; j < data.length; j++) {
                if (data[j] == null && failOnUnknownAttribute) {
                    log.warn("No attribute with name: " + streamDefinition.getAttributeNameArray()[j] +
                            " found in input event: " + data[j] + ". Hence dropping the" +
                            " event.");
                    isMalformedEvent = true;
                    break;
                }
            }
            if (!isMalformedEvent) {
                eventList.add(event);
            }
        }
        return eventList.toArray(new Event[0]);
    }

    /**
     * Method to build an event from the String
     *
     * @param eventData contain object's data in String which is used to build an event
     * @return Event
     */
    private Event buildEvent(String eventData) {
        Event event = new Event(this.streamDefinition.getAttributeList().size());
        Object[] data = event.getData();
        String[] spliteventobj0 = eventData.split(delimiter);
        Attribute.Type type;
        int position = 0;
        if (isCustomMappingEnabled) {
            for (int j = 0; j < attributeList.size(); j++) {
                Attribute attribute = attributeList.get(j);

                for (int i = 0; i < attributeMappingList.size(); i++) {
                    if (attributeList.get(j).getName().equals(attributeMappingList.get(i).getName())) {
                        try {
                            type = attribute.getType();
                            position = Integer.parseInt(attributeMappingList.get(i).getMapping());
                            if (type.equals(Attribute.Type.STRING)) {
                                try {
                                    data[j] = spliteventobj0[position];
                                } catch (SiddhiAppRuntimeException | NumberFormatException e) {
                                    if (failOnUnknownAttribute) {
                                        log.warn("Error occurred when extracting attribute value. Cause: " +
                                                e.getMessage() + ". Hence dropping the event: " + spliteventobj0[j]);
                                    }
                                }
                            } else {
                                try {
                                    data[j] = attributeConverter.getPropertyValue(spliteventobj0[position], type);
                                } catch (SiddhiAppRuntimeException | NumberFormatException e) {
                                    if (failOnUnknownAttribute) {
                                        log.warn("Error occurred when extracting attribute value. Cause: " +
                                                e.getMessage() + ". Hence dropping the event: " + spliteventobj0[i]);
                                    }
                                }
                            }
                        } catch (SiddhiAppRuntimeException | NumberFormatException e) {
                            log.warn("Error occurred when extracting attribute value. Cause: " + e.getMessage() +
                                    ". Hence dropping the event: " + spliteventobj0[i]);
                        }
                        //log.info(event);
                    }
                }
            }

        } else {
            for (int j = 0; j < attributeList.size(); j++) {
                Attribute attribute = attributeList.get(j);
                String attributeName = attribute.getName();
                if ((type = attributeTypeMap.get(attributeName)) != null) {
                    try {
                        if (attribute.getType().equals(Attribute.Type.STRING)) {
                            try {
                                data[attributePositionMap.get(attributeName)] = spliteventobj0[j];
                            } catch (SiddhiAppRuntimeException | NumberFormatException e) {
                                if (failOnUnknownAttribute) {
                                    log.warn("Error occurred when extracting attribute value. Cause: " +
                                            e.getMessage() + ". Hence dropping the event: " + spliteventobj0[j]);
                                }
                            }
                        } else {
                            try {
                                data[attributePositionMap.get(attributeName)] = attributeConverter.
                                        getPropertyValue(spliteventobj0[j], type);
                            } catch (SiddhiAppRuntimeException | NumberFormatException e) {
                                if (failOnUnknownAttribute) {
                                    log.warn("Error occurred when extracting attribute value. Cause: " +
                                            e.getMessage() + ". Hence dropping the event: " + spliteventobj0[j]);

                                }
                            }
                        }
                    } catch (SiddhiAppRuntimeException | NumberFormatException e) {
                        log.warn("Error occurred when extracting attribute value. Cause: " + e.getMessage() +
                                ". Hence dropping the event: " + spliteventobj0[j]);
                    }
                } else {
                    log.warn("Attribute : " + attributeName + "was not found in given stream definition. " +
                            "Hence ignoring this attribute");
                }
            }
        }
        return event;
    }
    /*******END:Private method for Source Mapper*******/

}
