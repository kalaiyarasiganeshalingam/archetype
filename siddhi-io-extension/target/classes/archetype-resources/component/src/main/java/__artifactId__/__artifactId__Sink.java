package ${package}.${artifactId};

import org.apache.log4j.Logger;
import org.wso2.siddhi.annotation.Example;
import org.wso2.siddhi.annotation.Extension;
import org.wso2.siddhi.annotation.Parameter;
import org.wso2.siddhi.annotation.util.DataType;
import org.wso2.siddhi.core.config.SiddhiAppContext;
import org.wso2.siddhi.core.exception.ConnectionUnavailableException;
import org.wso2.siddhi.core.stream.output.sink.Sink;
import org.wso2.siddhi.core.util.config.ConfigReader;
import org.wso2.siddhi.core.util.transport.DynamicOptions;
import org.wso2.siddhi.core.util.transport.Option;
import org.wso2.siddhi.core.util.transport.OptionHolder;
import org.wso2.siddhi.query.api.definition.StreamDefinition;

import java.io.BufferedWriter;
import java.io.File;
import java.io.IOException;
import java.nio.file.FileSystemNotFoundException;
import java.nio.file.Files;
import java.nio.file.Paths;
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
        name = "file",
        namespace = "sink",
        description = " File Sink can be used to write events into the file in an appropriate format.",
        parameters = {
                @Parameter(name = "file.path",
                        description = "The path of the file to which events should be appended.",
                        dynamic = false,
                        type = {DataType.STRING}),
                @Parameter(name = "file.name",
                        description = "The file to which the events should be appended.",
                        dynamic = true,
                        optional = true, defaultValue = "output.xml",
                        type = {DataType.STRING}),
        },
        examples = {
                @Example(
                        syntax = "@sink(type='write',fileName='example.xml',@map(type='xml'))\n" +
                                "define stream FooStream (symbol string, price float, volume long)",

                        description = " Under above configuration, for each event,\n " +
                                " data will be appended to that file in the following format" +
                                " <events><event><symbol>WSO2</symbol><price>55.6</price><volume>100</volume>\n" +
                                " </event></events>"
                ),
        }
)
public class ${artifactId}Sink extends Sink {
    private static final Logger log = Logger.getLogger(${siddhi_function_name}Sink.class);

    /*** BEGIN: Sample class variables for File Sink ***/
    private static final String FILENAME = "file.name";
    private static final String FOLDERPATH = "folder.path";
    private Option fileName;
    private String filePath;
    /***END: Sample class variables for File Sink***/

    //if you can get more information click this link : https://wso2.github.io/siddhi/documentation/siddhi-4.0/#sink

    /**
     * Returns a list of supported dynamic options (that means for each event value of the option can change) by
     * the transport
     *
     * @return the list of supported dynamic option keys
     */
    @Override
    public String[] getSupportedDynamicOptions() {
        //In this example, File Sink has only one dynamic option: FILENAME
        //Change this line with appropriate dynamic options for the sink
        return new String[]{FILENAME};
    }

    /**
     * The initialization method for {@link Sink}, will be called before other methods. It used to validate
     * all configurations and to get initial values.
     * @param streamDefinition  containing stream definition bind to the {@link Sink}
     * @param optionHolder      Option holder containing static and dynamic configuration related
     *                          to the {@link Sink}
     * @param configReader      to read the sink related system configuration.
     * @param siddhiAppContext  the context of the {@link org.wso2.siddhi.query.api.SiddhiApp} used to
     *                          get siddhi related utility functions.
     */
    @Override
    protected void init(StreamDefinition streamDefinition, OptionHolder optionHolder,
                        ConfigReader configReader, SiddhiAppContext siddhiAppContext) {
        // In this example, File sink is initialized with only two parameters,
        // Change this implementation with appropriate static & dynamic parameters,
        // which contains what are the parameters need from{@link Sink}.

        //For the dynamic parameters: getOrCreateOption method get option if it is exist,if not it will create
        //the options. If it is mandatory to have the corresponding option then use validateAndGet() method.
        this.fileName = optionHolder.getOrCreateOption(FILENAME , "example.xml");

        //Fore the Static parameter: validateAndGetStaticValue method gets the value of
        // the corresponding option if the option is exist, if not throw an exception.
        this.filePath = optionHolder.validateAndGetStaticValue(FOLDERPATH , "Files");
    }

    /**
     * Returns the list of classes which this sink can consume.
     * Based on the type of the sink, it may be limited to being able to publish specific type of classes.
     * For example, a sink of type file can only write objects of type String .
     * @return array of supported classes , if extension can support of any types of classes
     * then return empty array .
     */
    @Override
    public Class[] getSupportedInputEventClasses() {
        // Change the following line with appropriate supported classes.
        return new Class[]{ String.class};
    }

    /**
     * This method will be called before the processing method.
     * Intention to establish connection to publish event.
     * @throws ConnectionUnavailableException if end point is unavailable the ConnectionUnavailableException thrown
     *                                        such that the  system will take care retrying for connection
     */
    @Override
    public void connect() throws ConnectionUnavailableException {
        // Connection will be made here ,
        // In this example,check whether folder is exist or not
        // If folder is not available then throw the exception
        // Change this implementation according to the task,
        File folder = new File(filePath);
        if (!folder.exists()) {
            File dir = new File(filePath);
            // attempt to create the directory here
            boolean successful = dir.mkdir();
            if (!successful) {
                throw new FileSystemNotFoundException("failed trying to create the directory");
            }
        } else if (!folder.isDirectory()) {
            throw new FileSystemNotFoundException(filePath + " is not a directory.");
        }
    }

    /**
     * This method will be called when events need to be published via this sink
     * @param payload        payload of the event based on the supported event class exported by the extensions
     * @param dynamicOptions holds the dynamic options of this sink and Use this object to obtain dynamic options.
     * @throws ConnectionUnavailableException if end point is unavailable the ConnectionUnavailableException thrown
     *                                        such that the  system will take care retrying for connection
     */
    @Override
    public void publish(Object payload, DynamicOptions dynamicOptions) throws ConnectionUnavailableException {
        //In this example, the logic is written to write the payload into the specified file.
        //Change the logic according to the Sink
        //for this extension the payload will be always String as we only define String as a supported class
        String createFileName;
        createFileName = fileName.getValue(dynamicOptions);
        String messages = (String) payload;
        String path = filePath + "/" + createFileName;
        try {
            try (BufferedWriter writer = Files.newBufferedWriter(Paths.get(path))) {
                writer.write(messages);
                writer.newLine();
                writer.flush();
            }
        } catch (IOException e) {
            log.info("Error is encountered while writing the content: " + messages +
                    " to the file:" + fileName + "." + e.getMessage());
        }
    }

    /**
     * Called after all publishing is done, or when {@link ConnectionUnavailableException} is thrown
     * Implementation of this method should contain the steps needed to disconnect from the sink.
     */
    @Override
    public void disconnect() {
    }

    /**
     * The method can be called when removing an event receiver.
     * The cleanups that have to be done after removing the receiver could be done here.
     */
    @Override
    public void destroy() {
    }

    /**
     * Used to collect the serializable state of the processing element, that need to be
     * persisted for reconstructing the element to the same state on a different point of time
     * This is also used to identify the internal states and debugging
     * @return all internal states should be return as an map with meaning full keys
     */
    @Override
    public Map<String, Object> currentState() {
        return null;
    }

    /**
     * Used to restore serialized state of the processing element, for reconstructing
     * the element to the same state as if was on a previous point of time.
     *
     * @param state the stateful objects of the processing element as a map.
     *              This map will have the  same keys that is created upon calling currentState() method.
     */
    @Override
    public void restoreState(Map<String, Object> state) {

    }
}
