package ${package}.${artifactId}.source;

import org.apache.log4j.Logger;
import org.wso2.siddhi.annotation.Example;
import org.wso2.siddhi.annotation.Extension;
import org.wso2.siddhi.annotation.Parameter;
import org.wso2.siddhi.annotation.util.DataType;
import org.wso2.siddhi.core.config.SiddhiAppContext;
import org.wso2.siddhi.core.exception.ConnectionUnavailableException;
import org.wso2.siddhi.core.stream.input.source.Source;
import org.wso2.siddhi.core.stream.input.source.SourceEventListener;
import org.wso2.siddhi.core.util.config.ConfigReader;
import org.wso2.siddhi.core.util.transport.OptionHolder;

import java.io.BufferedReader;
import java.io.File;
import java.io.FilenameFilter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;

import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;


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
 * //Sink configurations
 * parameters = {
 * {@literal @}Parameter(name = "The name of the first parameter",
 *                               description= "The description of the first parameter",
 *                               type =  "Supported parameter types.
 *                                        eg:{DataType.STRING, DataType.INT, DataType.LONG etc}",
 *                               dynamic= "false
 *                                         (if parameter doesn't depend on each event then dynamic parameter is false.
 *                                         In Source, only use static parameter)",
 *                               optional= "true/false, defaultValue= if it is optional then assign a default value
 *                                          according to the type."),
 * {@literal @}Parameter(name = "The name of the second parameter",
 *                               description= "The description of the second parameter",
 *                               type =   "Supported parameter types.
 *                                         eg:{DataType.STRING, DataType.INT, DataType.LONG etc}",
 *                               dynamic= "false
 *                                         (if parameter doesn't depend on each event then dynamic parameter is false.
 *                                         In Source, only use static parameter)",
 *                               optional= "true/false, defaultValue= if it is optional then assign a default value
 *                                         according to the type."),
 * },
 * //If Source system configurations will need then
 * systemParameters = {
 * {@literal @}SystemParameter(name = "The name of the first  system parameter",
 *                                      description="The description of the first system parameter." ,
 *                                      defaultValue = "the default value of the system parameter.",
 *                                      possibleParameter="the possible value of the system parameter.",
 *                               ),
 * },
 * examples = {
 * {@literal @}Example(syntax = "sample query with Source annotation that explain how extension use in Siddhi."
 *                              description =" The description of the given example's query."
 *                      ),
 * }
 * )
 * </code></pre>
 */

@Extension(
        name = "file",
        namespace = "source",
        description = " Under the specific folder ,File Source  checks the file name's extension " +
                "with a given extension, If it is satisfied and if that file's content has a given mapper format then" +
                " system should convert this content to an event ",

        parameters = {
                @Parameter(
                        name = "folder.path",
                        description =
                                "The path of the file to which files' contents should be converted to event " ,
                        dynamic = false,
                        optional = true, defaultValue = "Files",
                        type = {DataType.STRING}),
                @Parameter(
                        name = "file.extension",
                        description =
                                "The extension of the file to which type of files' contents should be " +
                                        "converted to event",
                        dynamic = false,
                        type = {DataType.STRING})
        },
        examples = {
                @Example(
                        syntax = "@source(type='file',folder.Path='Files',file.extension='xml',@map(type='xml'))\n " +
                                "define stream FooStream (symbol string, price float, volume int);",
                        description = "Under above configuration," +
                                "input should be in the following format." +
                                "[WSO2, 55.6, 100]"
                ),
        }
)

public class ${artifactId}Source extends Source {

    /****BEGIN: Sample class variables for File Source***/
    public static final String FOLDER_PATH = "folder.path";
    public static final String FILE_EXTENSION = "file.extension";
    private static final Logger log = Logger.getLogger(${siddhi_function_name}Source.class);
    private String folderPath;
    private String fileExtension;
    private SourceEventListener sourceEventListener;
    private String pointer = "0";
    private String currentFileName;
    private StringBuilder storeLineData;
    private boolean isPaused;
    private ReentrantLock lock;
    private Condition condition;
    /****END: Sample class variables for File Source***/

    //if you can get more information click this link : https://wso2.github.io/siddhi/documentation/siddhi-4.0/#source


    /**
     * Returns the list of classes which this source can output.
     *
     * @return Array of classes that will be output by the source.
     * Null or empty array if it can produce any type of class.
     */
    @Override
    public Class[] getOutputEventClasses() {
        // Change the following line with appropriate classes.
        return new Class[]{String.class};

    }

    /**
     * The initialization method for {@link Source}, will be called before other methods. It used to validate
     * all configurations and to get initial values.
     * @param sourceEventListener After receiving events, the source should trigger onEvent() of this listener.
     *                            Listener will then pass on the events to the appropriate mappers for processing .
     * @param optionHolder        Option holder containing static configuration related to the {@link Source}
     * @param configReader        ConfigReader is used to read the {@link Source} related system configuration.
     * @param siddhiAppContext    the context of the {@link org.wso2.siddhi.query.api.SiddhiApp} used to get Siddhi
     *                            related utility functions.
     */
    @Override
    public void init(SourceEventListener sourceEventListener,
                     OptionHolder optionHolder, String[] strings, ConfigReader configReader,
                     SiddhiAppContext siddhiAppContext) {
        // In this case,File Source is initialized with only two parameter. For the source, all parameters are static.
        // Change this line with appropriate static parameters,
        // which need by {@link Source} to finish the task.
        this.sourceEventListener = sourceEventListener;
        this.folderPath = optionHolder.validateAndGetStaticValue(FOLDER_PATH , "Files");
        this.fileExtension = optionHolder.validateAndGetStaticValue(FILE_EXTENSION, "xml");
        lock = new ReentrantLock();
        condition = lock.newCondition();
        storeLineData = new StringBuilder();
    }

    /**
     * Initially Called to connect to the end point for start retrieving the messages asynchronously .
     *
     * @param connectionCallback Callback to pass the ConnectionUnavailableException in case of connection failure after
     *                           initial successful connection. (can be used when events are receiving asynchronously)
     * @throws ConnectionUnavailableException if it cannot connect to the source backend immediately.
     */
    @Override
    public void connect(ConnectionCallback connectionCallback) throws ConnectionUnavailableException {
        //In this example, the logic is written to read the file content from the specified file URI and
        // publish the content.
        //Implement the logic according the source
        try {
            getFileContentAndPublish();
        } catch (IOException e) {
            log.error("Error is occurred while getting files or publishing", e);
        }
    }

    /**
     * This method can be called when it is needed to disconnect from the end point.
     */
    @Override
    public void disconnect() {

    }

    /**
     * Called at the end to clean all the resources consumed by the {@link Source}
     */
    @Override
    public void destroy() {

    }

    /**
     * Called to pause event consumption
     */
    @Override
    public void pause() {
        isPaused = true;

    }

    /**
     * Called to resume event consumption
     */
    @Override
    public void resume() {
        isPaused = false;
        try {
            lock.lock();
            condition.signalAll();
        } finally {
            lock.unlock();
        }
    }

    /**
     * Used to collect the serializable state of the processing element, that need to be
     * persisted for the reconstructing the element to the same state on a different point of time
     *
     * @return stateful objects of the processing element as a map
     */

    @Override
    public Map<String, Object> currentState() {
        Map<String, Object> currentState = new HashMap<>();
        currentState.put("Filepointer1", pointer);
        currentState.put("CurrentFileName", currentFileName);
        currentState.put("CurrentStoredata", storeLineData);
        return currentState;
    }

    /**
     * Used to restore serialized state of the processing element, for reconstructing
     *
     * @param map the stateful objects of the element as a map.
     *              This is the same map that is created upon calling currentState() method.
     */
    @Override
    public void restoreState(Map<String, Object> map) {
        this.pointer = (String) map.get("pointer");
        this.currentFileName = (String) map.get("currentFileName");
        this.storeLineData = storeLineData.append(map.get("CurrentStoredata"));
    }

    /****BEGIN:Sample private methods for File Source*******/
    /**
     * Used to read the contents of files under the related path
     *
     * @param fileName denote the folder location
     */
    private void startPublishing(String fileName, int filePointer, StringBuilder storeLineData) throws IOException {

        String currentpath = folderPath + "/" + fileName;
        int counter = 0;
        try (BufferedReader reader = Files.newBufferedReader(Paths.get(currentpath))) {
            String line = null;
            while ((line = reader.readLine()) != null) {
                if (isPaused) { //spurious wakeup condition is deliberately traded off for performance
                    lock.lock();
                    try {
                        while (!isPaused) {
                            condition.await();
                        }
                    } catch (InterruptedException ie) {
                        Thread.currentThread().interrupt();
                    } finally {
                        lock.unlock();
                    }
                }
                counter++;
                if (counter > filePointer) {
                    storeLineData.append(line);
                    filePointer = filePointer + 1;
                    pointer = String.valueOf(filePointer);
                    currentFileName = fileName;
                }

            }
            sourceEventListener.onEvent(storeLineData.toString(), null);

        } catch (IOException e) {
            log.error("Error is occurred while receiving events" , e);
        }
    }
    /**
     * Get the name of files which satisfied file extension given in the file.extension parameter.
     */
    private void getFileContentAndPublish() throws IOException {
        File filesInFolder = new File(folderPath);
        String[] files = filesInFolder.list(new FilenameFilter() {
            /* accept or reject the file names under the folder
             * @param filesInFolder contains all files in the given folder
             * @param name is the file's name
             */
            @Override
            public boolean accept(File file, String name) {
                if (name.toLowerCase(Locale.ENGLISH).endsWith(fileExtension)) {
                    return true;
                } else {
                    log.warn("File '" + name + "' is not match with the given file extension '" + fileExtension + "'.");
                    return false;
                }
            }
        });

        if (files != null) {
            List listFile = Arrays.asList(files);
            Collections.sort(listFile);
            if (currentFileName == null) {
                for (Object filename : listFile) {
                    storeLineData.setLength(0);
                    startPublishing((String) filename , 0 , storeLineData);
                }
            } else {
                Optional<String> queryResult = listFile.stream()
                        .filter(value -> value != null)
                        .filter(value -> value.equals(currentFileName))
                        .findFirst();
                if (queryResult.isPresent()) {
                    storeLineData.setLength(0);
                    startPublishing(currentFileName , Integer.parseInt(pointer) , storeLineData);
                }
            }
        } else {
            log.warn(folderPath + " , Folder hasn't any files with a given file extension '" + fileExtension + "'.");
        }
    }
    /****ENd:Sample private methods for File Source***/
}






