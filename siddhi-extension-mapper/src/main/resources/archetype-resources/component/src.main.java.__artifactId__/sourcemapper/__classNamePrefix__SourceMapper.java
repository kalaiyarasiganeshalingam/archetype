package ${package}.${artifactId}.sourcemapper;

import org.wso2.siddhi.annotation.Example;
import org.wso2.siddhi.annotation.Extension;
import org.wso2.siddhi.annotation.Parameter;
import org.wso2.siddhi.annotation.util.DataType;
import org.wso2.siddhi.core.config.SiddhiAppContext;
import org.wso2.siddhi.core.stream.input.source.AttributeMapping;
import org.wso2.siddhi.core.stream.input.source.InputEventHandler;
import org.wso2.siddhi.core.stream.input.source.SourceMapper;
import org.wso2.siddhi.core.util.config.ConfigReader;
import org.wso2.siddhi.core.util.transport.OptionHolder;
import org.wso2.siddhi.query.api.definition.StreamDefinition;

import java.util.List;

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
        name = " ",
        namespace = "source",
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

public class CSVSource extends SourceMapper {

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
    public void init(StreamDefinition streamDefinition, OptionHolder optionHolder,
                     List<AttributeMapping> attributeMappingList, ConfigReader configReader,
                     SiddhiAppContext siddhiAppContext) {

    }

    /**
     * Returns the list of classes which this source can output.
     *
     * @return Array of classes that will be output by the source.
     * Null or empty array if it can produce any type of class.
     */
    @Override
    public Class[] getSupportedInputEventClasses() {
        return new Class[0];
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

    }
}
