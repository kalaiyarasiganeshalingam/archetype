package ${package}.streamprocessor;

import org.wso2.siddhi.annotation.Example;
import org.wso2.siddhi.annotation.Extension;
import org.wso2.siddhi.annotation.Parameter;
import org.wso2.siddhi.annotation.util.DataType;
import org.wso2.siddhi.core.config.SiddhiAppContext;
import org.wso2.siddhi.core.event.ComplexEventChunk;
import org.wso2.siddhi.core.event.stream.StreamEvent;
import org.wso2.siddhi.core.event.stream.StreamEventCloner;
import org.wso2.siddhi.core.event.stream.populater.ComplexEventPopulater;
import org.wso2.siddhi.core.executor.ExpressionExecutor;
import org.wso2.siddhi.core.query.processor.Processor;
import org.wso2.siddhi.core.query.processor.stream.StreamProcessor;
import org.wso2.siddhi.core.util.config.ConfigReader;
import org.wso2.siddhi.query.api.definition.AbstractDefinition;
import org.wso2.siddhi.query.api.definition.Attribute;

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
        name = " ",
        namespace = "custom",
        description = " ",
        parameters = {
                @Parameter(name = " ",
                        description = " " ,
                        /* dynamic = false/true,
                        optional = true/false, defaultValue = " ",
                        type = {DataType.INT, DataType.BOOL, DataType.STRING, DataType.DOUBLE, }),*/
                        type = {DataType.INT, DataType.BOOL, DataType.STRING, DataType.DOUBLE, }),
        },
        examples = {
                @Example(
                        syntax = " ",
                        description = " "
                )
        }
)
public class Streamprocessor  extends StreamProcessor {
    @Override
    protected void process(ComplexEventChunk<StreamEvent> streamEventChunk, Processor nextProcessor,
                           StreamEventCloner streamEventCloner, ComplexEventPopulater complexEventPopulater) {

    }

    /**
     * The initialization method for {@link FileSink}, which will be called before other methods and validate
     * the all configuration and getting the intial values.
     * @param attributeExpressionExecutors are the executors of each attributes in the Function
     * @param configReader        this hold the {@link Streamprocessor} extensions configuration reader.
     * @param siddhiAppContext    Siddhi app runtime context
     */
    @Override
    protected List<Attribute> init(AbstractDefinition inputDefinition,
                                   ExpressionExecutor[] attributeExpressionExecutors, ConfigReader configReader,
                                   SiddhiAppContext siddhiAppContext) {
        return null;
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
        return null;
    }

    /**
     * Used to restore serialized state of the processing element, for reconstructing
     * the element to the same state as if was on a previous point of time.
     *
     * @param state the stateful objects of the processing element as a map.
     *              This is the same map that is created upon calling currentState() method.
     */
    @Override
    public void restoreState(Map<String, Object> state) {

    }
}
