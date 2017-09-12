package ${package}.aggregate;

import org.apache.log4j.Logger;
import org.wso2.siddhi.annotation.Example;
import org.wso2.siddhi.annotation.Extension;
import org.wso2.siddhi.annotation.Parameter;
import org.wso2.siddhi.annotation.ReturnAttribute;
import org.wso2.siddhi.annotation.util.DataType;
import org.wso2.siddhi.core.config.SiddhiAppContext;
import org.wso2.siddhi.core.executor.ExpressionExecutor;
import org.wso2.siddhi.core.query.selector.attribute.aggregator.AttributeAggregator;
import org.wso2.siddhi.core.util.config.ConfigReader;
import org.wso2.siddhi.query.api.definition.Attribute;

import java.util.HashMap;
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
        name = "aggregator",
        namespace = "custom",
        description = "Returns the count of all the events for time batch in 20 seconds.",
        parameters = {
                @Parameter(name = "window.time",
                        description = "The time of the event count period",
                        dynamic = false,
                        type = {DataType.TIME})},
        returnAttributes = @ReturnAttribute(
                description = "Returns the event count as a long.",
                type = {DataType.LONG}),
        examples = @Example(
                syntax = "@info(name = 'query')\n " +
                        "from pizzaOrder#window.time(20 sec) \n" +
                        "select custom:aggregator(orderNo) as totalOrders\n" +
                        " insert into OutMediationStream;",
                description = "This will return the count of all the events for time batch in 20 seconds."
        )
)

public class ${siddhi_function_name}AggregateFunction extends AttributeAggregator {
    private static Attribute.Type type = Attribute.Type.LONG;
    private static final Logger log = Logger.getLogger(${siddhi_function_name}AggregateFunction.class);
    private long value = 0L;

    /**
     * The initialization method for {@link AggregateFunction}, which will be called before other methods and validate
     * the all configuration and getting the intial values.
     * @param expressionExecutors are the executors of each attributes in the function
     * @param configReader        this hold the {@link AttributeAggregator} extensions configuration reader.
     * @param siddhiAppContext    Siddhi app runtime context
     */
    @Override
    protected void init(ExpressionExecutor[] expressionExecutors,
                        ConfigReader configReader, SiddhiAppContext siddhiAppContext) {
    }

    /**
     * Get attribute's type in the expressionExecutors .
     *
     * @return attribute's type
     */
    @Override
    public Attribute.Type getReturnType() {
        return type;

    }

    /**
     * The main execution method which will be called upon event arrival
     * when there are zero or one function parameter
     *
     * @param data null if the function parameter count is zero or
     *             runtime data value of the function parameter
     * @return the function result
     */
    @Override
    public Object processAdd(Object data) {
        value++;
        return value;
    }


    /**
     * The main execution method which will be called upon event arrival
     * when there are more than one function parameter
     *
     * @param data the runtime values of  parameters
     * @return the value
     */
    @Override
    public Object processAdd(Object[] data) {
        value++;
        return value;
    }

    /**
     * The main execution method which will be called upon event expired or out
     *when there are zero or one function parameter
     *
     * @param data null if the function parameter count is zero or
     *             runtime data value of the function parameter
     * @return the value
     */
    @Override
    public Object processRemove(Object data) {
        value--;
        return value;
    }

    /**
     * The main execution method which will be called upon event expired or out
     * when there are more than one function parameter
     *
     * @param data null if the function parameter count is zero or
     *             runtime data value of the function parameter
     * @return the value
     */
    @Override
    public Object processRemove(Object[] data) {
        value--;
        return value;
    }

    /**
     * The execution method which will be called to reset the event
     *
     * @return the value
     */
    @Override
    public Object reset() {
        value = 0L;
        return value;
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
        state.put("Value", value);
        return state;

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

        value = (long) state.get("Value");
    }
}
