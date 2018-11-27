package utils;

import com.google.common.base.Function;
import com.google.common.base.Joiner;
import com.google.common.collect.Collections2;
import com.google.common.collect.Sets;
import groovy.lang.Closure;
import groovy.lang.GString;
import groovy.lang.GroovyShell;
import groovy.lang.Script;
import lombok.RequiredArgsConstructor;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

/**
 * @Author WangSen
 * @Data 2018/6/7 10:57
 */

@RequiredArgsConstructor
public class InlineExpressionParser {
    private static final char SPLITTER = ',';

    //存放groovyScript的缓冲池，否则会导致内存泄漏（大量的class文件堆积在metaspace）。
    private static final Map<String, Script> SCRIPT_MAP = new ConcurrentHashMap<>();

    private static final GroovyShell GROOVY_SHELL = new GroovyShell();

    private final String inlineExpression;

    /**
     * Replace all the inlineExpression placeholders.
     *
     * @param inlineExpression inlineExpression
     * @return result inlineExpression
     */
    public static String handlePlaceHolder(final String inlineExpression) {
        return inlineExpression.contains("$->{") ? inlineExpression.replaceAll("\\$->\\{", "\\$\\{") : inlineExpression;
    }

    public Closure<?> evaluateClosure() {
        return (Closure) evaluate(Joiner.on("").join("{it->\"",inlineExpression,"\"}"));
    }

    /**
     * Split and evaluate inline expression.
     *
     * @return result list
     */
    public List<String> evaluate() {
        if (null == inlineExpression) {
            return Collections.emptyList();
        }
        return flatten(evaluate(split()));
    }

    private List<Object> evaluate(final List<String> inlineExpressions) {
        List<Object> result = new ArrayList<>(inlineExpressions.size());
        for (String each : inlineExpressions) {
            StringBuilder expression = new StringBuilder(handlePlaceHolder(each));
            if (!each.startsWith("\"")) {
                expression.insert(0, "\"");
            }
            if (!each.endsWith("\"")) {
                expression.append("\"");
            }
            result.add(evaluate(expression.toString()));
        }
        return result;
    }

    private Object evaluate(final String inlineExpression) {
        if (!SCRIPT_MAP.containsKey(inlineExpression)) {
            SCRIPT_MAP.put(inlineExpression, GROOVY_SHELL.parse(inlineExpression));
        }
        return SCRIPT_MAP.get(inlineExpression).run();
    }

    /**
     * 分离出单个的guava表达式(expression1,expression2,...)
     */
    private List<String> split() {
        List<String> result = new ArrayList<>();
        StringBuilder segment = new StringBuilder();
        int bracketsDepth = 0;
        for (int i = 0; i < inlineExpression.length(); i++) {
            char each = inlineExpression.charAt(i);
            switch (each) {
                case SPLITTER:
                    if (bracketsDepth > 0) {
                        segment.append(each);
                    } else {
                        result.add(segment.toString().trim());
                        segment.setLength(0);
                    }
                    break;
                case '$':
                    if ('{' == inlineExpression.charAt(i + 1)) {
                        bracketsDepth++;
                    }
                    if ("->{".equals(inlineExpression.substring(i + 1, i + 4))) {
                        bracketsDepth++;
                    }
                    segment.append(each);
                    break;
                case '}':
                    if (bracketsDepth > 0) {
                        bracketsDepth--;
                    }
                    segment.append(each);
                    break;
                default:
                    segment.append(each);
                    break;
            }
        }
        if (segment.length() > 0) {
            result.add(segment.toString().trim());
        }
        return result;
    }

    private List<String> flatten(final List<Object> segments) {
        List<String> result = new ArrayList<>();
        for (Object each : segments) {
            if (each instanceof GString) {
                result.addAll(assemblyCartesianSegments((GString) each));
            } else {
                result.add(each.toString());
            }
        }
        return result;
    }

    private List<String> assemblyCartesianSegments(final GString segment) {
        Set<List<String>> cartesianValues = getCartesianValues(segment);
        List<String> result = new ArrayList<>(cartesianValues.size());
        for (List<String> each : cartesianValues) {
            result.add(assemblySegment(each, segment));
        }
        return result;
    }

    @SuppressWarnings("unchecked")
    private Set<List<String>> getCartesianValues(final GString segment) {
        List<Set<String>> result = new ArrayList<>(segment.getValues().length);
        for (Object each : segment.getValues()) {
            if (null == each) {
                continue;
            }
            if (each instanceof Collection) {
                result.add(Sets.newLinkedHashSet(Collections2.transform((Collection<Object>) each, new Function<Object, String>() {

                    @Override
                    public String apply(final Object input) {
                        if (input == null) {
                            return "";
                        }
                        return input.toString();
                    }
                })));
            } else {
                result.add(Sets.newHashSet(each.toString()));
            }
        }
        return Sets.cartesianProduct(result);
    }

    private String assemblySegment(final List<String> cartesianValue, final GString segment) {
        StringBuilder result = new StringBuilder();
        for (int i = 0; i < segment.getStrings().length; i++) {
            result.append(segment.getStrings()[i]);
            if (i < cartesianValue.size()) {
                result.append(cartesianValue.get(i));
            }
        }
        return result.toString();
    }
}
