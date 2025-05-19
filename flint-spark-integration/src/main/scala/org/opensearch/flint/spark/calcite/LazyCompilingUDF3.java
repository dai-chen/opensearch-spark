/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.flint.spark.calcite;

import org.apache.spark.sql.api.java.UDF3;
import org.codehaus.janino.SimpleCompiler;
import java.util.concurrent.atomic.AtomicReference;

@Deprecated
public class LazyCompilingUDF3 implements UDF3<Object, Object, Object, Object> {
    private static final long serialVersionUID = 1L;

    // The source code to be compiled (serializable String)
    private final String sourceCode;
    private final String className;

    // Don't initialize here since this will be null after deserialization
    private transient AtomicReference<UDF3<Object, Object, Object, Object>> compiledImpl;

    public LazyCompilingUDF3(String className, String sourceCode) {
        this.className = className;
        this.sourceCode = sourceCode;
        // Initialize here for the driver
        ensureInitialized();
    }

    // Make sure the AtomicReference is initialized
    private void ensureInitialized() {
        if (compiledImpl == null) {
            compiledImpl = new AtomicReference<>(null);
        }
    }

    @Override
    public Object call(Object a0, Object a1, Object a2) throws Exception {
        // Re-initialize if needed (will happen after deserialization)
        ensureInitialized();

        // Get or create the implementation
        UDF3<Object, Object, Object, Object> impl = compiledImpl.get();
        if (impl == null) {
            // First call on this executor - compile the source
            System.err.println("Compiling UDF code: " + sourceCode);
            impl = compileSource();
            compiledImpl.set(impl);
        }

        // Delegate to the compiled implementation
        return impl.call(a0, a1, a2);
    }

    @SuppressWarnings("unchecked")
    private UDF3<Object, Object, Object, Object> compileSource() {
        try {
            // Compile the class
            SimpleCompiler compiler = new SimpleCompiler();
            compiler.cook(sourceCode);

            // Instantiate the compiled class
            Class<?> udfClass = compiler.getClassLoader().loadClass(className);
            return (UDF3<Object, Object, Object, Object>) udfClass.getDeclaredConstructor().newInstance();
        } catch (Exception e) {
            throw new RuntimeException("Failed to compile UDF: " + e.getMessage() +
                    "\nSource code:\n" + sourceCode, e);
        }
    }
}