/*
 * The MIT License (MIT)
 *
 * Copyright (c) 2016 mergehead.com
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in all
 * copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
 * SOFTWARE.
 */

package com.mergehead.kotlinhive.udtfs

import com.fasterxml.jackson.module.kotlin.jacksonObjectMapper
import com.fasterxml.jackson.module.kotlin.readValue
import com.klarna.hiverunner.HiveShell
import com.klarna.hiverunner.StandaloneHiveRunner
import org.apache.commons.logging.LogFactory
import org.apache.commons.logging.impl.Log4JLogger
import org.apache.hadoop.hive.ql.optimizer.ConstantPropagateProcFactory
import org.apache.log4j.Level
import org.junit.Assert.assertEquals
import org.junit.Assert.assertNotNull
import org.junit.Before
import org.junit.runner.RunWith
import kotlin.reflect.KClass

@RunWith(StandaloneHiveRunner::class)
abstract class TestBase(val methodName:String, val classToTest:KClass<*>) {

    companion object {

        val JSON_MAPPER = jacksonObjectMapper()

    }

    var setupComplete = false

    /**
     * The HiveShell *must* get on the child class for Klarna to work properly via reflection.
     * By convention here, we will expect that all our own child test classes use the field name "hiveShell" for
     * convenience.
     */
    val childHiveShell by lazy {
        ReflectUtils.getFieldValue(this, "hiveShell") as HiveShell
    }

    fun execute(str:String) {
        childHiveShell.execute(str)
    }

    fun query(queryStr:String):List<String> {
        return childHiveShell.executeQuery(queryStr)
    }

    fun queryOne(queryStr:String):String? {
        val results = query(queryStr)
        assertNotNull("Hive should not provide a null response!", results)
        assertEquals("Expected exactly 1 result!", 1, results.size)
        return results.first()
    }

    /**
     * By using Kotlin's reified types, this allows Jackson to just figure out what you expect at runtime and apply
     * the correct mappings between the serialized JSON and your expected type.  This won't always work, but it's
     * pretty convenient for quick solutions (especially in tests).
     */
    inline fun <reified T : Any> queryForJSON(queryStr:String):T? {
        val results = query(queryStr)
        if(results.size > 1) {
            throw RuntimeException("Expected zero or one result, got ${results.size}}")
        }
        if(results.size == 0 || "null".equals(results.first(), ignoreCase = true)) {
            return null
        }
        return JSON_MAPPER.readValue(results.first())
    }

    @Before
    fun prepare() {
        if(!setupComplete) {
            // Quick hack to remove all the annoying, innocuous ERROR lines from test output
            (LogFactory.getLog(ConstantPropagateProcFactory::class.java.name) as Log4JLogger).logger.level = Level.FATAL

            execute("CREATE TEMPORARY FUNCTION $methodName AS '${classToTest.qualifiedName}'")
            setupHQL()
            setupComplete = true
        }
    }

    open fun setupHQL() {}

}
