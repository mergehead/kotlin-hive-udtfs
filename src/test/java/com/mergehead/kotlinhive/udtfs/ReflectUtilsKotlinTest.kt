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

import org.junit.Assert
import org.junit.Test

@Suppress("unused")
class ReflectUtilsKotlinTest {

    var testMe = "Hello, public kotlin"
    private val testMePrivate = "Hello, private kotlin"

    companion object {
        val javaObj = ReflectUtilsJavaTest()
        var testMeStatic = "Hello, static public kotlin"
        private val testMeStaticPrivate = "Hello, static private kotlin"

    }

    /////// VERIFY KOTLIN CALLING JAVA WORKS

    @Test
    fun publicFieldJava() {
        val result = ReflectUtils.getField(javaObj, "testMe")
        Assert.assertNotNull(result)
    }

    @Test
    fun publicFieldValueJava() {
        val result = ReflectUtils.getFieldValue(javaObj, "testMe") as String
        Assert.assertEquals("Hello, public java", result)
    }

    @Test
    fun privateFieldJava() {
        val result = ReflectUtils.getField(javaObj, "testMePrivate")
        Assert.assertNotNull(result)
    }

    @Test
    fun privateFieldValueJava() {
        val result = ReflectUtils.getFieldValue(javaObj, "testMePrivate") as String
        Assert.assertEquals("Hello, private java", result)
    }

    @Test
    fun publicStaticFieldJava() {
        val result = ReflectUtils.getField(ReflectUtilsJavaTest::class.java, "testMeStatic")
        Assert.assertNotNull(result)
    }

    @Test
    fun publicStaticFieldValueJava() {
        val result = ReflectUtils.getFieldValue(ReflectUtilsJavaTest::class.java, "testMeStatic") as String
        Assert.assertEquals("Hello, static public java", result)
    }

    @Test
    fun privateStaticFieldJava() {
        val result = ReflectUtils.getField(ReflectUtilsJavaTest::class.java, "testMeStaticPrivate")
        Assert.assertNotNull(result)
    }

    @Test
    fun privateStaticFieldValueJava() {
        val result = ReflectUtils.getFieldValue(ReflectUtilsJavaTest::class.java, "testMeStaticPrivate") as String
        Assert.assertEquals("Hello, static private java", result)
    }

    /////// VERIFY KOTLIN INSPECTING KOTLIN WORKS

    @Test
    fun publicFieldKotlin() {
        val result = ReflectUtils.getField(this, "testMe")
        Assert.assertNotNull(result)
    }

    @Test
    fun publicFieldValueKotlin() {
        val result = ReflectUtils.getFieldValue(this, "testMe") as String
        Assert.assertEquals("Hello, public kotlin", result)
    }

    @Test
    fun privateFieldKotlin() {
        val result = ReflectUtils.getField(this, "testMePrivate")
        Assert.assertNotNull(result)
    }

    @Test
    fun privateFieldValueKotlin() {
        val result = ReflectUtils.getFieldValue(this, "testMePrivate") as String
        Assert.assertEquals("Hello, private kotlin", result)
    }

    @Test
    fun publicStaticFieldKotlin() {
        val result = ReflectUtils.getField(ReflectUtilsKotlinTest::class.java, "testMeStatic")
        Assert.assertNotNull(result)
    }

    @Test
    fun publicStaticFieldValueKotlin() {
        val result = ReflectUtils.getFieldValue(ReflectUtilsKotlinTest::class.java, "testMeStatic") as String
        Assert.assertEquals("Hello, static public kotlin", result)
    }

    @Test
    fun privateStaticFieldKotlin() {
        val result = ReflectUtils.getField(ReflectUtilsKotlinTest::class.java, "testMeStaticPrivate")
        Assert.assertNotNull(result)
    }

    @Test
    fun privateStaticFieldValueKotlin() {
        val result = ReflectUtils.getFieldValue(ReflectUtilsKotlinTest::class.java, "testMeStaticPrivate") as String
        Assert.assertEquals("Hello, static private kotlin", result)
    }

}
