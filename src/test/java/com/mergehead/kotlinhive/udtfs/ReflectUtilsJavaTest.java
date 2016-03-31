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

package com.mergehead.kotlinhive.udtfs;

import org.junit.Assert;
import org.junit.Test;

import java.lang.reflect.Field;

@SuppressWarnings("unused")
public class ReflectUtilsJavaTest {

    public static ReflectUtilsKotlinTest kotlinObj = new ReflectUtilsKotlinTest();

    public String testMe = "Hello, public java";
    private String testMePrivate = "Hello, private java";

    public static String testMeStatic = "Hello, static public java";
    private static String testMeStaticPrivate = "Hello, static private java";

    /////// VERIFY JAVA CALLING JAVA WORKS

    @Test
    public void publicFieldJava() {
        Field result = ReflectUtils.getField(this, "testMe");
        Assert.assertNotNull(result);
    }

    @Test
    public void publicFieldValueJava() {
        String result = (String) ReflectUtils.getFieldValue(this, "testMe");
        Assert.assertEquals("Hello, public java", result);
    }

    @Test
    public void privateFieldJava() {
        Field result = ReflectUtils.getField(this, "testMePrivate");
        Assert.assertNotNull(result);
    }

    @Test
    public void privateFieldValueJava() {
        String result = (String) ReflectUtils.getFieldValue(this, "testMePrivate");
        Assert.assertEquals("Hello, private java", result);
    }

    @Test
    public void publicStaticFieldJava() {
        Field result = ReflectUtils.getField(ReflectUtilsJavaTest.class, "testMeStatic");
        Assert.assertNotNull(result);
    }

    @Test
    public void publicStaticFieldValueJava() {
        String result = (String) ReflectUtils.getFieldValue(ReflectUtilsJavaTest.class, "testMeStatic");
        Assert.assertEquals("Hello, static public java", result);
    }

    @Test
    public void privateStaticFieldJava() {
        Field result = ReflectUtils.getField(ReflectUtilsJavaTest.class, "testMeStaticPrivate");
        Assert.assertNotNull(result);
    }

    @Test
    public void privateStaticFieldValueJava() {
        String result = (String) ReflectUtils.getFieldValue(ReflectUtilsJavaTest.class, "testMeStaticPrivate");
        Assert.assertEquals("Hello, static private java", result);
    }

    /////// VERIFY JAVA CALLING KOTLIN WORKS

    @Test
    public void publicFieldKotlin() {
        Field result = ReflectUtils.getField(kotlinObj, "testMe");
        Assert.assertNotNull(result);
    }

    @Test
    public void publicFieldValueKotlin() {
        String result = (String) ReflectUtils.getFieldValue(kotlinObj, "testMe");
        Assert.assertEquals("Hello, public kotlin", result);
    }

    @Test
    public void privateFieldKotlin() {
        Field result = ReflectUtils.getField(kotlinObj, "testMePrivate");
        Assert.assertNotNull(result);
    }

    @Test
    public void privateFieldValueKotlin() {
        String result = (String) ReflectUtils.getFieldValue(kotlinObj, "testMePrivate");
        Assert.assertEquals("Hello, private kotlin", result);
    }

    @Test
    public void publicStaticFieldKotlin() {
        Field result = ReflectUtils.getField(ReflectUtilsKotlinTest.class, "testMeStatic");
        Assert.assertNotNull(result);
    }

    @Test
    public void publicStaticFieldValueKotlin() {
        String result = (String) ReflectUtils.getFieldValue(ReflectUtilsKotlinTest.class, "testMeStatic");
        Assert.assertEquals("Hello, static public kotlin", result);
    }

    @Test
    public void privateStaticFieldKotlin() {
        Field result = ReflectUtils.getField(ReflectUtilsKotlinTest.class, "testMeStaticPrivate");
        Assert.assertNotNull(result);
    }

    @Test
    public void privateStaticFieldValueKotlin() {
        String result = (String) ReflectUtils.getFieldValue(ReflectUtilsKotlinTest.class, "testMeStaticPrivate");
        Assert.assertEquals("Hello, static private kotlin", result);
    }

}
