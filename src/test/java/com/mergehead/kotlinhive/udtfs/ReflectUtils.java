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

import java.lang.reflect.Field;

public abstract class ReflectUtils {

    public static Field getField(Object obj, String fieldName) {
        try {
            Class<?> clazz;
            if(obj instanceof Class<?>) {
                clazz = (Class<?>) obj;
            }
            else {
                clazz = obj.getClass();
            }
            Field field = clazz.getDeclaredField(fieldName);
            if(field != null) {
                field.setAccessible(true);
            }
            return field;
        } catch (NoSuchFieldException e) {
            return null;
        }
    }

    public static Object getFieldValue(Object obj, String fieldName) {
        Field field = getField(obj, fieldName);
        return getFieldValue(field, obj);
    }

    public static Object getFieldValue(Field field, Object obj) {
        if(field != null) {
            try {
                if(obj instanceof Class<?>) {
                    return field.get(null);
                }
                return field.get(obj);
            } catch (IllegalAccessException e) {
                return null;
            }
        }

        return null;
    }

}
