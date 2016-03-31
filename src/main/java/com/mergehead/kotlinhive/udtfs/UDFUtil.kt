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

import org.apache.hadoop.hive.ql.exec.UDFArgumentException
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector
import org.apache.hadoop.hive.serde2.objectinspector.primitive.WritableConstantIntObjectInspector
import org.apache.hadoop.hive.serde2.objectinspector.primitive.WritableIntObjectInspector

object UDFUtil {

    fun expectPrimitive(oi:ObjectInspector, context:String) {
        if(oi.category != ObjectInspector.Category.PRIMITIVE) {
            throw UDFArgumentException("$context should have been primitive, was ${oi.category}")
        }
    }

    fun expectInt(oi:ObjectInspector, context:String) {
        expectPrimitive(oi, context)
        if(oi !is WritableIntObjectInspector) {
            throw UDFArgumentException("$context should have been an int, was ${oi.javaClass}")
        }
    }

    fun expectConstantInt(oi:ObjectInspector, context:String) {
        expectPrimitive(oi, context)
        if(oi !is WritableConstantIntObjectInspector) {
            throw UDFArgumentException("$context should have been a constant int, was ${oi.javaClass}")
        }
    }

    fun requireParams(context:String, args:Array<Any?>, sizeRange:IntRange, allowNull:Boolean=false):Array<Any> {
        if(!sizeRange.contains(args.size)) {
            if(sizeRange.first == sizeRange.last) {
                throw UDFArgumentException("$context takes ${sizeRange.first} params!")
            }
            else {
                throw UDFArgumentException("$context takes ${sizeRange.first} to ${sizeRange.last} params!")
            }
        }

        if(!allowNull) {
            val nullParamIndexes = args.mapIndexed { index, it -> if (it == null) index else null }.filterNotNull()
            if(nullParamIndexes.size > 0) {
                throw UDFArgumentException("$context requires no null params! (found nulls for params: ${nullParamIndexes.joinToString(", ")})")
            }
        }

        return args.requireNoNulls()
    }

}