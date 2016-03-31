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

import org.apache.hadoop.hive.ql.exec.Description
import org.apache.hadoop.hive.ql.exec.UDFArgumentException
import org.apache.hadoop.hive.ql.udf.generic.GenericUDTF
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory

@Description(
    name = "explode_times",
    value = "_FUNC_(low, high, incr) - Emits all ints from the low bound to the high bound adding the incr each time",
    extended = """
For example, you could do something like this:

  SELECT explode_times(3)
    ==>
        1
        2
        3

  or

  SELECT explode_times(3, 5)
    ==>
        3
        4
        5

  or

  SELECT explode_times(1, 3, 2)
    ==>
        1
        3
        5
"""
)
class ExplodeTimesUDTF : GenericUDTF() {

    override fun initialize(argOIs:StructObjectInspector?):StructObjectInspector {
        if(argOIs == null) {
            throw UDFArgumentException("Something went wrong, this should not be null")
        }

        val inputFields = argOIs.allStructFieldRefs
        if(inputFields.size < 1 || inputFields.size > 3) {
            throw UDFArgumentException("ExplodeTimesUDTF takes 1 to 3 params!")
        }

        UDFUtil.expectInt(inputFields[0].fieldObjectInspector, "ExplodeTimesUDTF param 1")
        if(inputFields.size > 1) {
            UDFUtil.expectInt(inputFields[1].fieldObjectInspector, "ExplodeTimesUDTF param 2")
        }
        if(inputFields.size > 2) {
            UDFUtil.expectInt(inputFields[2].fieldObjectInspector, "ExplodeTimesUDTF param 3")
        }

        return ObjectInspectorFactory.getStandardStructObjectInspector(
            listOf("iter"),
            listOf(PrimitiveObjectInspectorFactory.javaIntObjectInspector)
        )
    }

    override fun process(args:Array<Any?>?) {
        if(args == null) return
        val safeArgs = UDFUtil.requireParams("ExplodeTimesUDTF", args, 1..3)

        // Start at 1 if we only specify a high bound, or take the first param otherwise
        val lowBound = if(safeArgs.size > 1) {
            PrimitiveObjectInspectorFactory.writableIntObjectInspector.get(safeArgs[0])
        }
        else {
            1
        }

        // End at the first param if it's the only one, or the second if we have more
        val highBound = if(safeArgs.size > 1) {
            PrimitiveObjectInspectorFactory.writableIntObjectInspector.get(safeArgs[1])
        }
        else {
            PrimitiveObjectInspectorFactory.writableIntObjectInspector.get(safeArgs[0])
        }

        // Increment by 1 by default, else use the third param if present
        val increment = if(safeArgs.size > 2) {
            PrimitiveObjectInspectorFactory.writableIntObjectInspector.get(safeArgs[2])
        }
        else {
            1
        }

        for(index in lowBound..highBound step increment) {
            forward(arrayOf(index))
        }
    }

    override fun close() {}

}
