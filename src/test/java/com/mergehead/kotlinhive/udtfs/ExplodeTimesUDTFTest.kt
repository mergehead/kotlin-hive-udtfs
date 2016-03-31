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

import com.klarna.hiverunner.HiveShell
import com.klarna.hiverunner.annotations.HiveSQL
import org.junit.Assert.*
import org.junit.Test

class ExplodeTimesUDTFTest : TestBase("explode_times", ExplodeTimesUDTF::class) {

    @Suppress("unused")
    @field:HiveSQL(files = arrayOf())
    var hiveShell:HiveShell? = null

    @Test
    fun simple() {
        assertEquals(
            listOf("1", "2", "3", "4", "5"),
            query("SELECT explode_times(5)")
        )
    }

    @Test
    fun simpleRange() {
        assertEquals(
            listOf("3", "4", "5"),
            query("SELECT explode_times(3, 5)")
        )
    }

    @Test
    fun rangeWithIncrementAmount() {
        assertEquals(
            listOf("1", "3", "5"),
            query("SELECT explode_times(1, 5, 2)")
        )
    }

    /**
     * Nulls are not allowed!
     */
    @Test(expected = IllegalArgumentException::class)
    fun nullSingle() {
        query("SELECT explode_times(NULL)")
    }

    @Test
    fun nonConstantInputIsAllowed() {
        assertEquals(
            listOf(
                "A\t1",
                "B\t1",
                "B\t2",
                "C\t1",
                "C\t2",
                "C\t3"
            ),
            query("""
                SELECT
                    group,
                    iter
                FROM (
                    SELECT INLINE(ARRAY(
                        STRUCT('A', 1),
                        STRUCT('B', 2),
                        STRUCT('C', 3)
                    )) AS (group, dynamic_high)
                ) data
                LATERAL VIEW explode_times(1, dynamic_high) v1 AS iter
            """)
        )
    }

}