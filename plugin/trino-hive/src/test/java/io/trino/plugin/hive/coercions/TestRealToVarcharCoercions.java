/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.trino.plugin.hive.coercions;

import io.airlift.slice.Slices;
import io.trino.plugin.hive.coercions.CoercionUtils.CoercionContext;
import io.trino.spi.TrinoException;
import io.trino.spi.block.Block;
import io.trino.spi.type.Type;
import org.junit.jupiter.api.Test;

import static io.trino.plugin.hive.HiveTimestampPrecision.DEFAULT_PRECISION;
import static io.trino.plugin.hive.HiveType.toHiveType;
import static io.trino.plugin.hive.coercions.CoercionUtils.createCoercer;
import static io.trino.spi.predicate.Utils.blockToNativeValue;
import static io.trino.spi.predicate.Utils.nativeValueToBlock;
import static io.trino.spi.type.RealType.REAL;
import static io.trino.spi.type.VarcharType.createUnboundedVarcharType;
import static io.trino.spi.type.VarcharType.createVarcharType;
import static io.trino.type.InternalTypeManager.TESTING_TYPE_MANAGER;
import static java.lang.Float.floatToIntBits;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class TestRealToVarcharCoercions
{
    @Test
    public void testRealToVarcharCoercions()
    {
        testRealToVarcharCoercions(Float.NEGATIVE_INFINITY, true);
        testRealToVarcharCoercions(Float.MIN_VALUE, true);
        testRealToVarcharCoercions(Float.MAX_VALUE, true);
        testRealToVarcharCoercions(Float.POSITIVE_INFINITY, true);
        testRealToVarcharCoercions(Float.parseFloat("123456789.12345678"), true);

        testRealToVarcharCoercions(Float.NEGATIVE_INFINITY, false);
        testRealToVarcharCoercions(Float.MIN_VALUE, false);
        testRealToVarcharCoercions(Float.MAX_VALUE, false);
        testRealToVarcharCoercions(Float.POSITIVE_INFINITY, false);
        testRealToVarcharCoercions(Float.parseFloat("123456789.12345678"), false);
    }

    private void testRealToVarcharCoercions(Float floatValue, boolean treatNaNAsNull)
    {
        assertCoercions(REAL, floatValue, createUnboundedVarcharType(), Slices.utf8Slice(floatValue.toString()), treatNaNAsNull);
    }

    @Test
    public void testRealToSmallerVarcharCoercions()
    {
        testRealToSmallerVarcharCoercions(Float.NEGATIVE_INFINITY, true);
        testRealToSmallerVarcharCoercions(Float.MIN_VALUE, true);
        testRealToSmallerVarcharCoercions(Float.MAX_VALUE, true);
        testRealToSmallerVarcharCoercions(Float.POSITIVE_INFINITY, true);
        testRealToSmallerVarcharCoercions(Float.parseFloat("123456789.12345678"), true);

        testRealToSmallerVarcharCoercions(Float.NEGATIVE_INFINITY, false);
        testRealToSmallerVarcharCoercions(Float.MIN_VALUE, false);
        testRealToSmallerVarcharCoercions(Float.MAX_VALUE, false);
        testRealToSmallerVarcharCoercions(Float.POSITIVE_INFINITY, false);
        testRealToSmallerVarcharCoercions(Float.parseFloat("123456789.12345678"), false);
    }

    private void testRealToSmallerVarcharCoercions(Float floatValue, boolean treatNaNAsNull)
    {
        assertThatThrownBy(() -> assertCoercions(REAL, floatValue, createVarcharType(1), floatValue.toString(), treatNaNAsNull))
                .isInstanceOf(TrinoException.class)
                .hasMessageContaining("Varchar representation of %s exceeds varchar(1) bounds", floatValue);
    }

    @Test
    public void testNaNToVarcharCoercions()
    {
        assertCoercions(REAL, Float.NaN, createUnboundedVarcharType(), null, true);

        assertCoercions(REAL, Float.NaN, createUnboundedVarcharType(), Slices.utf8Slice("NaN"), false);
        assertThatThrownBy(() -> assertCoercions(REAL, Float.NaN, createVarcharType(1), "NaN", false))
                .isInstanceOf(TrinoException.class)
                .hasMessageContaining("Varchar representation of NaN exceeds varchar(1) bounds");
    }

    public static void assertCoercions(Type fromType, Float valueToBeCoerced, Type toType, Object expectedValue, boolean isOrcFile)
    {
        Block coercedValue = createCoercer(TESTING_TYPE_MANAGER, toHiveType(fromType), toHiveType(toType), new CoercionContext(DEFAULT_PRECISION, isOrcFile)).orElseThrow()
                .apply(nativeValueToBlock(fromType, (long) floatToIntBits(valueToBeCoerced)));
        assertThat(blockToNativeValue(toType, coercedValue))
                .isEqualTo(expectedValue);
    }
}
