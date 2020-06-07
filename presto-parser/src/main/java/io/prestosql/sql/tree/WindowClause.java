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
package io.prestosql.sql.tree;

import com.google.common.collect.ImmutableList;

import java.util.List;
import java.util.Optional;

import static java.util.Objects.requireNonNull;

public class WindowClause
        extends Node
{
    private final List<Window> window;

    public WindowClause(NodeLocation location, List<Window> window)
    {
        this(Optional.of(location), window);
    }

    public WindowClause(Optional<NodeLocation> location, List<Window> window)
    {
        super(location);
        this.window = ImmutableList.copyOf(requireNonNull(window, "window is null"));
    }

    public List<Window> getWindow()
    {
        return window;
    }

    @Override
    public List<? extends Node> getChildren()
    {
        return ImmutableList.copyOf(window);
    }

    @Override
    public int hashCode()
    {
        return 0;
    }

    @Override
    public boolean equals(Object obj)
    {
        return false;
    }

    @Override
    public String toString()
    {
        return null;
    }
}
