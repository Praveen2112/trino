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
import java.util.Objects;
import java.util.Optional;

import static com.google.common.base.MoreObjects.toStringHelper;
import static java.util.Objects.requireNonNull;

public class Window
        extends Node
{
    private final Identifier windowName;
    private final WindowSpecification windowSpecification;

    public Window(Identifier windowName, WindowSpecification windowSpecification)
    {
        this(Optional.empty(), windowName, windowSpecification);
    }

    public Window(NodeLocation location, Identifier windowName, WindowSpecification windowSpecification)
    {
        this(Optional.of(location), windowName, windowSpecification);
    }

    public Window(Optional<NodeLocation> location, Identifier windowName, WindowSpecification windowSpecification)
    {
        super(location);
        this.windowName = requireNonNull(windowName, "windowName is null");
        this.windowSpecification = requireNonNull(windowSpecification, "windowSpecification is null");
    }

    public Identifier getWindowName()
    {
        return windowName;
    }

    public WindowSpecification getWindowSpecification()
    {
        return windowSpecification;
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context)
    {
        return visitor.visitWindow(this, context);
    }

    @Override
    public List<Node> getChildren()
    {
        return ImmutableList.of(windowSpecification);
    }

    @Override
    public boolean equals(Object obj)
    {
        if (this == obj) {
            return true;
        }
        if ((obj == null) || (getClass() != obj.getClass())) {
            return false;
        }
        Window o = (Window) obj;
        return Objects.equals(windowName, o.windowName) &&
                Objects.equals(windowSpecification, o.windowSpecification);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(windowName, windowSpecification);
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("windowName", windowName)
                .add("windowSpecification", windowSpecification)
                .toString();
    }
}
