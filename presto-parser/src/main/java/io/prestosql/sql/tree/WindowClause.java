package io.prestosql.sql.tree;

import com.google.common.collect.ImmutableList;

import java.util.List;
import java.util.Optional;

import static java.util.Objects.requireNonNull;

public class WindowClause
        extends Node
{
    private final List<Window> window;

    public WindowClause(Optional<NodeLocation> location, List<Window> window)
    {
        super(location);
        this.window = ImmutableList.copyOf(requireNonNull(window, "window is null"));
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
