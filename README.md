# Polygon Overlay / Hadoop MapReduce / Grid

- [Polygon Overlay / Hadoop MapReduce / Grid](#polygon-overlay--hadoop-mapreduce--grid)
  - [Algorithm](#algorithm)
  - [References](#references)

## Algorithm

```
method MAP (id, polygon p)
{
    compute bounding box b of p
    find overlapping grid cell(s) C for p based on b
    for all grid cell ∈ C do
        EMIT: (cell id, p)
    end for
}

method REDUCE (cell id, [p1, p2, ..])
{
    for all p in [p1, p2, ..] do
        if p is from base layer then
            add to list B
        else
            compute the bounding box b of p
            add b to R-tree tree
        end if
    end for

    for each base polygon pb in B
        compute bounding box b of pb
        C ← get overlapping polygons by searching for b in tree
        for each overlay layer polygon pc in C
            compute overlay (pb, pc)
            EMIT: output polygon
        end for
    end for
}
```

## Development

This project includes a [Development Containers](https://containers.dev) definition. If you open it in an environment that supports dev containers, such as [Visual Studio Code](https://code.visualstudio.com), you will be prompted to reopen the project in the dev container.

After the project opens in the dev container, wait for the initialization process to complete. Then, install the recommended extensions.

To open this project using Development Containers, the ports `9870`, `8088`, and `8042` should **not** be in use, as they are exposed to the host for accessing various Hadoop's management user interfaces.

Exposed Hadoop Management User Intefaces:
- [Name Node](http://localhost:9870)
- [Resource Manager](http://localhost:8088)
- [Node Manager](http://localhost:8042)

## References

- Puri, S. (2015). *Efficient Parallel and Distributed Algorithms for GIS Polygon Overlay Processing* (Doctoral dissertation, Georgia State University). https://doi.org/10.57709/7282021
