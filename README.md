# graph-from-tracerouting

This Rust application takes traceroutes of different formats and converts them into a graph form. The graph is then used
to perform statistics calculations on the snapshot of the internet resulting from the traceroutes.

Per run and file format, the application will take all files in the specified input directory and merge them into one
graph. If you want to separate them into different measurements, you need to run the application several times. **The
application does not support mixing inputs of different formats or protocol versions.**

The application is built to deal with large data sizes. Please always keep an eye on resource usage, as you might run
out of free RAM during processing.

The [Config.toml](./Config.toml) file allows you to configure your run without recompiling the application. You can
enable or disable every single step as well as each statistic calculation. **Please make sure to setup your paths to
point to actual places on your storage.**

Currently supported file formats:

- YARRP (.yarrp, as well as in compressed form as .yarrp.bz2)
- WARTS (.warts.gz)

Currently supported statistics:

- Degree (degree in/out, average neighbor degree in/out, iterated average neighbor degree in/out)
- Betweenness centrality

## Important usage considerations

- Setup [Config.toml](./Config.toml) before your first run.
- Make sure your input path actually exists, even if you are skipping the preprocessing step. The current setup checks
  for its existence on every run (can be adapted if necessary).
- Intermediate files are not deleted between runs. Make sure to clean up the intermediate folder, otherwise you might
  obtain results cross-contaminated with data from other datasets or previous runs.
- For betweenness centrality, do not use more threads than available cores. Firstly, it does not achieve any additional
  speedup, secondly, the more threads you use, the more RAM you need - and the RAM usage is considerable. You can
  configure the number of threads in [Config.toml](./Config.toml).
- You might consider deleting the edges.csv once you obtain edges_deduplicated.csv. Make sure to not mistakenly enable
  the deduplication step afterwards, as you will wipe the edges_deduplicated.csv.
- The IP mapping for YARRP never includes Node 0. Node 0 is the starting point.
- The betweenness centrality calculation does not use progress bars due to multithreading. Instead, it publishes the
  progress per thread into the log in plain form (Thread x: y / z nodes)