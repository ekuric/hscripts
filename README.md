# hscripts - helper scripts 
In absence of more creative name, I call this repository `hscripts`. It will contain different helper scripts used in various tasks. 

For now we have here 

- `analyze_bw_mean_with_graphs.py` when executed inside fio collected directory, eg

```
fio_results/fio-machine{1,2}
``` 
will draw graphs for fio bandwidth ( `bw_mean `) results collected on tests running on machines fio-machine1 and fio-machine2. It runs on 1000s of fio results. 

- `iops_analyzer.py` this script will analyze iops ( `iops_mean` ) from fio results collected on same way as in previous example. 



