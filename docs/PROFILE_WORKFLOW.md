# GPU Trace Profiling Workflow

The supported workflow is:

```bash
gputrace report /abs/path/input.gputrace
```

`report` profiles the trace automatically if cached profiler data is missing. By default, it stores both the profiler cache and Markdown report inside the `.gputrace` bundle. Start at `gputrace-report/index.md`.

For the complete guide, see the repository `README.md`.
