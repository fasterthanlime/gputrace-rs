# GPU Trace Profiling Workflow

The supported workflow is:

```bash
gputrace report /abs/path/input.gputrace
```

`report` profiles the trace automatically if cached profiler data is missing. By default, it stores both the profiler cache and Markdown report inside the `.gputrace` bundle. It prints live progress while running, and `gputrace-report/index.md` records total and per-section timings.

For the complete guide, see the repository `README.md`.
