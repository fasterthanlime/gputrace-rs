# GPU Trace Profiling Workflow

The supported workflow is:

```bash
gputrace report /abs/path/input.gputrace
```

`report` profiles the trace automatically if cached profiler data is missing,
stores that cache inside the `.gputrace` bundle, and writes a Markdown report.
Start at `index.md`.

For the complete guide, see the repository `README.md`.
