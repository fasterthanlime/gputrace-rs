# LLM Usage Guide

Use the public workflow. Do not start from internal subcommands.

```bash
gputrace report /abs/path/input.gputrace
```

If profiler data is missing, `report` profiles the trace automatically and caches
the result inside the `.gputrace` bundle. Then it writes a Markdown report next
to the trace unless `--output` is provided.

Read `/abs/path/input-report/index.md` first. Follow links from that file only
when needed.

Rules for agents:

- Use absolute paths.
- Pass the original `.gputrace` to `report`.
- Do not run report-section subcommands for normal analysis; they are internal
  plumbing.
- Treat private Xcode/MIO data as implementation detail, not user workflow.
- If source mapping is needed, search the source tree by kernel names from the
  report: `rg -n 'kernel_name' /abs/path/source/root`.

For the full user guide, read the repository `README.md`.
