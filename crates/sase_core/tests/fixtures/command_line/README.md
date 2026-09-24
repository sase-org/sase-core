# Command Line fixtures

`sase_spec.json` is the full-descriptions spec used by the resolver goldens.
It is the compact (sorted-keys, `separators=(",",":")`) form of
`sase completion spec -d -j`.

Regenerate from the sase workspace root:

```bash
sase completion spec -d -j -o /tmp/cl_spec.json
python -c "import json; obj=json.load(open('/tmp/cl_spec.json')); open('crates/sase_core/tests/fixtures/command_line/sase_spec.json','w').write(json.dumps(obj, separators=(',',':'), sort_keys=True))"
```

`mini_spec.json` is hand-written for shapes the real spec lacks: a subcommand
alias, an int `nargs`, a required option, a group without `default_child`,
an option with `-y`, three stacked short flags, and two long options sharing
a prefix.
