# website-backend

## MongoDB Index Management

### Startup behavior

Backend startup now performs index create/ensure only. It does not drop indexes.

- Startup entrypoint: `src/main.py`
- Ensure function: `database.index_bootstrap.ensure_backend_indexes`

This makes startup safe and idempotent on both existing and fresh volumes.

### Manual cleanup behavior

Legacy/redundant index cleanup is intentionally manual-only.

- Cleanup function: `database.index_bootstrap.run_manual_legacy_index_cleanup`
- Cleanup entrypoint: `database.index_cleanup`

Run from `backend/src`:

```bash
python -m database.index_cleanup
```

Recommended sequence:

1. Start backend once and let startup ensure required indexes.
2. Run manual cleanup during a maintenance window.
3. Restart backend and confirm startup remains additive-only.