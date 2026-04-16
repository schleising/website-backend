import logging

try:
    from database.index_bootstrap import run_manual_legacy_index_cleanup
except ModuleNotFoundError:
    from .index_bootstrap import run_manual_legacy_index_cleanup


if __name__ == "__main__":
    logging.basicConfig(
        format="Index Cleanup: %(asctime)s - %(levelname)s - %(message)s",
        level=logging.INFO,
    )

    run_manual_legacy_index_cleanup()
