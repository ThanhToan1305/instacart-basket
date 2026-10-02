"""Logger ra console và file, tránh handler trùng lặp."""
import logging
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[2]


def get_logger(name: str, log_file: str | Path | None = None) -> logging.Logger:
    logger = logging.getLogger(name)
    if logger.handlers:
        return logger
    path = Path(log_file) if log_file else PROJECT_ROOT / "logs/project.log"
    path.parent.mkdir(parents=True, exist_ok=True)
    formatter = logging.Formatter("%(asctime)s %(levelname)s %(name)s: %(message)s")
    for handler in (logging.FileHandler(path, encoding="utf-8"), logging.StreamHandler()):
        handler.setFormatter(formatter)
        logger.addHandler(handler)
    logger.setLevel(logging.INFO)
    logger.propagate = False
    return logger
