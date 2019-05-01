import logging


log = logging.getLogger(__name__)
log.level = logging.INFO
sh = logging.StreamHandler()
sh.setFormatter(
    logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")
)
log.addHandler(sh)
