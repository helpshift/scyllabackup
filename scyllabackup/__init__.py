import logging

logging.basicConfig(level=logging.WARNING,
                    format='[%(asctime)s] %(levelname)s %(name)s: %(message)s')
logger = logging.getLogger(__name__)
