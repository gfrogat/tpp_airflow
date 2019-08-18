import logging
import logging.handlers

from logging import Logger


def get_socket_logger(
    logger_name: str,
    host="localhost",
    port=logging.handlers.DEFAULT_TCP_LOGGING_PORT,
    log_level=logging.INFO,
) -> Logger:
    logger = logging.getLogger(logger_name)

    if logger.handlers:
        # handler already initialized
        pass
    else:
        logger.setLevel(log_level)
        socket_handler = logging.handlers.SocketHandler(host, port)
        logger.addHandler(socket_handler)

    return logger
