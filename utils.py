import contextlib
import logging


class LogSuppress(contextlib.suppress):
    def __exit__(self, exctype, excinst, exctb):
        if excinst:
            logging.exception(f'')
        return super().__exit__(exctype, excinst, exctb)
