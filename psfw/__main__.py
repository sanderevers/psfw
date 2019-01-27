import asyncio
import logging
import warnings
import sys
import importlib
from .runner import Runner

def main():
    if len(sys.argv)>1:
        config_name=sys.argv[1]
    else:
        config_name='psfw_config'

    config = importlib.import_module(config_name)

    if config.DEBUG_ASYNCIO:
        asyncio.get_event_loop().set_debug(True)
        warnings.simplefilter("always", ResourceWarning)
    logging.basicConfig(level=logging.DEBUG if config.DEBUG else logging.INFO, filename=config.LOGFILE)

    loop=asyncio.get_event_loop()
    loop.run_until_complete(Runner(config).run(loop))

if __name__=='__main__':
    main()