import sys
from asyncio import events


def run(main):
    """
      A very basic backport shim of the asyncio.run method from python 3.7
      so we have something that will work in python 3.6
      """
    if sys.version_info >= (3, 7):
        import asyncio
        asyncio.run(main)
    else:
        loop = events.new_event_loop()
        events.set_event_loop(loop)
        return loop.run_until_complete(main)
