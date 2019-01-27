import aioredis
import logging
import aiohttp
import traceback
import asyncio

class Runner:

    def __init__(self,config):
        self.config = config
        self.http = None
        self.r = None

    async def run(self, loop):
        self.r = await aioredis.create_redis_pool(self.config.REDIS_URI, loop=loop)
        (self.channel,) = await self.r.psubscribe(self.config.REDIS_CHANNEL_PREFIX+'*')
        self.http = aiohttp.ClientSession(loop=loop)
        try:
            await self.reader()
        finally:
            await self.r.punsubscribe(self.config.REDIS_CHANNEL_PREFIX+'*')
            await self.http.close()

    async def reader(self):
        while (await self.channel.wait_message()):
            try:
                dest_channel, msg = await self.channel.get_json()
                event_name = str(dest_channel,'utf-8').replace(self.config.REDIS_CHANNEL_PREFIX,'',1)
                log.debug('recv on {}: {}'.format(event_name,msg))
                asyncio.ensure_future(self.multiposter(event_name,msg))
            except:
                log.error(traceback.format_exc())

    async def multiposter(self,event_name,msg):
        urls = await self.r.smembers(self.config.REDIS_KEY_PREFIX + event_name)
        log.debug('urls = {}'.format(urls))
        await asyncio.gather(*[self.poster(msg,url) for url in urls])

    async def poster(self,msg,url):
        headers = {'Content-Type': 'application/json'}
        async with self.http.post(url, json=msg, headers=headers) as resp:
            if resp.status>=400:
                log.warning('{}: HTTP {}'.format(url,resp.status))


log = logging.getLogger(__name__)