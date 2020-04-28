import asyncio
import aiohttp
import json

STATION_ID = "5b8449037c519100190fc728"
SENSOR_ID = "5b8449037c519100190fc72a"

async def fetchContent(session, url):
  async with session.get(url) as response:
    return await response.text()

async def getTemperature():
  async with aiohttp.ClientSession() as session:
    url = f"https://api.opensensemap.org/boxes/{STATION_ID}?format=json"
    content = await fetchContent(session, url)
    data = json.loads(content)
    sensors = data["sensors"]
    for sensor in sensors:
      if sensor["_id"] == SENSOR_ID:
        print( 
          sensor["lastMeasurement"]["createdAt"], 
          sensor["title"],
          sensor["lastMeasurement"]["value"],
          sensor["unit"],
          sep = ", ")

        await asyncio.sleep(60)
        loop.create_task(getTemperature())


if __name__ == '__main__':
    loop = asyncio.get_event_loop()
    loop.create_task(getTemperature())
    loop.run_forever()
    loop.stop()
    loop.close()